#!/usr/bin/env bash
#
# One command, two failure modes, on one machine. Needs only the Go toolchain.
#
#   ./scripts/demo.sh
#
# Act 1 kills a worker outright. Act 2 freezes a healthy one until the detector
# misjudges it. The two acts are not interchangeable: under a crash the transport
# reports the failure, the dead attempt is known-dead, and no second computation
# is ever started -- so no duplicate is produced and none is discarded. Only the
# false positive puts two attempts on the same request at the same time, which is
# the case adjudication exists for.
#
# Every wait here is a polled condition. None is a fixed sleep, for the same
# reason the test suite has none: a demo that passes on an idle laptop and fails
# on a loaded one is worse than no demo. The 100ms below is a poll interval, not
# a guess at how long something takes.

set -euo pipefail

cd "$(dirname "$0")/.."

WORKER_DELAY=12s   # long enough that a batch is still in flight when we kill
BATCH=20           # requests per act
REQ_TIMEOUT=90s    # client-side ceiling; attemptTimeout in the gateway is 30s
POLL=0.1           # poll interval for the wait helpers
DEADLINE=60        # seconds any single wait_for may take before giving up

LOGS="$(mktemp -d "${TMPDIR:-/tmp}/llm-cp-demo.XXXXXX")"
PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -CONT "$pid" 2>/dev/null || true
    [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m%s\033[0m\n' "$*"; }
bad()  { printf '  \033[31m%s\033[0m\n' "$*"; }

# wait_for <file> <pattern> <description>
# Blocks until the pattern appears in the file. Fails loudly instead of hanging.
wait_for() {
  local file="$1" pat="$2" what="$3" end=$(( $(date +%s) + DEADLINE ))
  while ! grep -qE "$pat" "$file" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$end" ]; then
      bad "timed out waiting for $what"
      bad "  (no /$pat/ in $file after ${DEADLINE}s)"
      exit 1
    fi
    sleep "$POLL"
  done
}

# wait_for_count <file> <pattern> <n> <description>
wait_for_count() {
  local file="$1" pat="$2" want="$3" what="$4" end=$(( $(date +%s) + DEADLINE ))
  while [ "$(count "$file" "$pat")" -lt "$want" ]; do
    if [ "$(date +%s)" -ge "$end" ]; then
      bad "timed out waiting for $what"
      bad "  (saw $(count "$file" "$pat") of $want /$pat/ in $file)"
      exit 1
    fi
    sleep "$POLL"
  done
}

count() { grep -cE "$2" "$1" 2>/dev/null || true; }

# busiest_worker prints the index of the worker log holding the most in-flight
# requests. Selection in the gateway is uniform random, so which worker matters
# varies per run -- we read the answer rather than assume it. That is what makes
# the outcome deterministic without asserting on a random quantity.
busiest_worker() {
  local best=0 bestn=-1 i n
  for i in "${ALIVE[@]}"; do
    n=$(count "$LOGS/worker-$i.log" 'Generate: start')
    if [ "$n" -gt "$bestn" ]; then bestn=$n; best=$i; fi
  done
  printf '%s' "$best"
}

# ── build ────────────────────────────────────────────────────────────────────
say "Building"
go build -o bin/controlplane ./cmd/controlplane
go build -o bin/worker       ./cmd/worker
go build -o bin/gateway      ./cmd/gateway
go build -o bin/democlient   ./cmd/democlient
step "logs in $LOGS"

# ── bring the cluster up ─────────────────────────────────────────────────────
say "Starting control plane, 3 workers, gateway"

./bin/controlplane >"$LOGS/controlplane.log" 2>&1 &
CP_PID=$!; PIDS+=("$CP_PID")
wait_for "$LOGS/controlplane.log" 'control plane listening' "the control plane to listen"

WORKER_PID=()
for i in 1 2 3; do
  ./bin/worker -addr "localhost:6000$i" -delay "$WORKER_DELAY" \
    >"$LOGS/worker-$i.log" 2>&1 &
  WORKER_PID[$i]=$!
  PIDS+=("${WORKER_PID[$i]}")
done
ALIVE=(1 2 3)

wait_for_count "$LOGS/controlplane.log" 'registered worker' 3 "3 workers to register"
step "3 workers registered, ${WORKER_DELAY} mock inference each"

./bin/gateway >"$LOGS/gateway.log" 2>&1 &
GW_PID=$!; PIDS+=("$GW_PID")
wait_for "$LOGS/gateway.log" 'gateway InferenceService listening' "the gateway to listen"

# The gateway learns membership by polling, so it does not see the workers the
# instant they register; until it does, Generate fails fast with Unavailable.
# Probe with one throwaway request and wait for a WORKER to log that it arrived
# -- routing is proven the moment the request lands, and waiting for the result
# instead would cost a full WORKER_DELAY of dead air before the demo starts.
./bin/democlient -n 1 -prefix warmup -timeout "$REQ_TIMEOUT" >/dev/null 2>&1 &
PIDS+=("$!")
GW_READY_END=$(( $(date +%s) + DEADLINE ))
until grep -qE 'Generate: start req=warmup' "$LOGS"/worker-*.log 2>/dev/null; do
  if [ "$(date +%s)" -ge "$GW_READY_END" ]; then
    bad "gateway never became able to route"
    bad "  (no warmup request reached any worker; see $LOGS/gateway.log)"
    exit 1
  fi
  sleep "$POLL"
done
step "gateway routing"

# ─────────────────────────────────────────────────────────────────────────────
say "ACT 1 — crash (kill -9)"

# The fast path has no log line of its own. attemptFailed re-dispatches by
# calling attempt() directly (gateway.go:473), so the evidence of a crash
# rescue is "attempt failed" for a request id, followed by "served by" for that
# same id on a different worker. The slow path's "rerouting req=" line does not
# appear here -- act 2 is where that one shows up.
./bin/democlient -n "$BATCH" -prefix act1 -timeout "$REQ_TIMEOUT" \
  -json "$LOGS/act1.json" >"$LOGS/act1.out" 2>&1 &
CLIENT_PID=$!

wait_for "$LOGS/worker-1.log" 'Generate: start' "requests to reach the workers"
wait_for "$LOGS/worker-2.log" 'Generate: start' "requests to reach the workers"
wait_for "$LOGS/worker-3.log" 'Generate: start' "requests to reach the workers"

VICTIM=$(busiest_worker)
INFLIGHT=$(count "$LOGS/worker-$VICTIM.log" 'Generate: start')
step "worker-$VICTIM holds $INFLIGHT in-flight requests — killing it"
kill -9 "${WORKER_PID[$VICTIM]}" 2>/dev/null || true
wait "${WORKER_PID[$VICTIM]}" 2>/dev/null || true   # reap quietly
ALIVE=(); for i in 1 2 3; do [ "$i" != "$VICTIM" ] && ALIVE+=("$i"); done

wait_for "$LOGS/gateway.log" 'gateway: attempt failed req=act1-' \
  "the transport to report the dead worker"

ACT1_RC=0; wait "$CLIENT_PID" || ACT1_RC=$?

# Stranded ids: every act-1 request whose attempt died with the worker.
# RESCUED counts how many of those were nonetheless answered.
STRANDED=$(grep -oE 'attempt failed req=act1-[0-9]+' "$LOGS/gateway.log" \
  | sed 's/.*req=//' | sort -u)
ACT1_STRANDED=$(printf '%s\n' "$STRANDED" | grep -c . || true)
ACT1_RESCUED=0
for id in $STRANDED; do
  if grep -qE "req=$id served by" "$LOGS/gateway.log"; then
    ACT1_RESCUED=$(( ACT1_RESCUED + 1 ))
  fi
done
ACT1_DEDUP=$(count "$LOGS/gateway.log" 'lost the race')

sed -n '/completed,/p' "$LOGS/act1.out" | sed 's/^ */  /'
step "$ACT1_STRANDED requests were stranded by the crash, $ACT1_RESCUED answered anyway"
step "$ACT1_DEDUP duplicate results discarded — expected: a crash is known-dead,"
step "  so no second attempt is ever started and there is nothing to discard"

# ─────────────────────────────────────────────────────────────────────────────
say "ACT 2 — false positive (SIGSTOP)"

BEFORE_REROUTE=$(count "$LOGS/gateway.log" 'gateway: rerouting req=')
BEFORE_DEDUP=$(count "$LOGS/gateway.log" 'lost the race')
for i in "${ALIVE[@]}"; do : >"$LOGS/worker-$i.inflight"; done

./bin/democlient -n "$BATCH" -prefix act2 -timeout "$REQ_TIMEOUT" \
  -json "$LOGS/act2.json" >"$LOGS/act2.out" 2>&1 &
CLIENT_PID=$!

for i in "${ALIVE[@]}"; do
  wait_for_count "$LOGS/worker-$i.log" 'Generate: start' 1 "requests to reach worker-$i"
done

FROZEN=$(busiest_worker)
step "worker-$FROZEN is healthy — freezing it with SIGSTOP"
kill -STOP "${WORKER_PID[$FROZEN]}"

wait_for_count "$LOGS/gateway.log" 'gateway: rerouting req=' \
  $(( BEFORE_REROUTE + 1 )) "the detector to misjudge worker-$FROZEN and reroute"
step "misjudged dead and rerouted — two attempts now compute the same requests"

kill -CONT "${WORKER_PID[$FROZEN]}"
step "resumed — its late results are about to arrive for requests already answered"

ACT2_RC=0; wait "$CLIENT_PID" || ACT2_RC=$?
wait_for_count "$LOGS/gateway.log" 'lost the race' \
  $(( BEFORE_DEDUP + 1 )) "the late duplicate to be discarded"

ACT2_REROUTED=$(( $(count "$LOGS/gateway.log" 'gateway: rerouting req=') - BEFORE_REROUTE ))
ACT2_DEDUP=$(( $(count "$LOGS/gateway.log" 'lost the race') - BEFORE_DEDUP ))

sed -n '/completed,/p' "$LOGS/act2.out" | sed 's/^ */  /'
step "$ACT2_REROUTED rerouted"
step "$ACT2_DEDUP duplicate results discarded"

# ─────────────────────────────────────────────────────────────────────────────
say "Verdict"

FAIL=0
check() { if [ "$1" -eq 1 ]; then ok "PASS  $2"; else bad "FAIL  $2"; FAIL=1; fi; }

check "$([ "$ACT1_RC" -eq 0 ] && echo 1 || echo 0)" \
  "act 1: all $BATCH requests completed, none lost to the crash"
check "$([ "$ACT1_STRANDED" -gt 0 ] && echo 1 || echo 0)" \
  "act 1: the crash stranded in-flight requests ($ACT1_STRANDED)"
check "$([ "$ACT1_RESCUED" -eq "$ACT1_STRANDED" ] && echo 1 || echo 0)" \
  "act 1: every stranded request was re-dispatched and answered ($ACT1_RESCUED/$ACT1_STRANDED)"
check "$([ "$ACT2_RC" -eq 0 ] && echo 1 || echo 0)" \
  "act 2: all $BATCH requests completed despite the misjudgement"
check "$([ "$ACT2_REROUTED" -gt 0 ] && echo 1 || echo 0)" \
  "act 2: requests on the frozen worker were rerouted ($ACT2_REROUTED)"
check "$([ "$ACT2_DEDUP" -gt 0 ] && echo 1 || echo 0)" \
  "act 2: late duplicate results were discarded ($ACT2_DEDUP)"

echo
if [ "$FAIL" -eq 0 ]; then
  ok "A worker died and a healthy one was misjudged. No request was lost, and"
  ok "where two attempts computed the same request, one result was discarded."
  echo
  step "logs: $LOGS"
  exit 0
fi
bad "demo did not hold. logs: $LOGS"
exit 1
