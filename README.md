# llm-inference-cp
[![CI](https://github.com/lucas1114/llm-inference-cp/actions/workflows/ci.yml/badge.svg)](https://github.com/lucas1114/llm-inference-cp/actions/workflows/ci.yml)

A distributed control plane for LLM inference serving, written in Go.

The control plane owns cluster membership, failure detection, and routing
decisions — and stays **out of the data path**. It tells the gateway _who_ to
route to; it never forwards a byte of inference traffic itself.

> **North star:** exactly-once is a property of the whole pipeline, never of any
> single component. The honest answer is always
> **at-least-once delivery + idempotent processing = effectively-once semantics.**
> That principle recurs throughout this system: in failure detection, in
> rerouting, and in request deduplication.

> This is a personal portfolio project, built to explore distributed systems
> depth rather than feature completeness. Workers are mocked; the inference
> engine is not the point.

## Architecture

```
        client
          │  Generate (gRPC)
          ▼
   ┌───────────────┐   poll ListWorkers   ┌────────────────────┐
   │    gateway    │─────────────────────▶│   control plane    │
   │  (stateless)  │                      │                    │
   │               │                      │  registry          │
   │ RequestTracker│                      │  phi-accrual       │
   │  first-wins   │                      │  failure detector  │
   └───────┬───────┘                      └─────────▲──────────┘
           │  Generate (gRPC)                       │ heartbeats
           ▼                                        │
   ┌──────────────┐    ┌──────────────┐             │
   │   worker A   │    │   worker B   │─────────────┘
   └──────────────┘    └──────────────┘

   (M3: a metadata store behind the control plane, so cluster state
    survives a control plane restart)
```

A worker registers, then proves liveness by heartbeating. The gateway asks the
control plane who is alive and dials those workers directly — the control plane
never sees an inference request. Its failure degrades routing quality; it does
not stall requests already in flight.

Membership reaches the gateway by polling rather than by subscription. The
detector's verdicts are an in-process channel inside the control plane, and a
poll diff carries them across the process boundary at the latency this system
targets. A watch stream becomes free once M3 introduces etcd.

## Status

| Milestone | Scope                                                                            | State   |
| --------- | -------------------------------------------------------------------------------- | ------- |
| **M1**    | gRPC skeleton, worker registry, end-to-end registration                          | ✅ Done |
| **M2**    | Heartbeats, phi-accrual failure detection, zero-loss rerouting, idempotent dedup | ✅ Done |
| **M3**    | Control plane HA via Raft leader election                                        | Planned |
| **M4**    | KV-cache-aware scheduling and backpressure                                       | Planned |
| **M5**    | Fault-injection harness with correctness assertions                              | Planned |

**M2 breakdown:** heartbeat pipeline ✅ · graceful deregister ✅ · phi-accrual
detection ✅ · data-plane forwarding ✅ · zero-loss rerouting ✅ · first-wins
deduplication ✅

## Design notes

### Liveness is judged by the observer, not the observed

A worker cannot distinguish "the network is partitioned," "the control plane is
restarting," and "I am dead" — from its own vantage point these are the same
observation. So the worker never judges its own liveness. It beats on a fixed
cadence and logs failures. The control plane's failure detector, which sees the
absence of heartbeats across the whole fleet, adjudicates.

Two consequences fall out of this:

- **No in-loop retry on a failed beat.** The next tick is already the retry.
  Retrying inside a tick would emit beats at irregular intervals and corrupt the
  inter-arrival distribution that phi-accrual learns from.
- **A worker never exits because it cannot reach the control plane.** Otherwise
  a control plane restart would amplify into a fleet-wide outage.

### `NotFound` is a semantic signal, not a transient error

When the control plane restarts, its registry is empty. The next heartbeat from
a surviving worker returns `NotFound` — the RPC succeeded, and the answer is
"I don't know who you are."

The control plane deliberately does _not_ silently upsert the unknown worker. A
silent upsert would resurrect a worker behind the failure detector's back,
bypassing its `ALIVE → SUSPECT → DEAD` state machine. Instead the worker walks
back through the front door with an explicit `Register` call, reusing the same
`worker_id`. Identity continuity is what makes this a resurrection rather than
the arrival of a new worker.

### Death is inferred from silence, on a clock

A dead worker sends nothing — there is no callback to hang detection on. So the
detector cannot be event-driven; it is a single scanner goroutine on a ticker
that periodically asks, for every worker, "how long has it been silent, and how
anomalous is that?"

The answer is a [phi-accrual](https://doi.org/10.1109/RELDIS.2004.1353004) score
rather than a fixed timeout. The detector models the distribution of past
heartbeat inter-arrival gaps and outputs a continuous suspicion value
φ = −log₁₀(P(a beat is later than the current silence)). φ is a normalized
confidence, not a raw duration, so one threshold works across a fast LAN and a
jittery WAN without hand-tuning: φ=1 ≈ 10% false-positive risk, φ=8 ≈ 1e-8. Two
thresholds map the same curve onto the state machine — a low one for `SUSPECT`,
a high one for `DEAD` — so a brief network wobble surfaces as `SUSPECT` and
recovers if beats resume, while a real crash rides φ past `DEAD` in a couple of
scans.

State lives with the detector, not in the registry. The registry is a pure fact
store (what workers reported); a worker's ALIVE/SUSPECT/DEAD classification is an
opinion the detector infers. Keeping the opinion out of the fact store means each
has a single writer and the two never share a lock.

### Duplication is the price of detection, not a bug in it

Phi-accrual yields a confidence, never a truth. In an asynchronous network
"slow" and "crashed" are indistinguishable, so choosing a `DEAD` threshold is
choosing a false-positive rate — no amount of tuning eliminates one. Everything
downstream follows from accepting that:

```
false positive → reroute → two attempts in flight → duplication
              → first-wins adjudication → effectively-once
```

Each arrow is a consequence rather than a design choice. The only real decision
is where the duplicate gets absorbed, and here it is the router: mock inference
has no side effects, so discarding a redundant _result_ is sufficient. Once a
worker writes a KV cache or debits a quota, idempotence has to move down to that
layer — the request id is carried unchanged through every reroute precisely so
it can.

The suspected worker is deliberately **not** cancelled. Acting on a
probabilistic guess by killing possibly-good work is worse than letting it
finish and discarding the answer; a per-attempt timeout bounds the waste.

Adjudication is a one-way atomic latch, not the result channel's buffer slot.
The buffer looks like a natural token — first sender claims the only seat — but
the handler's receive empties it, so a result arriving seconds later would find
it free and "win" a race already settled. The channel transports; the latch
decides. The channel is never closed: close is the sender's privilege, and with
several attempts sending, that privilege has no owner.

Reassignment happens under the tracker lock, and that is what makes it a claim:
once a stranded request points at its new worker, a second poll tick cannot
rescue it again and put a third attempt in the air. The lookup for stranded
requests matches on worker **ID**, never address — a restarted process can reuse
the port under a fresh id, and that is a different worker, one that never
received the request.

### Failed attempts are classified before they are retried

An error is either about the _machine_ (`Unavailable`, `DeadlineExceeded` —
another worker may well succeed) or about the _request_ (`InvalidArgument` and
friends — no worker will do better). Only the first kind earns a replacement
attempt; the second is the answer and is returned immediately.

Getting this split wrong is how retry storms start: retrying a request that is
invalid everywhere turns one failure into N, and does it during a failure, when
the fleet can least afford the extra load.

A machine-level error must also **not** settle the request on its own. Failing
fast would otherwise beat every slower success and quietly disable rerouting
altogether, so an error becomes the client's answer only once the in-flight
attempt count reaches zero and nobody is left to respond.

### The same code, two failure modes

Which failure you inject decides which property you can demonstrate.

|                    | `kill -9`                          | `SIGSTOP`                      |
| ------------------ | ---------------------------------- | ------------------------------ |
| Worker             | dies; kernel resets the connection | freezes; connection stays open |
| Gateway learns via | transport error, milliseconds      | poll diff, seconds             |
| Old attempt        | provably finished                  | possibly still computing       |
| Attempts in flight | one                                | two                            |
| Deduplication      | nothing to adjudicate              | decides the winner             |
| Client sees        | one result, ~320ms                 | one result, ~3.5s              |

`kill -9` cannot produce a duplicate. The transport reports the death, so the
failed attempt is known-dead rather than suspected-dead, and replacing it is
safe by construction. This fast path buys latency, not correctness — the poll
diff would have rerouted the same request roughly a second later, and in a
measured run it arrived to find the tracker already empty.

`SIGSTOP` is the interesting case: the worker is **healthy throughout**. The
detector is simply wrong about it, both attempts run concurrently, and exactly
one result reaches the client. That is the only condition under which
deduplication has anything to adjudicate, and the only demo that proves it
works.

## Demos

Each of these is reproducible from a clean checkout; see
[Running it](#running-it).

### Self-heal after a control plane restart

Kill the control plane while the worker keeps running, then restart it. The
worker survives, detects that it is no longer known, and re-registers itself —
without ever exiting.

Worker — nine failed beats, no exit, then `NotFound` triggers re-registration:

```
2026/07/10 21:45:00 registered ok. id=4e4c4bab-5451-48bc-a426-98c2ea8ede22 heartbeat every 1000ms
2026/07/10 21:45:08 heartbeat failed: rpc error: code = Unavailable desc = ... connect: connection refused
                    ... seven more identical failures, one per tick ...
2026/07/10 21:45:16 heartbeat failed: rpc error: code = Unavailable desc = ... connect: connection refused
2026/07/10 21:45:17 heartbeat: not found, re-registering
```

Control plane after restart — same worker id, no manual intervention:

```
2026/07/10 21:45:15 control plane listening on :50051
2026/07/10 21:45:17 registered worker id=4e4c4bab-5451-48bc-a426-98c2ea8ede22 addr=localhost:60001 capacity=10
2026/07/10 21:45:18 heartbeat from 4e4c4bab-5451-48bc-a426-98c2ea8ede22 load=0
```

The id is identical on both sides. That is what makes this a resurrection
rather than the arrival of a new worker — the registry entry is rebuilt for the
same identity, and any in-flight bookkeeping keyed on that id stays valid.

### Eviction on crash

`kill -9` a worker and watch φ climb from 0 to the cap within three scan ticks,
about two seconds, after which the worker is evicted from the registry.

```
10:37:29 SCAN worker=cf682942 state=0 phi=-0.00 count=54   # last healthy beat
10:37:30 SCAN worker=cf682942 state=0 phi=0.00  count=54   # kill -9 here; silence begins
10:37:30 SCAN worker=cf682942 state=0 phi=4.60  count=54   # ~1.2s silent: crosses PhiSuspect
10:37:31 SCAN worker=cf682942 state=1 phi=20.00 count=54   # ~1.5s silent: caps, ALIVE->DEAD
10:37:31 DETECTOR: worker cf682942-afec-4a3f-a80f-d26f04f2958d declared DEAD
```

`count` freezes at 54: silence is read as *no arrival*, never as an arrival
with gap 0 — which would poison the inter-arrival distribution and suppress the
score exactly when it is needed.

The ramp is explosive because the distribution is tight. Fifty-four beats at
~1s with sub-50ms jitter fit a very narrow normal, so the first sample past the
mean is already deep in the tail. That is the adaptive payoff over a fixed
timeout: a confident distribution declares death fast, with no hand-tuned
deadline. A jittery worker would linger in SUSPECT instead, and recover if its
beats resumed.

The `SCAN` line was a temporary probe, removed after this run.

### Fast-path reroute, no duplication

`kill -9` a worker _mid-request_. The transport error surfaces in milliseconds,
the gateway reroutes, and the client is answered well before the detector even
reaches its verdict. No dedup line appears — there is nothing to adjudicate.

```
23:11:43.914 gateway: attempt failed req=k1 worker=b55c3ddd: code = Unavailable desc = error reading from server: EOF
23:11:43.934 worker A: Generate: start req=k1                  <- 20ms later, already recomputing
23:11:44.235 gateway: req=k1 served by worker=9c4064e5
23:11:45.182 control plane: DETECTOR: worker b55c3ddd declared DEAD   <- 947ms too late to matter
23:11:45.333 gateway: 1 worker(s) left the view: [b55c3ddd]
```

Crash to answered client: **~320ms**. The failure detector reached its verdict
a full second after the client already had its answer, and the poll diff that
followed found nothing to reroute — the request had been unregistered.

No dedup line appears anywhere. Same binary that logs one in the demo below;
the difference is not the code but whether the failure was *observable*. A
crash is reported by the transport itself, so the failed attempt is known-dead
rather than suspected-dead, and replacing it is safe by construction.

Note the ~1.7ms of orchestration this costs. Measured separately against
unaffected requests in the same batch: 300ms of mock inference, 301.7ms
observed — detecting the failure, picking a replacement, and re-dispatching
fits in under two milliseconds. That figure is the *crash* number, though, not
the failure number: it depends on the transport returning an error. A
partitioned host sends no reset, so the dial hangs to the attempt timeout and
only the slow path below can recover it.

### False positive, reroute, and first-wins deduplication

Freeze a worker mid-request with `SIGSTOP`. It is misjudged dead, its request is
rerouted, and for several seconds two attempts compute the same request on two
machines. On `SIGCONT` the original result arrives, loses the race, and is
discarded — one client, one answer.

```
22:37:14.798 gateway: 1 worker(s) left the view: [8cc9badf]
22:37:14.798 gateway: rerouting req=z1 to worker=b9601c1a addr=localhost:60001
22:37:15.120 gateway: req=z1 served by worker=b9601c1a
22:37:21.446 dedup: attempt from worker=8cc9badf lost the race req=z1
```

The client's response, six seconds before the loser reported in:

```json
{ "requestId": "z1", "output": "mock-output for: hi",
  "workerId": "b9601c1a-0e7c-44f1-947b-3ad9c7b4138b" }
```

Two attempts computed the same request concurrently for **6.6 seconds**. One
side effect. The frozen worker was healthy the whole time — on `SIGCONT` its
heartbeat returned `NotFound` and it re-registered under the same id, which is
self-heal and eviction composing without either knowing about the other.

This is the scenario the whole design exists for, and it is the one that cannot
be demonstrated by killing a process: a crashed worker is provably finished, so
there is never a second attempt to adjudicate. Only a worker that is alive and
silent produces one.

The same sequence runs as a deterministic test —
`TestReroute_FrozenAttemptLosesTheRace` in `internal/gateway/tracker_test.go`.
The dispatch seam makes "frozen" a goroutine parked on a channel, so the race
is reproduced with no signals, no sleeps, and no timing window. If the
adjudication latch regresses, the second result arrives and the test fails; the
first implementation of it used the channel buffer as the claim token and
silently logged no dedup line at all.

## Running it

Requires Go 1.26+ and [grpcurl](https://github.com/fullstorydev/grpcurl).

Build first. Do **not** use `go run` for the failure demos: it forks a child
process, so signals land on the wrapper while the actual worker keeps
heartbeating, and the detector never sees it die.

```bash
go build -o bin/controlplane ./cmd/controlplane
go build -o bin/worker       ./cmd/worker
go build -o bin/gateway      ./cmd/gateway
```

Four processes, each in its own terminal:

```bash
./bin/controlplane                              # :50051
./bin/worker -addr localhost:60001              # 300ms mock inference latency
./bin/worker -addr localhost:60002 -delay 10s   # slow on purpose, for race demos
./bin/gateway                                   # :50052
```

Inspect the cluster view (server reflection is enabled in development):

```bash
grpcurl -plaintext localhost:50051 inference.v1.ControlPlane/ListWorkers
```

Send an inference request through the data path:

```bash
grpcurl -plaintext -d '{"request_id":"r1","prompt":"hi"}' \
  localhost:50052 inference.v1.InferenceService/Generate
```

### Reproducing the deduplication demo

Worker selection is uniform random, so send requests until one lands on the slow
worker — it blocks for ten seconds, and that is the window. Freeze it
mid-request:

```bash
kill -STOP $(pgrep -f "bin/worker -addr localhost:60002")
```

The detector declares it dead, the gateway notices it leave the view, and the
other worker answers the client. Now resume it:

```bash
kill -CONT $(pgrep -f "bin/worker -addr localhost:60002")
```

Its late result arrives and loses: one `dedup: ... lost the race` line in the
gateway log, and the client never saw a second answer.

## Testing

```bash
go test -race ./...
```

The race detector is treated as a first-class practice, not an afterthought. The
registry's copy-on-read design exists because a `-race` run proved that handing
out `*WorkerInfo` pointers let callers read fields outside the mutex that was
supposed to protect them.

## Layout

```
cmd/            controlplane, worker, gateway entry points
internal/
  registry/     worker facts: registrations and last-seen timestamps
  detector/     phi-accrual suspicion scoring and eviction
  gateway/      forwarding, request tracking, first-wins adjudication
  controlplane/ gRPC service implementation
proto/          service definitions
```
