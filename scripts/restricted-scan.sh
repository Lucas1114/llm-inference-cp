#!/usr/bin/env bash
#
# Scans every tracked file, and every commit message, for the restricted
# strings. This is the authority for done-bar item 9; a hand-typed grep is not.
#
#   ./scripts/restricted-scan.sh          # report, and fail if the count moved
#   ./scripts/restricted-scan.sh -v       # also print every hit with its line
#   ./scripts/restricted-scan.sh -u       # accept the current count as the new
#                                         # baseline (prints what to paste)
#
# WHY THIS EXISTS. The first inventory was taken with an ad-hoc grep that
# excluded *.pb.go as generated noise. gen/inference/v1/inference.pb.go is
# tracked and committed, protoc had copied a restricted string into it from the
# proto, and the count came out one short. The number was corrected by hand,
# which fixes a number and leaves the method that produced it intact. An
# acceptance check with a blind spot is not an acceptance check.
#
# THE RULE THAT FOLLOWS FROM THAT. The file set is exactly `git ls-files` --
# every tracked file, whatever its extension, whatever directory it sits in.
# There is no default exclusion. Anything excluded has to be written below with
# a reason, in this file, where it can be argued with.
#
# CURRENT EXCLUSIONS: one, this script itself.
#
# It has to name all thirteen strings in order to search for them, so scanning
# itself it reports its own vocabulary as violations -- which it did, on the
# commit that introduced it. The exclusion is one exact path, never a pattern,
# and it is printed on every run so it cannot quietly become the blind spot this
# script exists to remove. The cost of the exclusion is bounded and visible: a
# claim about the system hidden in this file's comments would be missed, and
# this file's comments are not where claims about the system live.
#
# Untracked paths are out of scope by construction rather than by exclusion:
# git ls-files does not list them. That covers notes/ and the private working
# files, and it matches section D of CLAUDE.md -- content no reader of the
# repository receives cannot make a claim on the repository's behalf.

set -euo pipefail
cd "$(dirname "$0")/.."

# Expected totals. Update only through a deliberate decision, never to make a
# red run go green.
EXPECT_FILE_LINES=12
EXPECT_COMMITS=1

VERBOSE=0
UPDATE=0
while [ $# -gt 0 ]; do
  case "$1" in
    -v|--verbose) VERBOSE=1 ;;
    -u|--update)  UPDATE=1 ;;
    -h|--help)    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

TERMS=(
  'stateless gateway'
  'idempotent deduplication'
  'exactly-once'
  'effectively-once'
  'highly available'
  'replicated'
  'leader election'
  'Raft'
  'etcd'
  'consensus'
  'fake clock'
  'clock abstraction'
  'managed Kubernetes clusters'
)

# Word-boundary the short terms so "raft" does not match "draft" and "etcd"
# does not match a longer identifier. The multi-word terms need no guard.
pattern_for() {
  case "$1" in
    Raft|etcd|consensus|replicated) printf '\\b%s\\b' "$1" ;;
    *) printf '%s' "$1" ;;
  esac
}

# A test coverage percentage. Deliberately narrow: the repository states
# phi-accrual false-positive probabilities as percentages, and those are not
# coverage numbers. Matching a bare "%" would flag them every run, and a check
# that cries wolf gets switched off.
COVERAGE_RE='(coverage[^.]{0,20}[0-9]+(\.[0-9]+)?[[:space:]]*%|[0-9]+(\.[0-9]+)?[[:space:]]*%[^.]{0,20}coverage|cover(ed|age)[[:space:]]*[:=][[:space:]]*[0-9])'

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

# Exact paths only. Each one needs a reason in the header above.
EXCLUDE=(
  'scripts/restricted-scan.sh'   # defines the vocabulary; would match itself
)

ALL=$(git ls-files)
FILES="$ALL"
for x in "${EXCLUDE[@]}"; do
  FILES=$(printf '%s\n' "$FILES" | grep -vxF "$x" || true)
done

NALL=$(printf '%s\n' "$ALL" | grep -c . || true)
NFILES=$(printf '%s\n' "$FILES" | grep -c . || true)

bold "Tracked files scanned: $NFILES of $NALL   (git ls-files)"
for x in "${EXCLUDE[@]}"; do
  printf '  excluded: %s\n' "$x"
done
echo

# ── files ────────────────────────────────────────────────────────────────────
FILE_HITS=$(mktemp)
for term in "${TERMS[@]}"; do
  pat=$(pattern_for "$term")
  printf '%s\n' "$FILES" | while IFS= read -r f; do
    [ -n "$f" ] || continue
    { grep -niE "$pat" -- "$f" 2>/dev/null || true; } | while IFS= read -r line; do
      printf '%s\t%s\t%s\n' "$term" "$f" "$line"
    done
  done
done >"$FILE_HITS"

printf '%s\n' "$FILES" | while IFS= read -r f; do
  [ -n "$f" ] || continue
  { grep -niE "$COVERAGE_RE" -- "$f" 2>/dev/null || true; } | while IFS= read -r line; do
    printf '%s\t%s\t%s\n' "test coverage percentage" "$f" "$line"
  done
done >>"$FILE_HITS"

# One file:line may carry two terms (README:59 has Raft and leader election).
# Count distinct LINES, which is what the inventory in CLAUDE.md indexes.
FILE_LINES=$(cut -f2,3 "$FILE_HITS" | cut -d: -f1,2 | sort -u | grep -c . || true)
TERM_HITS=$(grep -c . "$FILE_HITS" || true)

bold "Per-term counts (files)"
for term in "${TERMS[@]}" 'test coverage percentage'; do
  n=$(awk -F'\t' -v t="$term" '$1==t' "$FILE_HITS" | grep -c . || true)
  if [ "$n" -gt 0 ]; then
    printf '  %-30s %s\n' "$term" "$n"
  else
    printf '  %-30s %s\n' "$term" "0"
  fi
done
echo
printf '  distinct lines: %s      term hits: %s\n\n' "$FILE_LINES" "$TERM_HITS"

if [ "$VERBOSE" -eq 1 ]; then
  bold "Every hit"
  sort -t'	' -k2,2 "$FILE_HITS" | while IFS=$'\t' read -r term f line; do
    printf '  %s:%s\n    → %s\n    %s\n' "$f" "${line%%:*}" "$term" "${line#*:}"
  done
  echo
fi

# ── commit messages ──────────────────────────────────────────────────────────
#
# Read each message on its own, by hash. An earlier version streamed
# `--format='%H%x1f%s%x1f%b'` into `read -r h s b`, and read stops at the first
# newline -- so every multi-line body was truncated to its first line and the
# rest was never scanned. It reported clean while a restricted string sat in a
# body's thirteenth line. Slower and correct beats fast and blind.
COMMIT_HITS=$(mktemp)
for h in $(git log --all --format='%H'); do
  msg=$(git log -1 --format='%B' "$h")
  subj=$(git log -1 --format='%s' "$h")
  for term in "${TERMS[@]}"; do
    pat=$(pattern_for "$term")
    if printf '%s' "$msg" | grep -qiE "$pat"; then
      printf '%s\t%s\t%s\n' "$term" "${h:0:7}" "$subj"
    fi
  done
done | sort -u >"$COMMIT_HITS"

NCOMMITS=$(git rev-list --all --count)
COMMIT_LINES=$(cut -f2 "$COMMIT_HITS" | sort -u | grep -c . || true)

bold "Commit messages scanned: $NCOMMITS"
if [ "$COMMIT_LINES" -gt 0 ]; then
  while IFS=$'\t' read -r term h s; do
    printf '  %s  → %s\n    %s\n' "$h" "$term" "$s"
  done <"$COMMIT_HITS"
else
  printf '  no hits\n'
fi
echo

# ── verdict ──────────────────────────────────────────────────────────────────
TOTAL=$(( FILE_LINES + COMMIT_LINES ))
EXPECT_TOTAL=$(( EXPECT_FILE_LINES + EXPECT_COMMITS ))

if [ "$UPDATE" -eq 1 ]; then
  bold "Baseline to paste into this script:"
  printf '  EXPECT_FILE_LINES=%s\n  EXPECT_COMMITS=%s\n' "$FILE_LINES" "$COMMIT_LINES"
  exit 0
fi

if [ "$FILE_LINES" -eq "$EXPECT_FILE_LINES" ] && [ "$COMMIT_LINES" -eq "$EXPECT_COMMITS" ]; then
  green "PASS  $FILE_LINES file lines + $COMMIT_LINES commit = $TOTAL, unchanged"
  exit 0
fi

red "FAIL  $FILE_LINES file lines + $COMMIT_LINES commit = $TOTAL"
red "      expected $EXPECT_FILE_LINES + $EXPECT_COMMITS = $EXPECT_TOTAL"
echo
if [ "$TOTAL" -gt "$EXPECT_TOTAL" ]; then
  red "The inventory grew. Find what was added and remove it. Do NOT raise the"
  red "baseline to match -- that is the failure this check exists to catch."
else
  red "The inventory shrank. Something in the frozen set was edited or deleted."
  red "Restore it; existing occurrences are immutable (CLAUDE.md B.2)."
fi
red "Run with -v to see every hit."
exit 1
