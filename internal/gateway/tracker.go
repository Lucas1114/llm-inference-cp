package gateway

import (
	"log"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

// maxAttempts caps how many times ONE request may be dispatched, counting the
// original. It is a correctness bound, not a tuning knob: without it the retry
// path has no internal termination condition at all, and what actually stops it
// is the failure detector evicting the dead workers from the view — an
// unrelated component, on a ~2s timescale. Measured cost of that accident:
// 86,351 dispatches for a single request when two workers died together.
//
// Deliberately per-request. A per-request budget still amplifies total load
// during a correlated failure (every request on every dead worker retries at
// once); bounding that properly needs a CLUSTER-level retry budget — retry
// traffic capped as a fraction of total traffic, the approach gRPC's retry
// policy and the SRE Book both take. That belongs with M4 backpressure, where
// there is an admission controller to hang it on.
const maxAttempts = 3

// attemptResult is what one attempt (one RPC to one worker) produces.
// Exactly one of resp/err is set. workerID identifies who produced it, which
// is what makes first-wins observable in the logs.
type attemptResult struct {
	resp     *inferencev1.InferenceResponse
	err      error
	workerID string
}

// inflight is the rendezvous point for the three parties that touch a live
// request: the handler goroutine (waits for a result), one or more attempt
// goroutines (produce results), and the reroute path (reassigns).
//
// Fields fall into three synchronisation classes — that split is the design,
// not an accident:
//
//	immutable                → req
//	guarded by RequestTracker.mu → assigned, attempts, dispatched, tried
//	self-synchronising       → claimed (atomic), result (channel)
type inflight struct {
	// req is the original request, kept so the reroute path (which has no
	// access to the handler's call stack) can re-send it. It must be the SAME
	// object, request_id included: that id is the end-to-end idempotency key.
	// Re-sending under a fresh id would turn at-least-once delivery into
	// genuine duplicate execution and break effectively-once downstream.
	//
	// Never mutated — protobuf messages are not concurrency-safe, and this one
	// is read by every attempt goroutine simultaneously. If a future change
	// needs to tag a retry, proto.Clone it instead of writing in place.
	req *inferencev1.InferenceRequest

	// assigned is the worker ID of the CURRENT attempt target — not a history
	// of where this request has been. The reroute path scans for
	// assigned == <departed worker> to find which requests are stranded.
	//
	// Worker ID rather than address, deliberately: an address can be recycled
	// by a restarted process that is a different worker with a fresh id.
	// Identity is the id (same contract as heartbeat re-registration reusing
	// its id); addresses are looked up from the live view at dispatch time.
	assigned string

	// attempts counts attempts currently in flight. Registration starts it at
	// 1; reroute increments; a failed attempt decrements. Reaching zero means
	// nobody is left to answer, which is the only moment an error is allowed
	// to become the client's answer.
	attempts int

	// dispatched counts attempts EVER started for this request, across both
	// failure paths, and is the value maxAttempts bounds.
	//
	// It exists because attempts cannot serve as the budget: attempts is a
	// live gauge of what is in the air right now, so it rises on reroute and
	// falls on failure and is never monotonic. In the two-workers-die case it
	// sat at exactly 1 for all 86,351 iterations — a bound placed on it would
	// never have fired once. A budget must be spent, not borrowed.
	//
	// Both paths draw on the same counter deliberately. What the budget
	// protects is total cluster load during a failure, and the amplification
	// does not care which path caused it. Splitting it into two budgets would
	// require an argument for why the two thresholds differ, and there isn't
	// one.
	dispatched int

	// tried records workers this request has PROVABLY failed on — a target
	// that returned a real RPC error, never one the detector merely suspects.
	//
	// The distinction is load-bearing. A suspicion is revocable: a SIGSTOP'd
	// worker resumes, re-registers under the same id, and rejoins the view.
	// Blacklisting on a guess would permanently exclude a healthy node from
	// one request's candidate set. Evidence gets you blacklisted; suspicion
	// does not — the same principle as never cancelling a suspected worker.
	//
	// This is an efficiency measure, NOT the termination condition. The
	// candidate set is live: during a rolling restart fresh workers keep
	// entering the view and tried can never catch up with them. Only
	// maxAttempts terminates. Guarded by RequestTracker.mu.
	tried map[string]bool

	// claimed is the actual token. It cannot be the channel's buffer slot:
	// the handler's receive empties that slot, and a late loser would then
	// find it free and "win" a race that was decided seconds ago.
	// A one-way latch — false→true once, never back.
	claimed atomic.Bool

	// result is the adjudication point. Capacity is exactly 1 and that 1 is
	// load-bearing: the single buffer slot IS the claim token. First attempt
	// to land it wins; everyone else finds it full and walks away.
	//
	// Capacity 0 would leak goroutines — a late loser would block forever on a
	// send with no receiver left. Capacity 2 would let both attempts "succeed",
	// erasing the notion of a loser (and with it, the hook where a real
	// side-effecting worker would skip its duplicate work).
	//
	// Never closed: close is the sender's privilege, and with multiple senders
	// that privilege has no owner. A late send on a closed channel panics.
	result chan attemptResult
}

// settle publishes this attempt's result, first-wins. Returns true if this
// result is the one the client will see.
//
// Never blocks: a loser can arrive long after the handler returned, when no
// receiver exists at all. The default branch is what makes that safe — select
// takes it when no other case can proceed immediately.
func (ih *inflight) settle(r attemptResult) bool {
	if !ih.claimed.CompareAndSwap(false, true) {
		log.Printf("dedup: attempt from worker=%s lost the race req=%s",
			r.workerID, ih.req.GetRequestId())
		return false
	}
	// Exactly one send ever reaches this line, so cap 1 cannot block even
	// with no receiver left.
	ih.result <- r
	return true
}

// retryable reports whether err is about the MACHINE (another worker may well
// succeed) rather than about the REQUEST (no worker will do better).
//
// Getting this split wrong is how retry storms start: retrying a request that
// is invalid everywhere turns one failure into N, and does it during a
// failure, when the fleet can least afford the extra load.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		// The transport or the deadline failed us, not the request itself.
		return true
	default:
		// Everything else is terminal, including Internal and Unknown. Those
		// two are genuinely ambiguous; treating them as retryable would walk a
		// poisoned request around the whole fleet, lighting one fire per
		// worker. Note this also catches non-status errors (a dial failure
		// arrives as Unknown) — correct here, since grpc.NewClient only fails
		// on a malformed target, which no other worker would fix either.
		return false
	}
}

// RequestTracker holds every request currently in flight, keyed by request_id.
//
// One mutex covers both the map and the mutable fields of the inflight values
// it holds. Per-inflight locks were considered and rejected: contention here is
// low (reroute only runs when a worker dies), so a second lock would buy no
// throughput while adding a lock-ordering constraint to get wrong later.
type RequestTracker struct {
	mu       sync.Mutex
	requests map[string]*inflight
}

func NewRequestTracker() *RequestTracker {
	return &RequestTracker{requests: make(map[string]*inflight)}
}

// register admits a new request and returns its inflight record.
//
// A request_id already in flight is rejected rather than merged. Merging would
// be the richer behaviour (a client retrying over a flaky link would attach to
// the existing attempt — request-level idempotence at the gateway), but it
// needs a fan-out result path since a cap-1 channel can only satisfy one
// waiter. Out of scope for 4b; the rejection at least makes the collision loud
// instead of silently running two independent races under one id.
func (t *RequestTracker) register(
	req *inferencev1.InferenceRequest, workerID string,
) (*inflight, error) {

	t.mu.Lock()
	defer t.mu.Unlock()

	id := req.GetRequestId()
	if _, exists := t.requests[id]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "request %s already in flight", id)
	}

	ih := &inflight{
		req:        req,
		assigned:   workerID,
		attempts:   1,
		dispatched: 1, // the first dispatch spends budget too
		tried:      make(map[string]bool),
		result:     make(chan attemptResult, 1),
	}
	t.requests[id] = ih
	return ih, nil
}

// unregister removes a finished request. Attempts still running keep a pointer
// to the inflight record, so they can still call settle safely after this —
// they simply lose and get garbage collected. Losers travel by pointer, not by
// map lookup, which is exactly why removing the map entry early is harmless.
func (t *RequestTracker) unregister(reqID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.requests, reqID)
}
