package gateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

// ── helpers ──────────────────────────────────────────────────────────────

func workerView(ids ...string) []*inferencev1.WorkerInfo {
	out := make([]*inferencev1.WorkerInfo, 0, len(ids))
	for _, id := range ids {
		out = append(out, &inferencev1.WorkerInfo{WorkerId: id, Address: id + ":0"})
	}
	return out
}

// recorder is a fake dispatch. It records every call in order and delegates the
// outcome to respond, so a test states its scenario as a function of which
// worker was asked (or of how many calls have happened) instead of racing a
// terminal.
type recorder struct {
	mu      sync.Mutex
	calls   []string
	respond func(workerID string, callNo int) (*inferencev1.InferenceResponse, error)
}

func (r *recorder) dispatch(
	_ context.Context,
	target *inferencev1.WorkerInfo,
	_ *inferencev1.InferenceRequest,
) (*inferencev1.InferenceResponse, error) {

	id := target.GetWorkerId()
	r.mu.Lock()
	r.calls = append(r.calls, id)
	callNo := len(r.calls)
	r.mu.Unlock()

	return r.respond(id, callNo)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) distinct() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]bool, len(r.calls))
	for _, id := range r.calls {
		out[id] = true
	}
	return out
}

func newTestGateway(rec *recorder, workerIDs ...string) *Gateway {
	g := New(nil)
	g.workers = workerView(workerIDs...)
	g.dispatch = rec.dispatch
	return g
}

func alwaysUnavailable(string, int) (*inferencev1.InferenceResponse, error) {
	return nil, status.Error(codes.Unavailable, "connection refused")
}

func request(id string) *inferencev1.InferenceRequest {
	return &inferencev1.InferenceRequest{RequestId: id, Prompt: "hi"}
}

// ── the regression this file exists for ──────────────────────────────────

// Two workers dying inside one poll cycle used to send a single request
// through 86,351 dispatches: the candidate set excluded only the worker that
// had just failed, so it alternated between the two dead nodes, and the
// attempts counter — decremented and re-incremented every round — sat at 1 and
// never opened the exhausted gate. Nothing inside the retry path stopped it;
// the failure detector did, by evicting both workers from the view.
func TestGenerate_RetryIsBounded(t *testing.T) {
	rec := &recorder{respond: alwaysUnavailable}
	g := newTestGateway(rec, "w1", "w2")

	if _, err := g.Generate(context.Background(), request("r1")); err == nil {
		t.Fatal("expected an error when no worker can be reached")
	}
	if got := rec.count(); got > maxAttempts {
		t.Fatalf("dispatched %d times; the budget is %d", got, maxAttempts)
	}
}

// Two termination conditions exist and they are not the same one at different
// scales. With only two workers the CANDIDATE SET runs out first: both land in
// tried, eligible comes back empty, and the budget check is true on every
// iteration — it never fires.
func TestGenerate_TriedSetTerminatesWhenCandidatesRunOut(t *testing.T) {
	rec := &recorder{respond: alwaysUnavailable}
	g := newTestGateway(rec, "w1", "w2")

	if _, err := g.Generate(context.Background(), request("r1")); err == nil {
		t.Fatal("expected an error when no worker can be reached")
	}
	if got := rec.count(); got != 2 {
		t.Fatalf("dispatched %d times; expected one per worker", got)
	}
}

// With four workers the BUDGET fires first, and it stops with a candidate that
// was never tried. This is why tried cannot replace maxAttempts: the candidate
// set is live, and during a rolling restart fresh workers keep arriving faster
// than tried can absorb them.
func TestGenerate_BudgetTerminatesWithCandidatesLeft(t *testing.T) {
	rec := &recorder{respond: alwaysUnavailable}
	g := newTestGateway(rec, "w1", "w2", "w3", "w4")

	if _, err := g.Generate(context.Background(), request("r1")); err == nil {
		t.Fatal("expected an error when no worker can be reached")
	}
	if got := rec.count(); got != maxAttempts {
		t.Fatalf("dispatched %d times; the budget is %d", got, maxAttempts)
	}
	if tried := rec.distinct(); len(tried) >= 4 {
		t.Fatalf("all 4 workers were tried; the budget was not the terminator")
	}
}

// Exhaustion reports Unavailable rather than leaking the transport error. A
// bare "connection refused" reads to a client as its own network problem;
// Unavailable says the cluster could not serve this and a later retry may work.
func TestGenerate_ExhaustionReportsUnavailable(t *testing.T) {
	rec := &recorder{respond: alwaysUnavailable}
	g := newTestGateway(rec, "w1", "w2")

	_, err := g.Generate(context.Background(), request("r1"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got code %v, want Unavailable (err: %v)", status.Code(err), err)
	}
}

// ── error classification ─────────────────────────────────────────────────

// A terminal error is about the REQUEST, so no other worker will do better.
// Retrying it would turn one failure into N, during a failure. This branch had
// never executed before this test.
func TestGenerate_TerminalErrorIsNotRetried(t *testing.T) {
	rec := &recorder{
		respond: func(string, int) (*inferencev1.InferenceResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "bad prompt")
		},
	}
	g := newTestGateway(rec, "w1", "w2", "w3")

	_, err := g.Generate(context.Background(), request("r1"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got code %v, want InvalidArgument", status.Code(err))
	}
	if got := rec.count(); got != 1 {
		t.Fatalf("dispatched %d times; a terminal error must not be retried", got)
	}
}

func TestRetryable(t *testing.T) {
	cases := []struct {
		code codes.Code
		want bool
	}{
		{codes.Unavailable, true},      // the machine failed us
		{codes.DeadlineExceeded, true}, // the deadline did
		{codes.InvalidArgument, false}, // the request is wrong everywhere
		{codes.NotFound, false},
		{codes.Internal, false}, // ambiguous, deliberately conservative
		{codes.Unknown, false},
	}
	for _, c := range cases {
		if got := retryable(status.Error(c.code, "x")); got != c.want {
			t.Errorf("retryable(%v) = %v, want %v", c.code, got, c.want)
		}
	}
}

// ── reroute succeeds ─────────────────────────────────────────────────────

// The failure is keyed on call number, not on worker id: pickWorker is uniform
// random, so keying on "the dead one" would make this test pass vacuously
// whenever the live worker happened to be picked first. That exact vacuous
// pass happened once during manual testing with 4 workers and 1 killed — a 3/4
// chance of asserting nothing.
func TestGenerate_ReroutesAfterFastFailure(t *testing.T) {
	rec := &recorder{
		respond: func(id string, callNo int) (*inferencev1.InferenceResponse, error) {
			if callNo == 1 {
				return nil, status.Error(codes.Unavailable, "connection refused")
			}
			return &inferencev1.InferenceResponse{RequestId: "r1", WorkerId: id}, nil
		},
	}
	g := newTestGateway(rec, "w1", "w2")

	resp, err := g.Generate(context.Background(), request("r1"))
	if err != nil {
		t.Fatalf("reroute should have succeeded: %v", err)
	}
	if got := rec.count(); got != 2 {
		t.Fatalf("dispatched %d times; want one failure and one success", got)
	}
	if resp.GetWorkerId() == "" {
		t.Fatal("response carries no worker id")
	}
}

// ── adjudication ─────────────────────────────────────────────────────────

// The latch is a one-way CAS and the channel merely carries the winner's
// result. An earlier design used the cap-1 buffer itself as the token, which
// broke silently: the handler's receive emptied the slot, so a late loser found
// it free and believed it had won. The only symptom was a log line that never
// appeared. Run with -race.
func TestSettle_ExactlyOneWinner(t *testing.T) {
	ih := &inflight{
		req:    request("r1"),
		result: make(chan attemptResult, 1),
		tried:  make(map[string]bool),
	}

	const n = 64
	var wins int64
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if ih.settle(attemptResult{workerID: fmt.Sprintf("w%d", i)}) {
				atomic.AddInt64(&wins, 1)
			}
		}(i)
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("%d attempts believed they won; exactly one may", wins)
	}
	if len(ih.result) != 1 {
		t.Fatalf("result channel holds %d values; want exactly 1", len(ih.result))
	}
}

// A straggler timing out 30s later must not re-dispatch a request the client
// was answered for long ago. This branch had never executed before this test.
func TestAttemptFailed_StragglerDoesNotDispatch(t *testing.T) {
	rec := &recorder{respond: alwaysUnavailable}
	g := newTestGateway(rec, "w1", "w2", "w3")

	ih, err := g.tracker.register(request("r1"), "w1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	ih.settle(attemptResult{workerID: "winner"}) // race already decided

	g.attemptFailed(ih, status.Error(codes.Unavailable, "late"), "w1")

	if got := rec.count(); got != 0 {
		t.Fatalf("a straggler dispatched %d attempt(s) for an answered request", got)
	}
}

// ── admission ────────────────────────────────────────────────────────────

func TestRegister_RejectsDuplicateRequestID(t *testing.T) {
	tr := NewRequestTracker()

	if _, err := tr.register(request("r1"), "w1"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	_, err := tr.register(request("r1"), "w2")
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("got code %v, want AlreadyExists", status.Code(err))
	}
}
