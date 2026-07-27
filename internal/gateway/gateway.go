package gateway

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

// attemptTimeout bounds a single attempt's RPC. It exists because attempts run
// on a context detached from the client's (see attempt), so nothing else would
// ever reclaim an attempt aimed at a worker that has gone silent without
// closing its connection.
const attemptTimeout = 30 * time.Second

// Gateway is the data-plane entry point. To the client it is an
// InferenceService server; to the workers it is an InferenceService client.
// Same service interface, both roles — a forwarding proxy.
type Gateway struct {
	inferencev1.UnimplementedInferenceServiceServer

	cpClient inferencev1.ControlPlaneClient

	// workers: local view of healthy workers, refreshed by pollLoop.
	// pollLoop writes, Generate reads — guarded by mu.
	mu      sync.RWMutex
	workers []*inferencev1.WorkerInfo

	// conns: reuse gRPC connections to workers. A ClientConn is concurrency-safe,
	// long-lived, and self-reconnecting, so cache one per address rather than
	// dialing per request. Guarded by its own mutex (different contention
	// pattern from the view above).
	connsMu sync.Mutex
	conns   map[string]*grpc.ClientConn

	// tracker: every request currently in flight. Owns its own lock.
	tracker *RequestTracker
}

func New(cpClient inferencev1.ControlPlaneClient) *Gateway {
	return &Gateway{
		cpClient: cpClient,
		conns:    make(map[string]*grpc.ClientConn),
		tracker:  NewRequestTracker(),
	}
}

// PollLoop refreshes the local worker view until ctx is cancelled. Pull model:
// the gateway learns membership by polling ListWorkers, not by subscribing to
// events — the detector's DeadEvent channel lives inside the control plane
// process and cannot cross a process boundary.
func (g *Gateway) PollLoop(ctx context.Context, wg *sync.WaitGroup, interval time.Duration) {
	defer wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Poll once immediately so we don't serve a blank view for the first tick.
	g.pollOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("gateway poll loop: shutting down")
			return
		case <-ticker.C:
			g.pollOnce(ctx)
		}
	}
}

// pollOnce refreshes the view and, by diffing against the previous one, detects
// workers that have vanished. That diff is the SLOW failure path: the control
// plane's detector decided the worker was dead, and we find out one poll later.
//
// Slow, and uncertain: the vanished worker may still be alive and computing
// (a partitioned or paused process keeps its TCP connections open and reports
// no error to anyone). That uncertainty is precisely why this path can create
// duplicates, and why first-wins has to exist.
func (g *Gateway) pollOnce(ctx context.Context) {
	resp, err := g.cpClient.ListWorkers(ctx, &inferencev1.ListWorkersRequest{})
	if err != nil {
		// Log-and-continue: a failed poll just means we keep the stale view
		// until the next tick. Don't clear the view on a transient error —
		// that would blank out routing on one hiccup.
		log.Printf("gateway poll: ListWorkers failed: %v", err)
		return
	}
	fresh := resp.GetWorkers()

	g.mu.Lock()
	departed := departedIDs(g.workers, fresh)
	g.workers = fresh
	g.mu.Unlock()

	if len(departed) == 0 {
		return
	}
	log.Printf("gateway: %d worker(s) left the view: %v", len(departed), keysOf(departed))

	// Note we deliberately do NOT close cached connections to departed workers.
	// It looks like an obvious cleanup, but a departed worker may still be
	// running an attempt we launched — closing its connection would abort that
	// attempt, which is the very duplicate first-wins is here to adjudicate.
	// The leak is one idle ClientConn per departure; correctness wins.

	g.rerouteFrom(departed)
}

// departedIDs returns worker IDs present in old but absent from fresh.
func departedIDs(old, fresh []*inferencev1.WorkerInfo) map[string]bool {
	alive := make(map[string]bool, len(fresh))
	for _, w := range fresh {
		alive[w.GetWorkerId()] = true
	}
	departed := make(map[string]bool)
	for _, w := range old {
		if !alive[w.GetWorkerId()] {
			departed[w.GetWorkerId()] = true
		}
	}
	return departed
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// rerouteFrom re-dispatches every in-flight request stranded on a departed
// worker. Three-phase, mirroring the detector's scanOnce:
//
//	phase 1 (view lock)    snapshot candidate targets
//	phase 2 (tracker lock) find victims, claim them by reassigning
//	phase 3 (no locks)     fire the new attempts
//
// The phases exist to keep locks off the network path. Dispatching an RPC while
// holding the tracker lock would stall every registration behind a call that
// can hang for seconds.
func (g *Gateway) rerouteFrom(departed map[string]bool) {
	// Phase 1. Snapshot BEFORE taking the tracker lock: choosing a target reads
	// the view under g.mu, and nesting the view lock inside the tracker lock
	// establishes a lock order that some future code path will violate in the
	// other direction. Two locks that never nest cannot invert.
	healthy := g.healthyExcept(departed)
	if len(healthy) == 0 {
		log.Printf("gateway: reroute skipped, no healthy workers left")
		return
	}

	type job struct {
		reqID  string
		ih     *inflight
		target *inferencev1.WorkerInfo
	}
	var jobs []job

	// Phase 2.
	g.tracker.mu.Lock()
	for reqID, ih := range g.tracker.requests {
		if !departed[ih.assigned] {
			continue
		}
		target := healthy[rand.Intn(len(healthy))]

		// Reassignment IS the claim. Doing it here, under the lock, means a
		// later scan can no longer see this request as stranded — without
		// that, a second poll tick arriving before dispatch would rescue the
		// same request twice and put a third attempt in the air.
		ih.assigned = target.GetWorkerId()

		// Increment, not replace: on this path the old attempt may still be
		// alive and computing. That is the definition of the slow path.
		ih.attempts++

		jobs = append(jobs, job{reqID: reqID, ih: ih, target: target})
	}
	g.tracker.mu.Unlock()

	// Phase 3.
	for _, j := range jobs {
		log.Printf("gateway: rerouting req=%s to worker=%s addr=%s",
			j.reqID, j.target.GetWorkerId(), j.target.GetAddress())
		go g.attempt(j.ih, j.target)
	}
}

// healthyExcept snapshots the current view minus the excluded worker IDs.
func (g *Gateway) healthyExcept(exclude map[string]bool) []*inferencev1.WorkerInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make([]*inferencev1.WorkerInfo, 0, len(g.workers))
	for _, w := range g.workers {
		if exclude[w.GetWorkerId()] {
			continue
		}
		out = append(out, w)
	}
	return out
}

// pickWorker returns a worker to route to. Uniform random; load-aware and
// KV-cache-aware selection is M4.
func (g *Gateway) pickWorker() (*inferencev1.WorkerInfo, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.workers) == 0 {
		return nil, status.Error(codes.Unavailable, "no healthy workers available")
	}
	return g.workers[rand.Intn(len(g.workers))], nil
}

// connFor returns a cached connection to addr, dialing and caching on first use.
// grpc.NewClient is lazy (no eager handshake), so this is cheap; the connection
// establishes on first RPC.
func (g *Gateway) connFor(addr string) (*grpc.ClientConn, error) {
	g.connsMu.Lock()
	defer g.connsMu.Unlock()

	if conn, ok := g.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial worker %s: %w", addr, err)
	}
	g.conns[addr] = conn
	return conn, nil
}

// Generate is the client-facing entry point: register, dispatch one attempt,
// wait for whichever attempt lands first.
//
// The handler never talks to a worker itself. It parks on the adjudication
// channel and lets attempt goroutines race — which is what allows an attempt it
// never launched (a reroute) to answer on its behalf.
func (g *Gateway) Generate(
	ctx context.Context, req *inferencev1.InferenceRequest,
) (*inferencev1.InferenceResponse, error) {

	reqID := req.GetRequestId()
	if reqID == "" {
		// The id is the deduplication key for the whole pipeline. Without it
		// there is nothing to be idempotent about, so reject rather than
		// invent one — a gateway-minted id would not survive a client retry.
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}

	worker, err := g.pickWorker()
	if err != nil {
		return nil, err
	}

	ih, err := g.tracker.register(req, worker.GetWorkerId())
	if err != nil {
		return nil, err
	}
	defer g.tracker.unregister(reqID)

	go g.attempt(ih, worker)

	select {
	case r := <-ih.result:
		if r.err != nil {
			return nil, r.err
		}
		log.Printf("gateway: req=%s served by worker=%s", reqID, r.workerID)
		return r.resp, nil

	case <-ctx.Done():
		// The client gave up. Attempts keep running on their detached contexts
		// and will lose harmlessly when they finish — settle never blocks, so
		// nothing leaks by having no receiver left.
		log.Printf("gateway: client gave up req=%s: %v", reqID, ctx.Err())
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// attempt performs one RPC to one worker and feeds the outcome into the
// adjudication channel.
func (g *Gateway) attempt(ih *inflight, target *inferencev1.WorkerInfo) {
	workerID := target.GetWorkerId()

	conn, err := g.connFor(target.GetAddress())
	if err != nil {
		g.attemptFailed(ih, err, workerID)
		return
	}
	client := inferencev1.NewInferenceServiceClient(conn)

	// A per-attempt context detached from the client's, deliberately.
	//
	// Deriving from the client context would cancel every outstanding attempt
	// the moment the handler returns — including the suspected-dead one whose
	// result we specifically want to observe arriving late and losing. We never
	// cancel a suspect: the failure detector's verdict is a probabilistic
	// guess, and acting on a guess by killing possibly-good work is worse than
	// letting it finish and discarding the answer. The timeout is what bounds
	// the waste.
	ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
	defer cancel()

	resp, err := client.Generate(ctx, ih.req)
	if err != nil {
		g.attemptFailed(ih, err, workerID)
		return
	}

	ih.settle(attemptResult{resp: resp, workerID: workerID})
}

// attemptFailed handles one attempt's failure: decide whether another worker
// could do better, replace the attempt if so, and surface the error only when
// nothing is left in flight.
//
// This is also the FAST failure path. When the transport reports Unavailable
// the attempt is provably finished, so replacing it here creates no duplicate
// — a pure latency optimisation over waiting a poll cycle for the detector's
// verdict. It is blind to exactly what the slow path catches: a frozen worker
// returns no error, so nothing ever calls this function for it.
func (g *Gateway) attemptFailed(ih *inflight, err error, workerID string) {
	log.Printf("gateway: attempt failed req=%s worker=%s: %v",
		ih.req.GetRequestId(), workerID, err)

	// Race already decided: nothing left to do. Without this, every straggler
	// that eventually times out re-dispatches a request the client was
	// answered for long ago.
	if ih.claimed.Load() {
		return
	}

	// Terminal: this IS the answer, no worker will do better. Settle
	// immediately rather than burning the fleet on a request that cannot
	// succeed anywhere.
	//
	// Note the deliberate asymmetry: a terminal error outranks a slower
	// success that may still be in flight. That is only sound because
	// "terminal" means deterministic — a claim underwritten entirely by
	// retryable() being accurate.
	if !retryable(err) {
		ih.settle(attemptResult{err: err, workerID: workerID})
		return
	}

	// Snapshot candidates BEFORE taking the tracker lock: healthyExcept takes
	// the view lock, and nesting the two establishes a lock order that some
	// later code path will violate in the other direction. Two locks that
	// never nest cannot invert.
	candidates := g.healthyExcept(map[string]bool{workerID: true})

	var replacement *inferencev1.WorkerInfo
	var exhausted bool

	g.tracker.mu.Lock()
	ih.attempts-- // this attempt is over

	if len(candidates) > 0 {
		// Random, not candidates[0]: a worker dying with N requests on it
		// would otherwise dump all N onto the same survivor, converting one
		// failure into a load spike on a node already absorbing extra work.
		replacement = candidates[rand.Intn(len(candidates))]

		// Reassignment under the lock IS the claim — after this, a poll diff
		// can no longer see the request as stranded and rescue it a second
		// time.
		ih.assigned = replacement.GetWorkerId()
		ih.attempts++
	}

	// Read the counter while still holding the lock; act on it outside.
	exhausted = ih.attempts == 0
	g.tracker.mu.Unlock()

	if replacement != nil {
		// Synchronous, not `go`: this function already runs on the failed
		// attempt's goroutine, so continuing on it keeps the attempt count and
		// the goroutine count in step. Chained failures recurse into a single
		// sequential chain instead of fanning out.
		g.attempt(ih, replacement)
		return
	}

	if exhausted {
		// Nobody is left to answer. Only now may an error become the client's
		// result — settling unconditionally above would let every fast failure
		// beat a slower success and silently disable rerouting altogether.
		ih.settle(attemptResult{err: err, workerID: workerID})
	}
}

// Close tears down all cached worker connections. Called on shutdown, after
// GracefulStop has drained in-flight forwards that are still using them.
func (g *Gateway) Close() {
	g.connsMu.Lock()
	defer g.connsMu.Unlock()
	for addr, conn := range g.conns {
		conn.Close()
		delete(g.conns, addr)
	}
}
