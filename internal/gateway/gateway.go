package gateway

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

// Gateway is the data-plane entry point. To the client it is an
// InferenceService server; to the workers it is an InferenceService client.
// Same service interface, both roles — a forwarding proxy.
//
// 4a scope: happy path only. No RequestTracker, no first-wins, no reroute —
// those are 4b (hand-written core). This is the geundament they sit on.
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
}

func New(cpClient inferencev1.ControlPlaneClient) *Gateway {
	return &Gateway{
		cpClient: cpClient,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

// PollLoop refreshes the local worker view until ctx is cancelled. Pull model:
// the gateway learns membership by polling ListWorkers, not by subscribing to
// events. In 4b, diffing successive polls is how the gateway notices a worker
// vanished (route X) and triggers reroute — no cross-process event channel.
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

func (g *Gateway) pollOnce(ctx context.Context) {
	resp, err := g.cpClient.ListWorkers(ctx, &inferencev1.ListWorkersRequest{})
	if err != nil {
		// Log-and-continue: a failed poll just means we keep the stale view
		// until the next tick. Don't clear the view on a transient error —
		// that would blank out routing on one hiccup.
		log.Printf("gateway poll: ListWorkers failed: %v", err)
		return
	}

	g.mu.Lock()
	g.workers = resp.GetWorkers()
	g.mu.Unlock()
}

// pickWorker returns a healthy worker to route to. 4a: uniform random pick.
// Load-aware / least-outstanding selection is M4 — deliberately dumb here.
func (g *Gateway) pickWorker() (*inferencev1.WorkerInfo, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.workers) == 0 {
		return nil, fmt.Errorf("no healthy workers available")
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
	// 4b/M4: evict a dead worker's conn when it leaves the view. 4a leaks one
	// idle conn per departed worker — harmless at demo scale.
	return conn, nil
}

// Generate is the client-facing entry point. Pick a worker, forward the RPC,
// return its result. 4a is single-attempt: no reroute, no dedup. If the chosen
// worker fails, the error propagates to the client (4b makes this resilient).
func (g *Gateway) Generate(
	ctx context.Context, req *inferencev1.InferenceRequest,
) (*inferencev1.InferenceResponse, error) {

	worker, err := g.pickWorker()
	if err != nil {
		return nil, err
	}

	conn, err := g.connFor(worker.GetAddress())
	if err != nil {
		return nil, err
	}

	workerClient := inferencev1.NewInferenceServiceClient(conn)

	log.Printf("gateway: forwarding req=%s to worker=%s addr=%s",
		req.GetRequestId(), worker.GetWorkerId(), worker.GetAddress())

	// Forward. 4a: the client's ctx flows straight through to the worker, so a
	// client cancel / deadline cancels the worker call too. 4b introduces
	// per-attempt child contexts so reroute can manage attempts independently.
	resp, err := workerClient.Generate(ctx, req)
	if err != nil {
		log.Printf("gateway: worker=%s Generate failed req=%s: %v",
			worker.GetWorkerId(), req.GetRequestId(), err)
		return nil, err
	}

	return resp, nil
}

// Close tears down all cached worker connections. Called on shutdown.
func (g *Gateway) Close() {
	g.connsMu.Lock()
	defer g.connsMu.Unlock()
	for addr, conn := range g.conns {
		conn.Close()
		delete(g.conns, addr)
	}
}
