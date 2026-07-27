package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

const (
	workerCapacity  = 10
	shutdownTimeout = 3 * time.Second
)

// inferenceServer implements the worker side of InferenceService.Generate.
// Unlike the gateway's Generate (which picks a worker and forwards), the
// worker's Generate is where the actual (mock) inference happens.
type inferenceServer struct {
	inferencev1.UnimplementedInferenceServiceServer
	workerID string

	// delay simulates time-in-flight. Per-instance rather than a package
	// constant so two workers in the same demo can be asymmetric — a slow
	// worker is how we provoke the false-positive that reroute must survive.
	delay time.Duration
}

// Generate is the mock inference handler. It sleeps to simulate compute, then
// echoes back a result tagged with this worker's id so first-wins on the
// gateway side is observable (you can see WHICH attempt won).
func (s *inferenceServer) Generate(
	ctx context.Context, req *inferencev1.InferenceRequest,
) (*inferencev1.InferenceResponse, error) {

	log.Printf("Generate: start req=%s prompt=%q", req.GetRequestId(), req.GetPrompt())

	// Simulate time-in-flight, but stay cancellable. A bare time.Sleep would
	// block the full delay even after the caller cancels or the server starts
	// GracefulStop — the handler must observe ctx.Done() to stop early.
	// (This is the worker responding to ITS OWN shutdown / caller cancel — not
	// the router cancelling a "dead" worker, which we deliberately never do.)
	select {
	case <-time.After(s.delay):
		// compute finished normally
	case <-ctx.Done():
		// caller cancelled or server shutting down: abandon this request.
		log.Printf("Generate: cancelled req=%s: %v", req.GetRequestId(), ctx.Err())
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	output := "mock-output for: " + req.GetPrompt()

	log.Printf("Generate: done  req=%s worker=%s", req.GetRequestId(), s.workerID)

	return &inferencev1.InferenceResponse{
		RequestId: req.GetRequestId(),
		Output:    output,
		WorkerId:  s.workerID, // observability + first-wins visibility
	}, nil
}

func main() {

	// Millisecond resolution: 4b's whole story is two attempts racing, and
	// second-granularity timestamps can't show which one won.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Flags, not env vars: this is a demo binary launched by hand, and flags
	// self-document via -h. Every value here is per-process, which is exactly
	// what lets us run several workers side by side.
	var (
		addrFlag = flag.String("addr", "localhost:60001",
			"address this worker listens on and advertises to the control plane")
		cpFlag = flag.String("cp", "localhost:50051",
			"control plane address")
		delayFlag = flag.Duration("delay", 300*time.Millisecond,
			"simulated inference latency (e.g. 300ms, 2s)")
	)
	flag.Parse()

	workerID := uuid.NewString()
	workerAddr := *addrFlag

	conn, err := grpc.NewClient(
		*cpFlag,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to dial control plane: %v", err)
	}
	defer conn.Close()

	client := inferencev1.NewControlPlaneClient(conn)

	// Long-lived context, cancelled on Ctrl-C / SIGTERM. Shared shutdown signal
	// for BOTH the heartbeat loop and the InferenceService server below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	resp, err := client.Register(ctx, &inferencev1.RegisterRequest{
		WorkerId: workerID,
		Address:  workerAddr,
		Capacity: workerCapacity,
	})
	if err != nil {
		log.Fatalf("initial Register failed: %v", err) // startup failure: fail fast
	}

	log.Printf("registered ok. id=%s addr=%s delay=%s heartbeat every %dms",
		workerID, workerAddr, *delayFlag, resp.GetHeartbeatIntervalMs())

	interval := time.Duration(resp.GetHeartbeatIntervalMs()) * time.Millisecond

	// ── InferenceService server: the worker's inbound role. ──────────────
	// The worker is now BOTH a client (outbound conn to the control plane,
	// above) AND a server (inbound listener for the gateway's Generate calls).
	// Two roles, one process, independent connections.
	lis, err := net.Listen("tcp", workerAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", workerAddr, err)
	}

	grpcServer := grpc.NewServer()
	inferencev1.RegisterInferenceServiceServer(grpcServer, &inferenceServer{workerID: workerID, delay: *delayFlag})

	var wg sync.WaitGroup

	// Goroutine 1: serve InferenceService in the background. Serve blocks until
	// GracefulStop is called, so it can't run on the main goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("InferenceService listening on %s", workerAddr)
		if err := grpcServer.Serve(lis); err != nil {
			// Serve returns nil on GracefulStop; a non-nil error is a real
			// listener failure. Cancel ctx so the rest of the process unwinds
			// instead of hanging on a server that will never come up.
			log.Printf("InferenceService Serve error: %v", err)
			stop()
		}
	}()

	// Goroutine 2: bridge ctx cancellation to GracefulStop. On shutdown, drain
	// in-flight Generate calls (GracefulStop, not Stop) before the listener
	// closes — same "don't kill requests in flight" philosophy as the control
	// plane side.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Printf("shutting down InferenceService")
		grpcServer.GracefulStop()
	}()

	// Goroutine 3: heartbeat loop (unchanged).
	wg.Add(1)
	go heartbeatLoop(ctx, &wg, client, workerID, workerAddr, interval)

	wg.Wait()
	log.Printf("worker shut down cleanly")
}

// heartbeatLoop reports liveness every interval until ctx is cancelled.
// Liveness judgment belongs to the control plane's failure detector, not to
// the worker: a failed beat is logged and the next tick retries naturally.
func heartbeatLoop(ctx context.Context, wg *sync.WaitGroup,
	client inferencev1.ControlPlaneClient, id, addr string, interval time.Duration) {

	defer wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("heartbeat loop: shutting down")

			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()

			if _, err := client.Deregister(shutdownCtx, &inferencev1.DeregisterRequest{
				WorkerId: id,
			}); err != nil {
				log.Printf("deregister failed (failure detector will evict): %v", err)
			}

			// (future: stop accepting new requests, then drain in-flight here)
			return

		case <-ticker.C:
			_, err := client.Heartbeat(ctx, &inferencev1.HeartbeatRequest{
				WorkerId: id,
				Load:     &inferencev1.WorkerLoad{ActiveRequests: 0}, // M4: reflect real in-flight count
			})

			switch status.Code(err) {
			case codes.OK:
				// healthy tick

			case codes.NotFound:
				log.Printf("heartbeat: not found, re-registering")
				client.Register(ctx, &inferencev1.RegisterRequest{
					WorkerId: id,
					Address:  addr,
					Capacity: workerCapacity,
				})

			default:
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}
