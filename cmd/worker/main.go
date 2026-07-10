package main

import (
	"context"
	"log"
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
	controlPlaneAddr = "localhost:50051"
	workerCapacity   = 10
)

func main() {
	workerID := uuid.NewString()
	workerAddr := "localhost:60001"

	conn, err := grpc.NewClient(
		controlPlaneAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to dial control plane: %v", err)
	}
	defer conn.Close()

	client := inferencev1.NewControlPlaneClient(conn)

	// Long-lived context, cancelled on Ctrl-C / SIGTERM. This is the shutdown
	// signal the heartbeat loop watches.
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

	log.Printf("registered ok. id=%s heartbeat every %dms",
		workerID, resp.GetHeartbeatIntervalMs())

	// The control plane is the source of truth for heartbeat cadence; convert
	// its milliseconds into Go's duration type.
	interval := time.Duration(resp.GetHeartbeatIntervalMs()) * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1) // must happen before `go`
	go heartbeatLoop(ctx, &wg, client, workerID, workerAddr, interval)

	wg.Wait() // wait for the loop to exit cleanly before main returns
	log.Printf("worker shut down cleanly")
}

// heartbeatLoop reports liveness every interval until ctx is cancelled.
// Liveness judgment belongs to the control plane's failure detector, not to
// the worker: a failed beat is logged and the next tick retries naturally.
func heartbeatLoop(ctx context.Context, wg *sync.WaitGroup,
	client inferencev1.ControlPlaneClient, id, addr string, interval time.Duration) {

	defer wg.Done() // decrements no matter which path we exit through

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("heartbeat loop: shutting down")
			return

		case <-ticker.C:
			_, err := client.Heartbeat(ctx, &inferencev1.HeartbeatRequest{
				WorkerId: id,
				Load:     &inferencev1.WorkerLoad{ActiveRequests: 0}, // no real serving yet
			})

			switch status.Code(err) {
			case codes.OK:
				// healthy tick

			case codes.NotFound:
				// The control plane restarted and lost its registry. Re-register
				// with the same id: identity continuity is what makes this a
				// resurrection rather than a new worker.
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
