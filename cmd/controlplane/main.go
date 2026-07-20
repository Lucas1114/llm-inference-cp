package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
	"github.com/lucas1114/llm-inference-cp/internal/controlplane"
	"github.com/lucas1114/llm-inference-cp/internal/detector"
	"github.com/lucas1114/llm-inference-cp/internal/registry"
)

const listenAddr = ":50051"

func main() {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

	reg := registry.NewWorkerRegistry()
	srv := controlplane.NewServer(reg)

	grpcServer := grpc.NewServer()
	inferencev1.RegisterControlPlaneServer(grpcServer, srv)
	reflection.Register(grpcServer)

	// Failure detector: turns worker silence into ALIVE→SUSPECT→DEAD, evicts
	// dead workers from the registry, emits DeadEvents for the router (Phase 4).
	d := detector.New(reg, detector.Config{
		PollInterval: 500 * time.Millisecond,
		WindowSize:   100,
		PhiSuspect:   1,
		PhiDead:      8,
	})

	// Long-lived context, cancelled on Ctrl-C / SIGTERM. Both long-lived
	// components below wind down off this one signal.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	var wg sync.WaitGroup

	// (1) Detector scanner: ctx-driven, same shape as the worker's heartbeat
	// loop — returns on its own when ctx is cancelled.
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Run(ctx)
	}()

	// (2) gRPC server: does NOT watch ctx. Serve blocks until GracefulStop is
	// called from the shutdown path below. A non-nil err here means a real
	// failure (not a graceful stop), so cancel ctx to bring the detector down
	// too — otherwise main would wait forever on a signal that never comes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC Serve failed: %v", err)
			stop()
		}
	}()

	// (3) Temporary events drain (Phase 3 demo only). Phase 4's router replaces
	// this: it consumes DeadEvents to reroute in-flight requests. For now we log,
	// which keeps the buffered channel from filling and stalling the scanner, and
	// gives the kill -9 demo its visible "worker evicted" evidence line.
	events := d.Events()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				log.Printf("DETECTOR: worker %s declared DEAD at %s",
					ev.WorkerID, ev.At.Format(time.RFC3339))
			}
		}
	}()

	log.Printf("control plane listening on %s", listenAddr)

	<-ctx.Done()
	log.Printf("control plane: shutting down")

	grpcServer.GracefulStop() // bridge: unblock Serve so its goroutine returns
	wg.Wait()
	log.Printf("control plane shut down cleanly")
}
