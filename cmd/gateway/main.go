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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/lucas1114/llm-inference-cp/internal/gateway"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

const (
	controlPlaneAddr = "localhost:50051"
	gatewayAddr      = "localhost:50052"
	pollInterval     = 500 * time.Millisecond
)

func main() {

	// Millisecond resolution: 4b's whole story is two attempts racing, and
	// second-granularity timestamps can't show which one won.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	
	// Outbound: gateway is a ControlPlane client (to poll ListWorkers).
	conn, err := grpc.NewClient(
		controlPlaneAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to dial control plane: %v", err)
	}
	defer conn.Close()

	cpClient := inferencev1.NewControlPlaneClient(conn)

	gw := gateway.New(cpClient)

	// Shared shutdown signal for poll loop + InferenceService server.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Inbound: gateway is an InferenceService server (client calls Generate here).
	lis, err := net.Listen("tcp", gatewayAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", gatewayAddr, err)
	}

	grpcServer := grpc.NewServer()
	inferencev1.RegisterInferenceServiceServer(grpcServer, gw)

	// Reflection lets grpcurl discover the service without -proto flags.
	// Dev convenience only — a production gateway would leave this off
	// (it advertises the full API surface to anyone who can reach the port).
	reflection.Register(grpcServer)

	var wg sync.WaitGroup

	// Goroutine 1: poll ListWorkers, refresh the local view.
	wg.Add(1)
	go gw.PollLoop(ctx, &wg, pollInterval)

	// Goroutine 2: serve InferenceService in the background (Serve blocks until
	// GracefulStop).
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("gateway InferenceService listening on %s", gatewayAddr)
		if err := grpcServer.Serve(lis); err != nil {
			// GracefulStop makes Serve return nil; a non-nil error is a real
			// listener failure — cancel ctx so the process unwinds instead of
			// hanging on wg.Wait().
			log.Printf("gateway Serve error: %v", err)
			stop()
		}
	}()

	// Goroutine 3: bridge ctx cancellation to GracefulStop, draining in-flight
	// Generate calls before the listener closes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Printf("shutting down gateway")
		grpcServer.GracefulStop()
		gw.Close() // tear down cached worker connections
	}()

	wg.Wait()
	log.Printf("gateway shut down cleanly")
}
