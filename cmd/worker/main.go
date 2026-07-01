package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

const (
	controlPlaneAddr = "localhost:50051"
	workerCapacity   = 10
)

func main() {
	// worker_id is self-assigned at startup (our decision: worker mints its
	// own UUID, control plane does not allocate it).
	workerID := uuid.NewString()

	// M1: the worker doesn't actually serve Generate yet, so it advertises a
	// placeholder address. This becomes its real listen addr when the mock
	// inference server lands.
	workerAddr := "localhost:60001"

	conn, err := grpc.NewClient(
		controlPlaneAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // M1: no TLS
	)
	if err != nil {
		log.Fatalf("failed to dial control plane: %v", err)
	}
	defer conn.Close()

	client := inferencev1.NewControlPlaneClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Register(ctx, &inferencev1.RegisterRequest{
		WorkerId: workerID,
		Address:  workerAddr,
		Capacity: workerCapacity,
	})
	if err != nil {
		log.Fatalf("Register failed: %v", err)
	}

	log.Printf("registered ok. id=%s control plane says heartbeat every %dms",
		workerID, resp.GetHeartbeatIntervalMs())

	// M2: a heartbeat loop at resp.HeartbeatIntervalMs cadence goes here.
	// M1 stops after a successful registration.
}
