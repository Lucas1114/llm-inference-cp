package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
	"github.com/lucas1114/llm-inference-cp/internal/controlplane"
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

	log.Printf("control plane listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server stopped: %v", err)
	}
}
