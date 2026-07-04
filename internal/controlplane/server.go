package controlplane

import (
	"context"
	"log"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
	"github.com/lucas1114/llm-inference-cp/internal/registry"
)

// heartbeatIntervalMs is the cadence the control plane DICTATES to every
// worker in the RegisterResponse. Centralizing it here (not letting each
// worker pick its own) keeps the M2 failure detector's expectations and the
// workers' actual behavior in agreement — phi-accrual reasons about arrival
// timing, so a worker inventing its own interval would cause false positives.
const heartbeatIntervalMs = 1000

// Server implements the generated ControlPlane gRPC service. It is a thin
// adapter: it translates wire types (inferencev1.*) to/from our domain model
// and delegates all state to the registry. No business state lives here.
type Server struct {
	// Embedding the Unimplemented* struct is the gRPC forward-compat pattern:
	// if the proto later gains an RPC we haven't written yet, the code still
	// compiles (that RPC just returns "unimplemented") instead of breaking.
	inferencev1.UnimplementedControlPlaneServer

	reg *registry.WorkerRegistry
}

func NewServer(reg *registry.WorkerRegistry) *Server {
	return &Server{reg: reg}
}

// Register: a worker calls this once at startup to announce itself.
// We pull the fields off the wire request, hand them to the registry, and
// return the heartbeat cadence the worker must obey.
func (s *Server) Register(
	ctx context.Context,
	req *inferencev1.RegisterRequest,
) (*inferencev1.RegisterResponse, error) {
	s.reg.Register(req.GetWorkerId(), req.GetAddress(), req.GetCapacity())

	log.Printf("registered worker id=%s addr=%s capacity=%d",
		req.GetWorkerId(), req.GetAddress(), req.GetCapacity())

	return &inferencev1.RegisterResponse{
		HeartbeatIntervalMs: heartbeatIntervalMs,
	}, nil
}

// ListWorkers: the gateway polls this to learn who to route to.
//
// THE CONVERSION LAYER (your "domain model ≠ wire type" decision, made real):
// the registry hands back rich domain WorkerInfo (State/Load/Capacity/...),
// but the gateway only needs id+address to route. We map down to the lean
// wire type and expose nothing more. Two payoffs:
//  1. A proto change doesn't ripple into core logic (decoupled contract).
//  2. Internal signals (Load/State) don't leak to a caller that only routes.
func (s *Server) ListWorkers(
	ctx context.Context,
	req *inferencev1.ListWorkersRequest,
) (*inferencev1.ListWorkersResponse, error) {
	domain := s.reg.ListWorkers() // []registry.WorkerInfo (value-copy snapshot)

	out := make([]*inferencev1.WorkerInfo, 0, len(domain))
	for _, w := range domain {
		out = append(out, &inferencev1.WorkerInfo{
			WorkerId: w.ID,
			Address:  w.Addr,
		})
	}
	return &inferencev1.ListWorkersResponse{Workers: out}, nil
}

// Heartbeat is defined by the proto but only exercised in M2 (phi-accrual
// consumes arrival timing). Left unimplemented on purpose — the embedded
// UnimplementedControlPlaneServer supplies a stub so this compiles today.
