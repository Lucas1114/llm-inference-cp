package controlplane

import (
	"context"
	"log"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
	"github.com/lucas1114/llm-inference-cp/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// Heartbeat: a worker calls this every heartbeatIntervalMs to prove liveness.
//
// The registry stamps LastSeen with the SERVER's clock at this moment — not a
// timestamp sent by the worker — so phi-accrual reasons about arrival times on
// a single monotonic clock and never has to trust cross-machine wall clocks.
//
// An unknown worker_id is NOT silently upserted: it means the control plane
// restarted and lost its registry, so we tell the worker NotFound and let it
// re-register itself. Self-healing beats silent state divergence.
func (s *Server) Heartbeat(
	ctx context.Context,
	req *inferencev1.HeartbeatRequest,
) (*inferencev1.HeartbeatResponse, error) {
	// TODO(M4): `load` is the raw in-flight count today. Scheduling will likely
	// want a richer signal (KV-cache occupancy, queue depth) — revisit then.
	load := float64(req.GetLoad().GetActiveRequests())

	if found := s.reg.Heartbeat(req.GetWorkerId(), load); !found {
		return nil, status.Errorf(codes.NotFound,
			"unknown worker_id %q; re-register", req.GetWorkerId())
	}

	return &inferencev1.HeartbeatResponse{}, nil
}

func (s *Server) Deregister(
	ctx context.Context,
	req *inferencev1.DeregisterRequest,
) (*inferencev1.DeregisterResponse, error) {
	// Deregister asserts a final state ("worker absent"), not a transition, so
	// it is idempotent: deregistering an unknown worker is a success, not a
	// NotFound. Unlike Heartbeat — where a missing worker signals divergence and
	// must return NotFound to trigger self-heal re-registration — a worker
	// calling Deregister is on its way out and has nothing to reconcile.
	if existed := s.reg.Deregister(req.GetWorkerId()); !existed {
		// No-op deregister. Fine — the desired end state already holds.
		// Logged at debug level only; a spike here (e.g. deregister_noop_total)
		// would hint at duplicate delivery or a confused caller. Not an error.
		log.Printf("deregister: worker %q already absent (no-op)", req.GetWorkerId())
	}

	return &inferencev1.DeregisterResponse{}, nil
}
