package registry

import (
	"sync"
	"time"
)

// WorkerState is the failure-detector's classification of a worker.
//
// The TYPE lives here so there is a single definition shared across the
// codebase, but the authoritative STORAGE of a worker's state lives in the
// detector package, NOT in WorkerInfo below. Rationale: the registry is a pure
// FACT store (what workers reported); a worker's ALIVE/SUSPECT/DEAD state is an
// OPINION the detector infers. Keeping the opinion out of the fact store means
// the registry has ONE writer per concern and the detector's scanner is the
// sole writer of state — no two-writers-one-lock coupling.
type WorkerState int

const (
	StateAlive WorkerState = iota
	StateSuspect
	StateDead
)

// WorkerInfo is the control plane's *domain* model of a worker.
// Deliberately separate from the gRPC wire types (the generated
// inferencev1.* structs): the wire model crosses the network; the
// domain model is what we own and reason about. Keeping them apart
// means a proto change doesn't ripple into our core logic.
//
// NOTE: every field here is a value type (string, int, float64,
// time.Time). That's what makes the value-copy in ListWorkers a
// fully detached snapshot. If you ever add a slice/map/pointer
// field, the copy becomes shallow and the aliasing problem is back.
type WorkerInfo struct {
	ID       string
	Addr     string // host:port the router/data plane dials
	State    WorkerState
	Capacity uint32    // max concurrent requests; set at Register, used by M4 admission control
	Load     float64   // updated by heartbeats
	LastSeen time.Time // stamped by Register AND by every heartbeat (server clock)
}

// WorkerRegistry is the control plane's source of truth for which
// workers exist. M2's failure detection, router, and membership
// view all read from here — which is exactly why the locking has
// to be airtight from day one.
type WorkerRegistry struct {
	mu      sync.Mutex
	workers map[string]*WorkerInfo
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
	}
}

// Register adds or updates a worker. M1 decision: upsert —
// re-registering an existing ID overwrites it.
//
// Register only writes FACTS (id/addr/capacity/last-seen); it does NOT touch
// failure-detection state, which the detector owns. This dissolves the old
// "does re-register revive a SUSPECT worker?" question: it doesn't, directly.
// A re-registering worker refreshes LastSeen; the detector's next scan sees
// the refreshed arrival, φ falls, and the scanner pulls the worker back to
// ALIVE on its own. Revival is derived from the data flow, not forced here.
func (r *WorkerRegistry) Register(id, addr string, capacity uint32) {
	// TODO(M1): validate id/addr (non-empty, addr parseable).

	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[id] = &WorkerInfo{
		ID:       id,
		Addr:     addr,
		Capacity: capacity,
		LastSeen: time.Now(),
	}
}

// Deregister removes a worker. M2's failure detector calls this
// when a worker is judged DEAD; M1 just has it for symmetry.
//
// Returns whether the worker existed. existed == false is a normal idempotent
// outcome (the desired end state — "worker absent" — already holds), NOT an
// error: callers must not translate it into NotFound.
func (r *WorkerRegistry) Deregister(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, existed := r.workers[id]
	delete(r.workers, id)

	return existed
}

// Heartbeat records a worker's periodic liveness signal: it updates the
// reported load AND stamps LastSeen in a SINGLE critical section, so a
// reader can never observe load updated but freshness stale (or vice
// versa). LastSeen is stamped here with the server's clock (time.Now at
// receipt) — NOT a worker-sent timestamp — because M2's phi-accrual
// reasons about arrival timing on one consistent monotonic clock.
//
// This in-lock, in-place mutation is *exactly* why ListWorkers must not
// leak the *WorkerInfo pointer: this write and an out-of-lock read of the
// same object would be a data race the mutex can't see.
//
// Returns false when id is unknown (worker deregistered between beats, or
// a stray beat after graceful shutdown). The caller (server adapter) maps
// false → codes.NotFound; the worker does NOT get silently re-registered.
func (r *WorkerRegistry) Heartbeat(id string, load float64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if w, ok := r.workers[id]; ok {
		w.Load = load
		w.LastSeen = time.Now()
		return true
	}
	return false
}

// ListWorkers returns a snapshot of all workers.
//
// THE KEY DECISION: returns []WorkerInfo (values), NOT
// []*WorkerInfo (pointers). Reason #2 is the one that bites:
//
//  1. The mutex protects the *map structure* (which keys exist).
//     It does NOT protect the WorkerInfo objects behind the
//     pointers.
//  2. If we handed out *WorkerInfo, the caller would read those
//     fields OUTSIDE our lock — while a concurrent Heartbeat call
//     is INSIDE the lock writing the same object. The mutex can't
//     see that race: the pointer escaped the lock's boundary.
//
// Value copies make the snapshot fully detached. Cost is one copy
// per worker — negligible at our scale. When scale grows this
// evolves into copy-on-write (immutable snapshot + atomic pointer
// swap, zero-lock reads) — the MembershipView in M2.
//
// Caveat: map iteration order is randomized, so the returned slice
// is unordered. Don't rely on order in the router.
func (r *WorkerRegistry) ListWorkers() []WorkerInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]WorkerInfo, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, *w) // *w dereferences the pointer: copy by value
	}
	return out
}
