// Package detector implements phi-accrual failure detection for the control
// plane. It turns a worker's SILENCE into an ALIVE→SUSPECT→DEAD judgement,
// evicts a worker from the registry when it is declared DEAD, and emits a
// DeadEvent so the router can reroute in-flight work (Phase 4).
//
// WHERE STATE LIVES (Phase 3 decision): the failure-detector's State is OWNED
// HERE, in detectorEntry — NOT in registry.WorkerInfo. The registry stays a
// pure FACT store (what workers reported); the detector owns the OPINION (what
// we infer). Two writers, two concerns, two locks:
//   - registry.mu guards reported facts, written by the Heartbeat RPC handler.
//   - detector.mu guards inferred state, written by the scanner ONLY.
//
// Fusing them into one lock would weld two unrelated contention profiles.
package detector

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/lucas1114/llm-inference-cp/internal/registry"
)

// State reuses the registry enum so there is ONE definition, even though the
// authoritative STORAGE of state now lives here. Type shared; storage separated.
type State = registry.WorkerState

// maxPhi caps the suspicion score. As silence grows, P_later underflows toward
// 0 and -log10(P_later) heads to +Inf; an unbounded/NaN φ would be useless to
// log or threshold against. We clamp to a large-but-finite ceiling. Any φ this
// high is "certainly dead" — the exact number past the DEAD threshold is noise.
const maxPhi = 20.0

// minStdDevSeconds floors the sample standard deviation. If every observed
// interval is identical, the fitted normal has zero spread, so the tiniest
// deviation would spike φ to infinity (division by ~0). A floor models the
// irreducible timing jitter no real network is below. Akka's phi-accrual
// detector exposes the same knob (default 100ms); we hardcode 50ms for now.
const minStdDevSeconds = 0.05

// Config holds the detector's tunables. PollInterval is a control-plane
// INTERNAL parameter (workers never see it) — distinct from the heartbeat
// interval, which is the worker-facing protocol param.
type Config struct {
	PollInterval time.Duration // how often the scanner samples now−LastSeen
	WindowSize   int           // sliding-window capacity (inter-arrival samples)
	PhiSuspect   float64       // φ threshold: ALIVE → SUSPECT
	PhiDead      float64       // φ threshold: SUSPECT → DEAD
}

// ---------------------------------------------------------------------------
// slidingWindow — the bounded inter-arrival history that feeds φ.
// ---------------------------------------------------------------------------

// slidingWindow is a fixed-capacity ring buffer of inter-arrival samples
// (gaps between consecutive heartbeat arrivals, in SECONDS). It is φ's private
// input: raw material, never a fact anyone outside the detector consumes.
//
// It exposes exactly what φ needs — push, count, mean, variance — and nothing
// more. mean/variance are computed on read by iterating the buffer (two-pass).
// At WindowSize ≈ tens of samples, that O(n) walk is free, and it sidesteps the
// catastrophic-cancellation trap of maintaining Σx / Σx² incrementally (where
// variance = Σx²/n − mean² subtracts two large near-equal numbers and can even
// go negative). Correctness over a non-existent performance win.
type slidingWindow struct {
	buf  []float64 // ring storage, len == capacity
	head int       // index where the NEXT sample will be written
	n    int       // how many samples are currently valid (≤ cap)
}

func newSlidingWindow(capacity int) *slidingWindow {
	return &slidingWindow{buf: make([]float64, capacity)}
}

// push appends a sample, overwriting the oldest once the window is full.
func (w *slidingWindow) push(sample float64) {
	w.buf[w.head] = sample
	w.head = (w.head + 1) % len(w.buf) // wrap around
	if w.n < len(w.buf) {
		w.n++
	}
}

// count is how many valid samples the window currently holds.
func (w *slidingWindow) count() int { return w.n }

// mean is the average of the valid samples (pass 1). Caller guarantees n > 0.
// NOTE: valid samples always occupy buf[0:n] — while filling, head == n; once
// full, n == cap and every slot is valid. Order differs from insertion after
// wrap, but mean/variance don't care about order.
func (w *slidingWindow) mean() float64 {
	var sum float64
	for i := 0; i < w.n; i++ {
		sum += w.buf[i]
	}
	return sum / float64(w.n)
}

// variance is the average squared deviation from the mean (pass 2). Caller
// guarantees n > 1. We divide by n (population variance) rather than n−1: we're
// describing the observed samples, not inferring a wider population.
func (w *slidingWindow) variance() float64 {
	m := w.mean()
	var ss float64
	for i := 0; i < w.n; i++ {
		d := w.buf[i] - m
		ss += d * d
	}
	return ss / float64(w.n)
}

// ---------------------------------------------------------------------------
// detectorEntry — the detector's PRIVATE per-worker state.
// ---------------------------------------------------------------------------

type detectorEntry struct {
	state State

	// window holds recent inter-arrival intervals; φ's input.
	window *slidingWindow

	// lastApplied is the LastSeen value already folded into the window. Each
	// scan compares the registry's current LastSeen against this: advanced →
	// a new beat arrived (compute gap, push to window, update lastApplied);
	// unchanged → the worker has been silent, and φ is climbing.
	lastApplied time.Time
}

// phi scores how anomalous the current silence is against inter-arrival history:
//
//	φ(now) = -log10( P_later(t) ),   t = seconds since the last folded arrival
//
// P_later(t) = probability that, per the fitted normal distribution of past
// inter-arrival gaps, the NEXT beat arrives LATER than t from now. Small
// probability (very overdue) → large φ. The −log10 turns a probability that
// plunges toward zero into a linear, human-tunable scale: φ=1 ≈ 10% false-
// positive risk, φ=2 ≈ 1%, φ=8 ≈ 1e-8.
func (e *detectorEntry) phi(now time.Time) float64 {
	// Cold start: variance needs at least two samples. Until the window has
	// them, we cannot judge — treat as not-suspicious so we never declare a
	// warming-up worker dead.
	if e.window.count() < 2 {
		return 0
	}

	t := now.Sub(e.lastApplied).Seconds()

	mean := e.window.mean()
	stdDev := math.Sqrt(e.window.variance())
	if stdDev < minStdDevSeconds {
		stdDev = minStdDevSeconds // floor: no distribution is a perfect spike
	}

	// P_later(t) = 1 − CDF_normal(t) = 0.5 * erfc( (t − mean) / (stdDev*√2) ).
	// math.Erfc is numerically stable in the far tail, exactly where φ matters.
	z := (t - mean) / (stdDev * math.Sqrt2)
	pLater := 0.5 * math.Erfc(z)

	if pLater <= 0 {
		return maxPhi // tail underflowed to 0: certainly overdue
	}
	phi := -math.Log10(pLater)
	if phi > maxPhi {
		return maxPhi
	}
	return phi
}

// ---------------------------------------------------------------------------
// DeadEvent — emitted when a worker crosses φ_dead.
// ---------------------------------------------------------------------------

// DeadEvent notifies downstream (Phase 4's router) that a worker was declared
// DEAD, so it can stop routing and reroute in-flight requests. The detector has
// already evicted the worker from the registry by the time this is delivered.
type DeadEvent struct {
	WorkerID string
	At       time.Time // when the detector declared it dead (server clock)
}

// ---------------------------------------------------------------------------
// Detector — owns the state machine for ALL workers.
// ---------------------------------------------------------------------------

// Detector runs a SINGLE scanner goroutine + single ticker (see Run), NOT one
// goroutine per worker. The silence we detect is driven by the CLOCK, not by
// any inbound message — a dead worker produces no callback to hang logic on, so
// the judgement must be timer-driven.
type Detector struct {
	cfg Config
	reg *registry.WorkerRegistry // reads LastSeen snapshots; evicts on death

	mu      sync.Mutex // guards entries ONLY (the inferred state)
	entries map[string]*detectorEntry

	// events carries DEAD declarations out. Buffered so a slow consumer can't
	// stall the scanner — and we always send OUTSIDE the lock (see scanOnce).
	events chan DeadEvent
}

func New(reg *registry.WorkerRegistry, cfg Config) *Detector {
	return &Detector{
		cfg:     cfg,
		reg:     reg,
		entries: make(map[string]*detectorEntry),
		events:  make(chan DeadEvent, 64),
	}
}

// Events exposes the DEAD stream (Phase 4 wires the router to it).
func (d *Detector) Events() <-chan DeadEvent { return d.events }

// Run is the single scanner goroutine: ticks every PollInterval, sweeps, and
// returns on ctx cancel — mirroring the worker's heartbeat-loop shape, so the
// control-plane main can coordinate it with a WaitGroup exactly like cmd/worker.
func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			d.scanOnce(now)
		}
	}
}

// scanOnce is one sweep. THREE strictly ordered phases — this ordering IS the
// Phase-3 lock discipline:
//
//	phase 1 — VALUE snapshot of facts under registry.mu, released immediately
//	          (M1 copy-on-read, reused). We never compute φ under registry.mu:
//	          it would contend with the Heartbeat writer and distort the very
//	          LastSeen we're reading.
//	phase 2 — under detector.mu: fold new arrivals, recompute φ, advance state
//	          machines, collect DEAD ids into a LOCAL slice. Pure in-memory work
//	          — no channel send, no registry call while holding this lock.
//	phase 3 — OUTSIDE all locks: evict each dead worker from the registry and
//	          emit its event. Both can block / take another lock; doing them
//	          under detector.mu would risk lock-order inversion. So never.
func (d *Detector) scanOnce(now time.Time) {
	// ---- phase 1: snapshot facts (registry.mu, released on return) ----
	snapshot := d.reg.ListWorkers() // []registry.WorkerInfo value copies

	// ---- phase 2: advance state machines (detector.mu) ----
	var deaths []DeadEvent
	seen := make(map[string]bool, len(snapshot))

	d.mu.Lock()
	for _, w := range snapshot {
		seen[w.ID] = true

		e, ok := d.entries[w.ID]
		if !ok {
			// First sighting: born ALIVE, empty window, baseline the arrival
			// clock. No φ this scan — we have zero history yet.
			d.entries[w.ID] = &detectorEntry{
				state:       registry.StateAlive,
				window:      newSlidingWindow(d.cfg.WindowSize),
				lastApplied: w.LastSeen,
			}
			continue
		}

		// (1) Did a new heartbeat arrive since we last looked? LastSeen uses the
		// server's monotonic clock, so a strict After() means a fresh beat.
		if w.LastSeen.After(e.lastApplied) {
			gap := w.LastSeen.Sub(e.lastApplied).Seconds()
			e.window.push(gap)
			e.lastApplied = w.LastSeen
		}

		// (2) Score the silence since the last folded arrival.
		phi := e.phi(now)

		// (3) Advance the state machine. Transitions are one-way per scan; the
		// only writer of e.state is right here, in the single scanner goroutine.
		switch e.state {
		case registry.StateAlive:
			if phi > d.cfg.PhiDead {
				e.state = registry.StateDead
			} else if phi > d.cfg.PhiSuspect {
				e.state = registry.StateSuspect
			}
		case registry.StateSuspect:
			if phi > d.cfg.PhiDead {
				e.state = registry.StateDead
			} else if phi <= d.cfg.PhiSuspect {
				e.state = registry.StateAlive // recovered: beats resumed, φ fell
			}
		case registry.StateDead:
			// Terminal; handled below. Shouldn't linger — we delete on entry.
		}

		// On ENTRY into DEAD, record it once and drop the entry so we neither
		// re-emit nor keep evaluating a corpse. Deriving death here (not on a
		// re-register) keeps revival a pure data-flow effect: a resurrected
		// worker re-registers → reappears as a first sighting → born ALIVE.
		if e.state == registry.StateDead {
			deaths = append(deaths, DeadEvent{WorkerID: w.ID, At: now})
			delete(d.entries, w.ID)
		}
	}

	// Reap entries whose worker vanished from the registry (graceful-deregister
	// in Phase 2, or already evicted). Without this the map leaks dead keys.
	for id := range d.entries {
		if !seen[id] {
			delete(d.entries, id)
		}
	}
	d.mu.Unlock()

	// ---- phase 3: evict + emit, OUTSIDE the lock ----
	for _, ev := range deaths {
		// Evict from the registry ("死亡踢出"). Deregister is idempotent, so if
		// Phase 4's router also removes it later, that's a harmless no-op —
		// at-least-once + idempotent = effectively-once, the north star again.
		d.reg.Deregister(ev.WorkerID)
		d.events <- ev
	}
}
