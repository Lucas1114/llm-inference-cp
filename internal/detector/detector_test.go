package detector

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/lucas1114/llm-inference-cp/internal/registry"
)

// ── helpers ──────────────────────────────────────────────────────────────

func closeTo(got, want, tol float64) bool {
	return math.Abs(got-want) <= tol
}

// entryWith builds a detectorEntry whose window already holds the given
// inter-arrival samples (seconds) and whose last folded arrival was at
// lastApplied. state is left zero: phi never reads it.
func entryWith(lastApplied time.Time, samples ...float64) *detectorEntry {
	capacity := len(samples)
	if capacity == 0 {
		capacity = 1 // newSlidingWindow(0) would make push divide by zero
	}
	w := newSlidingWindow(capacity)
	for _, s := range samples {
		w.push(s)
	}
	return &detectorEntry{window: w, lastApplied: lastApplied}
}

func testConfig() Config {
	return Config{
		PollInterval: 500 * time.Millisecond,
		WindowSize:   100,
		PhiSuspect:   1,
		PhiDead:      8,
	}
}

// ── slidingWindow ────────────────────────────────────────────────────────

func TestSlidingWindow_CountCapsAtCapacity(t *testing.T) {
	w := newSlidingWindow(3)
	if w.count() != 0 {
		t.Fatalf("fresh window holds %d samples, want 0", w.count())
	}
	for i := 1; i <= 10; i++ {
		w.push(float64(i))
	}
	if w.count() != 3 {
		t.Fatalf("count = %d after 10 pushes into a 3-slot window, want 3", w.count())
	}
}

// The window is bounded on purpose: φ must track the CURRENT arrival regime,
// not an average over all history. A worker whose beats slow down permanently
// should see its mean follow, or φ would fire on a new normal.
func TestSlidingWindow_WrapKeepsMostRecentSamples(t *testing.T) {
	w := newSlidingWindow(3)
	for _, s := range []float64{1, 2, 3, 4, 5} {
		w.push(s)
	}
	// The three surviving samples are 3, 4 and 5 — mean 4. Their storage order
	// differs from insertion order after the wrap, which mean/variance ignore.
	if got := w.mean(); !closeTo(got, 4, 1e-9) {
		t.Fatalf("mean = %v, want 4 (samples 1 and 2 should have been evicted)", got)
	}
}

func TestSlidingWindow_MeanAndVariance(t *testing.T) {
	w := newSlidingWindow(3)
	w.push(1)
	w.push(2)
	w.push(3)

	if got := w.mean(); !closeTo(got, 2, 1e-9) {
		t.Fatalf("mean = %v, want 2", got)
	}
	// Population variance (divide by n, not n−1): ((1)²+0+(1)²)/3.
	if got := w.variance(); !closeTo(got, 2.0/3.0, 1e-9) {
		t.Fatalf("variance = %v, want %v — is it dividing by n−1?", got, 2.0/3.0)
	}
}

// ── phi ──────────────────────────────────────────────────────────────────

// Variance needs two samples. Until then the detector cannot judge, and it must
// say "not suspicious" rather than guess — otherwise a worker that has only
// just joined gets declared dead while its window is still filling.
func TestPhi_ColdStartIsZero(t *testing.T) {
	now := time.Now()
	long := now.Add(-10 * time.Minute) // absurdly overdue, deliberately

	for _, tc := range []struct {
		name    string
		samples []float64
	}{
		{"no samples", nil},
		{"one sample", []float64{1.0}},
	} {
		e := entryWith(long, tc.samples...)
		if got := e.phi(now); got != 0 {
			t.Errorf("%s: phi = %v, want 0 while the window is cold", tc.name, got)
		}
	}
}

// At exactly the mean interval the next beat is a coin flip: P_later = 0.5, so
// φ = −log10(0.5) ≈ 0.301. This pins the formula itself — a sign error or a
// missing 0.5 shows up here and nowhere else.
func TestPhi_AtTheMeanIsAboutPoint3(t *testing.T) {
	now := time.Now()
	// Mean 1.0s with real spread, so the stdDev floor does not kick in.
	e := entryWith(now.Add(-1*time.Second), 0.8, 1.0, 1.2)

	got := e.phi(now)
	if !closeTo(got, 0.301, 0.05) {
		t.Fatalf("phi at t == mean = %v, want ≈0.301", got)
	}
}

// φ is a suspicion curve, not a flag: silence that grows must score strictly
// higher. SUSPECT and DEAD are two points on this one curve.
func TestPhi_GrowsWithSilence(t *testing.T) {
	base := time.Now()
	samples := []float64{0.9, 1.0, 1.1}

	var prev float64 = -1
	for _, silence := range []time.Duration{
		time.Second,
		1200 * time.Millisecond,
		1500 * time.Millisecond,
		2 * time.Second,
	} {
		e := entryWith(base, samples...)
		got := e.phi(base.Add(silence))
		if got <= prev {
			t.Fatalf("phi did not increase at silence=%v: %v after %v", silence, got, prev)
		}
		prev = got
	}
}

// P_later underflows to zero in the far tail, and −log10(0) is +Inf. An
// unbounded φ is useless to log or threshold, so it clamps. Anything this far
// out is "certainly dead" and the exact number carries no information.
func TestPhi_CapsAtMaxPhi(t *testing.T) {
	now := time.Now()
	e := entryWith(now.Add(-1*time.Hour), 0.9, 1.0, 1.1)

	got := e.phi(now)
	if got != maxPhi {
		t.Fatalf("phi after an hour of silence = %v, want the %v cap", got, maxPhi)
	}
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Fatalf("phi = %v, must stay finite", got)
	}
}

// A perfectly regular heartbeat has zero sample variance, so the fitted normal
// is a spike and ANY overdue-ness divides by ~0 and saturates φ instantly. The
// floor models the irreducible jitter no real network is below. Without it this
// case returns maxPhi and a healthy worker 50ms late is declared dead.
func TestPhi_StdDevFloorKeepsIdenticalSamplesUsable(t *testing.T) {
	now := time.Now()
	// Every interval exactly 1.0s → variance 0 → stdDev floored to 50ms.
	e := entryWith(now.Add(-1050*time.Millisecond), 1.0, 1.0, 1.0, 1.0)

	got := e.phi(now) // one floored stdDev past the mean
	if got == maxPhi {
		t.Fatal("phi saturated 50ms past the mean — the stdDev floor is not applied")
	}
	if !closeTo(got, 0.80, 0.15) {
		t.Fatalf("phi = %v, want ≈0.80 (z = 1/√2 against the floored stdDev)", got)
	}
}

// ── scanOnce: the state machine ──────────────────────────────────────────

// warmUp registers a worker and feeds the detector enough beats for the window
// to leave cold start. scanOnce takes `now` as an argument, so the whole state
// machine is drivable without a ticker and without waiting on wall-clock time.
func warmUp(t *testing.T, reg *registry.WorkerRegistry, d *Detector, id string) {
	t.Helper()

	reg.Register(id, "localhost:1", 10)
	d.scanOnce(time.Now()) // first sighting: born ALIVE, window empty

	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Millisecond)
		reg.Heartbeat(id, 0)
		d.scanOnce(time.Now())
	}
}

func TestScanOnce_DeclaresDeadEmitsAndEvicts(t *testing.T) {
	reg := registry.NewWorkerRegistry()
	d := New(reg, testConfig())
	warmUp(t, reg, d, "w1")

	// Silence far beyond anything the window has seen. φ saturates at maxPhi,
	// which is above PhiDead by a wide margin — no timing sensitivity here.
	d.scanOnce(time.Now().Add(10 * time.Second))

	select {
	case ev := <-d.Events():
		if ev.WorkerID != "w1" {
			t.Fatalf("DeadEvent for %q, want w1", ev.WorkerID)
		}
	default:
		t.Fatal("no DeadEvent emitted after prolonged silence")
	}

	if got := len(reg.ListWorkers()); got != 0 {
		t.Fatalf("registry still holds %d worker(s); the corpse was not evicted", got)
	}
}

// A dead worker leaves no tombstone. Revival is therefore a pure data-flow
// effect: the worker re-registers, reappears in the snapshot as a FIRST
// SIGHTING, and is born ALIVE with an empty window. Nothing in Register has to
// know about detection state — which is the whole reason State lives here and
// not in the registry.
func TestScanOnce_RevivalIsAFirstSighting(t *testing.T) {
	reg := registry.NewWorkerRegistry()
	d := New(reg, testConfig())
	warmUp(t, reg, d, "w1")

	d.scanOnce(time.Now().Add(10 * time.Second))
	<-d.Events() // drain the death

	if _, ok := d.entries["w1"]; ok {
		t.Fatal("detector kept an entry for a worker it declared dead")
	}

	// Same id comes back, as a SIGSTOP'd worker does after SIGCONT.
	reg.Register("w1", "localhost:1", 10)
	d.scanOnce(time.Now())

	e, ok := d.entries["w1"]
	if !ok {
		t.Fatal("re-registered worker was not picked up as a first sighting")
	}
	if e.window.count() != 0 {
		t.Fatalf("revived worker inherited %d samples; its window must start empty",
			e.window.count())
	}

	select {
	case ev := <-d.Events():
		t.Fatalf("a second DeadEvent was emitted for %q after revival", ev.WorkerID)
	default:
	}
}

// The two locks guard different things and are never nested: registry.mu covers
// reported facts and is written by the Heartbeat handler, detector.mu covers
// inferred state and is written only by the scanner. This drives both at once.
// Its whole value is under -race.
func TestScanOnce_ConcurrentWithHeartbeats(t *testing.T) {
	reg := registry.NewWorkerRegistry()
	d := New(reg, testConfig())

	for _, id := range []string{"w1", "w2", "w3"} {
		reg.Register(id, "localhost:1", 10)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for _, id := range []string{"w1", "w2", "w3"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					reg.Heartbeat(id, 0)
				}
			}
		}(id)
	}

	for i := 0; i < 200; i++ {
		d.scanOnce(time.Now())
	}
	close(stop)
	wg.Wait()
}
