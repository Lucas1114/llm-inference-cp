package registry

import (
	"fmt"
	"sync"
	"testing"
)

func TestRegisterAndList(t *testing.T) {
	r := NewWorkerRegistry()
	r.Register("w1", "localhost:5001", 0)
	r.Register("w2", "localhost:5002", 0)

	if got := r.ListWorkers(); len(got) != 2 {
		t.Fatalf("want 2 workers, got %d", len(got))
	}
}

// Upsert: re-registering the same id overwrites, not duplicates.
func TestRegisterUpsert(t *testing.T) {
	r := NewWorkerRegistry()
	r.Register("w1", "localhost:5001", 0)
	r.Register("w1", "localhost:9999", 0) // same id, new addr

	got := r.ListWorkers()
	if len(got) != 1 {
		t.Fatalf("upsert should keep 1 worker, got %d", len(got))
	}
	if got[0].Addr != "localhost:9999" {
		t.Fatalf("upsert should overwrite addr, got %q", got[0].Addr)
	}
}

// Concurrency: hammer the registry from many goroutines at once.
// Run with: go test -race ./...
// With the mutex in place this is clean. Want to SEE the lesson
// from the notes? Temporarily comment out the Lock/Unlock in
// registry.go and re-run with -race — it'll scream
// "DATA RACE" / "concurrent map writes". Then put the lock back.
func TestConcurrentAccess(t *testing.T) {
	r := NewWorkerRegistry()
	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("w%d", i)
			r.Register(id, "localhost:5000",0)
			r.Heartbeat(id, float64(i)) // in-lock writer
			_ = r.ListWorkers()          // snapshot reader
		}(i)
	}
	wg.Wait()
}
