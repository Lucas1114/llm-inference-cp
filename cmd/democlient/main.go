// democlient drives the failure demonstrations: it fires a batch of concurrent
// requests at the gateway and reports, per request, what came back.
//
// This exists because grpcurl cannot do the job. grpcurl issues one request per
// invocation, so there is no way to hold a batch in flight while a worker is
// killed — and the whole point of these demos is what happens to requests that
// are already in the air when their worker dies. grpcurl stays the right tool
// for hand-inspecting ListWorkers; it is not a test harness.
//
// It reports only what the CLIENT can see. The interesting half of the story —
// a duplicate result arriving late and being discarded — is not observable from
// here: Generate is a unary RPC, so exactly one response comes back whether or
// not any adjudication happened. That half is read from the gateway's log by
// scripts/demo.sh. This program's job is to prove no request was lost.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	inferencev1 "github.com/lucas1114/llm-inference-cp/gen/inference/v1"
)

// result is one request's outcome, as the client saw it.
type result struct {
	RequestID string `json:"request_id"`
	WorkerID  string `json:"worker_id,omitempty"` // which worker actually answered
	OK        bool   `json:"ok"`
	Code      string `json:"code,omitempty"`
	Err       string `json:"error,omitempty"`
	ElapsedMs int64  `json:"elapsed_ms"`
}

func main() {
	var (
		addrFlag = flag.String("addr", "localhost:50052", "gateway address")
		nFlag    = flag.Int("n", 20, "number of concurrent requests")
		prefix   = flag.String("prefix", "req", "request id prefix; ids are <prefix>-NNN")
		timeout  = flag.Duration("timeout", 60*time.Second, "overall deadline for the batch")
		jsonOut  = flag.String("json", "", "also write per-request results as JSON to this path")
	)
	flag.Parse()

	// NewClient does not dial, so a gateway that is not up yet fails at the
	// first RPC rather than here. The script polls for readiness before
	// running us; we do not re-implement that wait.
	conn, err := grpc.NewClient(*addrFlag,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "democlient: dial %s: %v\n", *addrFlag, err)
		os.Exit(2)
	}
	defer conn.Close()

	client := inferencev1.NewInferenceServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Fire all requests at once and let them sit in flight. The batch has to
	// still be running when the script kills a worker, which is why the
	// workers are started with a long -delay.
	results := make([]result, *nFlag)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < *nFlag; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Ids are assigned here, not minted by the gateway, so the script
			// can correlate a client-side outcome with the gateway's log lines
			// for the same request.
			id := fmt.Sprintf("%s-%03d", *prefix, i)
			t0 := time.Now()

			resp, err := client.Generate(ctx, &inferencev1.InferenceRequest{
				RequestId: id,
				Prompt:    "demo",
			})

			r := result{RequestID: id, ElapsedMs: time.Since(t0).Milliseconds()}
			if err != nil {
				r.Code = status.Code(err).String()
				r.Err = err.Error()
			} else {
				r.OK = true
				r.WorkerID = resp.GetWorkerId()
			}
			results[i] = r
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	ok := 0
	byWorker := map[string]int{}
	for _, r := range results {
		if r.OK {
			ok++
			byWorker[r.WorkerID]++
		}
	}

	for _, r := range results {
		if r.OK {
			fmt.Printf("  %s  ok    %5dms  served by %s\n", r.RequestID, r.ElapsedMs, r.WorkerID)
		} else {
			fmt.Printf("  %s  FAIL  %5dms  %s: %s\n", r.RequestID, r.ElapsedMs, r.Code, r.Err)
		}
	}

	fmt.Printf("\n  %d/%d completed, %d failed, batch wall time %v\n",
		ok, *nFlag, *nFlag-ok, elapsed.Round(time.Millisecond))
	for w, n := range byWorker {
		fmt.Printf("  answered by %s: %d\n", w, n)
	}

	if *jsonOut != "" {
		f, err := os.Create(*jsonOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "democlient: %v\n", err)
			os.Exit(2)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			fmt.Fprintf(os.Stderr, "democlient: %v\n", err)
			os.Exit(2)
		}
	}

	// Exit non-zero if anything was lost. The script checks this; a demo that
	// prints a failure and exits 0 is a demo nobody notices failing.
	if ok != *nFlag {
		os.Exit(1)
	}
}
