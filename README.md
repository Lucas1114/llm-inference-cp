# llm-inference-cp

A distributed control plane for LLM inference serving, written in Go.

The control plane owns cluster membership, failure detection, and routing
decisions — and stays **out of the data path**. It tells the gateway *who* to
route to; it never forwards a byte of inference traffic itself.

> **North star:** exactly-once is a property of the whole pipeline, never of any
> single component. The honest answer is always
> **at-least-once delivery + idempotent processing = effectively-once semantics.**
> That principle recurs throughout this system: in failure detection, in
> rerouting, and in request deduplication.

> This is a personal portfolio project, built to explore distributed systems
> depth rather than feature completeness. Workers are mocked; the inference
> engine is not the point.

## Architecture

```
client → gateway / router          (stateless, horizontally scalable)
              ↓
         control plane             (membership, failure detection, routing decisions)
              ↓
         worker nodes              (mocked; real C++/CUDA engine later)
              ↑
         metadata store            (cluster state; survives control plane restarts)
```

The control plane is deliberately kept off the data path. A worker registers,
then proves liveness by heartbeating. The gateway asks the control plane who is
alive and dials those workers directly.

## Status

| Milestone | Scope | State |
|---|---|---|
| **M1** | gRPC skeleton, worker registry, end-to-end registration | ✅ Done |
| **M2** | Heartbeats, phi-accrual failure detection, zero-loss rerouting, idempotent dedup | 🔄 In progress |
| **M3** | Control plane HA via Raft leader election | Planned |
| **M4** | KV-cache-aware scheduling and backpressure | Planned |
| **M5** | Fault-injection harness with correctness assertions | Planned |

**M2 breakdown:** heartbeat pipeline ✅ · graceful deregister · phi-accrual
detection · eviction + zero-loss rerouting · idempotent dedup

## Design notes

### Liveness is judged by the observer, not the observed

A worker cannot distinguish "the network is partitioned," "the control plane is
restarting," and "I am dead" — from its own vantage point these are the same
observation. So the worker never judges its own liveness. It beats on a fixed
cadence and logs failures. The control plane's failure detector, which sees the
absence of heartbeats across the whole fleet, adjudicates.

Two consequences fall out of this:

- **No in-loop retry on a failed beat.** The next tick is already the retry.
  Retrying inside a tick would emit beats at irregular intervals and corrupt the
  inter-arrival distribution that phi-accrual learns from.
- **A worker never exits because it cannot reach the control plane.** Otherwise
  a control plane restart would amplify into a fleet-wide outage.

### `NotFound` is a semantic signal, not a transient error

When the control plane restarts, its registry is empty. The next heartbeat from
a surviving worker returns `NotFound` — the RPC succeeded, and the answer is
"I don't know who you are."

The control plane deliberately does *not* silently upsert the unknown worker. A
silent upsert would resurrect a worker behind the failure detector's back,
bypassing its `ALIVE → SUSPECT → DEAD` state machine. Instead the worker walks
back through the front door with an explicit `Register` call, reusing the same
`worker_id`. Identity continuity is what makes this a resurrection rather than
the arrival of a new worker.

### Self-heal demo

Kill the control plane while the worker keeps running, then restart it. The
worker survives, detects that it is no longer known, and re-registers itself —
without ever exiting.

<!-- paste the worker terminal output here -->

<!-- paste the control plane terminal output here -->

## Running it

```bash
go run ./cmd/controlplane   # listens on :50051
go run ./cmd/worker         # registers, then heartbeats every 1000ms
```

Inspect the cluster view with [grpcurl](https://github.com/fullstorydev/grpcurl)
(server reflection is enabled in development):

```bash
grpcurl -plaintext localhost:50051 inference.v1.ControlPlane/ListWorkers
```

## Testing

```bash
go test -race ./...
```

The race detector is treated as a first-class practice, not an afterthought. The
registry's copy-on-read design exists because a `-race` run proved that handing
out `*WorkerInfo` pointers let callers read fields outside the mutex that was
supposed to protect them.
