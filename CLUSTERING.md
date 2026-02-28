# Clustering Design

This document describes the incremental path from Rhei's current single-threaded executor to a fully distributed, dynamically scaled stream processing cluster. It is organized into three phases, each building on the previous.

For the full system topology vision, see [ARCHITECTURE.md](ARCHITECTURE.md). For the checklist of clustering work items, see the **Clustering** section of [ROADMAP.md](ROADMAP.md).

**Phases at a glance:**

| Phase | Scope | Timely Config | Scaling unit |
|-------|-------|---------------|--------------|
| Current | Single thread | `execute_directly()` | 1 worker |
| 1 | Multi-thread | `Config::process(n)` | N worker threads |
| 2 | Multi-process | `Config::Cluster { .. }` | N OS processes |
| 3 | Full control plane | OpenRaft + chitchat | Dynamic worker pool |

---

## Current State

The executor uses `timely::execute_directly()`, which runs a single worker on the calling thread. The full dataflow — source, operators, sink — lives in one OS thread inside a `spawn_blocking` task.

**Key characteristics:**

- **Source/sink bridge.** Async `Source` and `Sink` traits are bridged to synchronous Timely channels via `source_bridge` / `sink_bridge` (`rhei-runtime/src/bridge.rs`). A Tokio task calls `source.next_batch().await` in a loop and sends batches over a bounded `mpsc` channel. The Timely source operator reads with `try_recv()`.

- **Operator execution.** Each operator is wrapped in `TimelyAsyncOperator` (`rhei-runtime/src/timely_operator.rs`), which manages epoch capability tokens and delegates to `AsyncOperator` for the hot/cold path split. `TimelyAsyncOperator` is `!Send` because Timely's `Capability<T>` uses `Rc` internally — it must be constructed inside the worker thread.

- **Data pact.** All operators use the `Pipeline` pact (no data exchange). Every element stays on the single worker.

- **State.** Each operator gets its own `StateContext` backed by `PrefixedBackend` wrapping either `LocalBackend` (JSON file) or `TieredBackend` (L1 memtable → L2 Foyer → L3 SlateDB/S3). The prefix is the operator name (`"{operator_name}/{user_key}"`).

- **Checkpointing.** `TimelyAsyncOperator::maybe_checkpoint()` triggers a checkpoint when the frontier advances and no pending futures remain. There is no cross-worker coordination because there is only one worker.

---

## Phase 1: Multi-Thread (Single Process, Multiple Workers)

Spawn N Timely worker threads within a single OS process, enabling parallel stateful processing on one machine.

### Timely Configuration

Replace `execute_directly()` with `execute(Config::process(n), ...)`. This spawns N worker threads connected by shared-memory channels. The closure receives `Worker<Allocator>` (generic over allocator) instead of `Worker<Thread>`.

```rust
// Before (single worker):
timely::execute_directly(move |worker| { ... });

// After (N workers, shared memory):
timely::execute(timely::Config::process(n), move |worker| { ... });
```

### Data Partitioning

Stateful operators switch from `Pipeline` pact to `Exchange` pact. The exchange key function hashes the partition key to route each element to its owning worker:

```rust
Exchange::new(|element: &Element| hash(element.key()) % num_workers)
```

This guarantees that all elements with the same key are processed by the same worker, which is required for correct stateful operations (windows, joins, keyed aggregations).

Stateless operators (`Map`, `Filter`, `FlatMap`) remain on `Pipeline` pact — they can process any element regardless of key affinity.

Operators that already have a `key_fn` (e.g., window operators) can derive the exchange function from the same key extractor, keeping partitioning consistent with the operator's state access pattern.

### Source Fan-Out

Two strategies, chosen per source type:

1. **Single-source broadcast.** The source runs on worker 0 only (`if worker.index() == 0 { ... }`). Elements enter the dataflow on worker 0 and are distributed to other workers via the first Exchange pact. Simple to implement; the source does not need to be partition-aware.

2. **Partitioned source.** Each worker runs its own source instance (e.g., Kafka consumer group where each worker consumes a subset of partitions). Higher throughput since source reads parallelize. Requires the source to support partition assignment.

For the initial implementation, strategy 1 is simpler and sufficient. Strategy 2 can be added as an optimization for Kafka and similar partitioned sources.

### Sink Fan-In

Each worker writes to its own sink instance. For sinks that support concurrent writes (e.g., Kafka producer), this is the simplest approach — each worker independently flushes its output.

For sinks that require ordered or single-writer semantics, worker 0 can collect outputs from all workers via an Exchange pact with a constant key (routing everything to worker 0) before writing.

### State Isolation

Each worker gets its own `StateContext` with a worker-index prefix. The existing `PrefixedBackend` extends naturally:

```
Current:  "{operator_name}/{user_key}"
Phase 1:  "w{worker_index}/{operator_name}/{user_key}"
```

No shared state between workers. The Exchange pact guarantees each key routes to exactly one worker, so there are no concurrent writes to the same state key. Each worker's state is fully independent.

When using `TieredBackend`, each worker gets its own L1 memtable and L2 Foyer cache. The L3 `SlateDbBackend` can be shared (wrapped in `Arc`) since SlateDB handles concurrent access, but key prefixing ensures workers never contend on the same keys.

### Checkpoint Coordination

Timely's frontier tracking naturally synchronizes epochs across workers within a process. When all workers advance past epoch E (i.e., the global frontier moves beyond E), it is safe to checkpoint.

The existing `TimelyAsyncOperator::maybe_checkpoint()` already checks the frontier before checkpointing. In multi-worker mode, each worker independently observes the global frontier (Timely propagates frontier information across workers via its progress protocol). When a worker sees the frontier advance, it checkpoints its own state.

A `std::sync::Barrier(n)` can optionally coordinate the flush to ensure all workers have completed their checkpoint for epoch E before committing source offsets. Alternatively, Timely's own progress tracking can serve as the coordination mechanism — if the frontier has advanced past E on all workers, all workers must have finished processing E.

### Metrics

Each worker emits metrics with a `worker_index` label:

```rust
metrics::counter!("executor_elements_total", "worker" => worker.index().to_string())
```

The TUI and Prometheus exporter aggregate across workers for pipeline-level views.

### Key Risks and Mitigations

- **`!Send` constraint.** `TimelyAsyncOperator` is `!Send` due to `Rc` in Timely capabilities. It must be constructed inside each worker thread, not moved across threads. The current `Mutex` wrapping pattern (used to move `source_rx` and operators into `execute_directly`) extends to `execute()`: wrap captures in `Mutex`, unwrap inside each worker thread, and construct `TimelyAsyncOperator` per-worker.

- **Backward compatibility.** `--workers 1` (the default) behaves identically to today's `execute_directly()`. No breaking changes.

- **Tokio bridge.** Each worker thread needs access to the Tokio runtime handle for the cold path (`block_in_place`). The `Handle` is `Clone + Send`, so it can be shared across workers.

---

## Phase 2: Multi-Process (Timely TCP Cluster)

Distribute the computation across multiple OS processes on different machines, connected via TCP.

### Timely Configuration

```rust
timely::execute(timely::Config::Cluster {
    threads: threads_per_process,
    process: process_id,
    addresses: vec!["host0:2101".into(), "host1:2101".into(), ...],
    report: false,
    log_fn: Box::new(|_| None),
}, move |worker| { ... });
```

Each process is started independently with its process ID and the full list of peer addresses. Timely handles TCP connection setup, handshaking, and data exchange internally.

### Deployment Model

Each process is a separate OS process (potentially on a different machine). The CLI gains cluster flags:

```
rhei run pipeline.toml --workers 4 --process-id 0 --peers "host0:2101,host1:2101"
rhei run pipeline.toml --workers 4 --process-id 1 --peers "host0:2101,host1:2101"
```

A `--hostfile` option can mirror Timely's hostfile convention for clusters with many processes.

### Data Plane

From the operator's perspective, the data plane is identical to Phase 1. The Exchange pact routes data, the Pipeline pact keeps data local. The only difference is the transport layer: shared-memory channels within a process, TCP sockets across processes.

Timely serializes elements crossing process boundaries. This introduces a serialization requirement that does not exist in Phase 1.

### Serialization

Current element types implement `serde::Serialize + Deserialize`. Timely's wire format requires either:

1. **`Abomonation`** — Timely's native zero-copy serialization. Extremely fast (memcpy-level), but `unsafe` and requires types to have a fixed memory layout. Good for hot-path types with simple structures.

2. **Serde via Timely containers** — Timely supports `serde`-based containers that serialize/deserialize elements at process boundaries. Safe, but involves a copy. Suitable as the default.

**Recommended approach:** Use serde-based containers as the default. Add `Abomonation` implementations for performance-critical types as an optimization. This keeps the API safe by default while leaving a path to zero-copy for hot paths.

**Trait bound change:** `StreamFunction::Input` and `StreamFunction::Output` must additionally satisfy Timely's serialization bounds. This is potentially breaking for types that contain non-serializable fields (`Rc`, closures, file handles, etc.).

### State Backend

Each process has its own `TieredBackend` (L1 memtable → L2 Foyer → L3 SlateDB). The state prefix extends to include the process ID:

```
Phase 2:  "p{process_id}/w{worker_index}/{operator_name}/{user_key}"
```

Since the Exchange pact guarantees key affinity, each process only manages state for its assigned key range. The L3 layer (SlateDB backed by S3) provides durability and enables process replacement without state migration — a new process for the same key range simply reads state from S3.

### Source Partitioning

- **Kafka.** Consumer groups naturally partition across processes. Each process joins the same consumer group and is assigned a subset of partitions by the Kafka broker. This is the ideal model — source parallelism scales with the number of processes.

- **Non-partitioned sources.** Worker 0 of process 0 acts as the single source, distributing elements via Exchange. This limits source throughput to a single reader but works for sources that don't support partitioning.

### Checkpoint Coordination

Timely's progress protocol extends across processes — frontiers are exchanged via TCP. When the global frontier advances past epoch E, all workers across all processes have finished processing epoch E.

However, confirming that all processes have durably flushed state to L3 requires additional coordination. Options:

1. **Piggyback on Timely progress.** Inject a special "checkpoint barrier" epoch. When Timely confirms all workers have processed it, the checkpoint is committed. This reuses Timely's existing reliable broadcast but conflates progress tracking with durability confirmation.

2. **Out-of-band confirmation.** After each process completes its L3 flush, it sends a confirmation via a lightweight RPC. A coordinator (e.g., process 0 or a dedicated checkpoint manager) collects confirmations and commits the checkpoint ID. Cleaner separation of concerns.

Option 2 is recommended for its clarity. The coordinator writes the committed checkpoint ID to S3 (or a shared metadata store) so that recovery can find the latest consistent checkpoint.

### Failure Handling

Timely does not support partial failure recovery. If any process crashes, the entire computation restarts from the last committed checkpoint:

1. All surviving processes stop.
2. All processes restart, loading state from L3 (SlateDB/S3) for their assigned key range.
3. Sources replay from the committed offsets (e.g., Kafka consumer offsets committed at the last checkpoint).

This is the standard approach for Timely-based systems. Partial recovery (replacing a single failed process without restarting the whole computation) would require a different dataflow engine.

### CLI Changes

```
rhei run pipeline.toml                                    # default: 1 worker, local
rhei run pipeline.toml --workers 4                        # Phase 1: 4 threads
rhei run pipeline.toml --workers 4 --process-id 0 \
    --peers "host0:2101,host1:2101"                       # Phase 2: TCP cluster
rhei run pipeline.toml --workers 4 --hostfile cluster.txt # alternative
```

---

## Phase 3: Full Control Plane

Add a Job Manager with consensus-backed metadata, gossip-based discovery, and dynamic scaling. This phase introduces the control plane described in [ARCHITECTURE.md](ARCHITECTURE.md).

### Job Manager

A gRPC service (built with `tonic`) that:

- Accepts pipeline submissions (`rhei submit pipeline.toml`).
- Schedules pipelines across available TaskManager workers.
- Tracks checkpoint metadata (latest committed checkpoint ID per job).
- Coordinates scaling events (add/remove workers, repartition).

The Job Manager does not handle user data. It is a metadata-only service.

### Consensus (OpenRaft)

The Job Manager uses [OpenRaft](https://github.com/datafuselabs/openraft) to replicate:

- Active job graphs and their configurations.
- Latest checkpoint IDs per job.
- Worker assignments (which workers are running which key ranges).

OpenRaft provides strong consistency for metadata. If the Job Manager leader fails, a follower takes over with the full committed metadata log. No metadata is lost.

### Discovery (chitchat)

TaskManager workers use [chitchat](https://github.com/quickwit-oss/chitchat) (gossip protocol) to:

- Announce their presence and capabilities (CPU, memory, disk).
- Emit heartbeats so failures are detected quickly.
- Propagate membership changes to all participants.

The Job Manager watches chitchat membership events to detect worker failures and trigger reassignment. Gossip-based failure detection is faster and more scalable than Raft leader polling for liveness.

### Dynamic Scaling

When a new worker joins or an existing worker is removed:

1. The Job Manager triggers a checkpoint on the running computation.
2. After the checkpoint commits, the Job Manager computes the new key range assignments.
3. Workers are notified of their new assignments and restart the computation from the checkpoint.
4. Each worker pulls state for its assigned key range from L3 (S3).

Because state is disaggregated (L3 is on S3, not local disk), there is no state migration between workers. A worker that receives a new key range simply reads the relevant keys from S3 into its L1/L2 caches on first access. This turns scaling into a cache-warming problem, not a data-transfer problem.

### Leader Election

OpenRaft handles Job Manager high availability. A cluster of 3 or 5 Job Manager instances runs the Raft protocol. The leader accepts client requests; followers replicate the log. If the leader fails, a new leader is elected automatically.

### Exactly-Once Semantics

Exactly-once processing requires coordinating source offsets and sink writes at checkpoint boundaries:

1. At checkpoint, the Job Manager instructs all workers to flush state to L3.
2. Once all workers confirm, the Job Manager commits the checkpoint (source offsets + checkpoint ID) atomically.
3. Sinks that support transactions (e.g., Kafka transactional producer) commit their pending writes as part of the checkpoint.

This is a two-phase commit: prepare (flush state + buffer sink writes) and commit (commit offsets + commit sink transactions). If any participant fails before commit, the entire checkpoint is aborted and the computation replays from the previous checkpoint.

### API

```
rhei submit pipeline.toml           # submit a pipeline to the cluster
rhei status                         # list running jobs and their status
rhei status <job-id>                # detailed status for a specific job
rhei cancel <job-id>                # cancel a running job
rhei scale <job-id> --workers N     # scale a job to N workers
rhei checkpoint <job-id>            # trigger an immediate checkpoint
```

---

## Key Design Decisions

These decisions apply across all phases.

### Exchange Pact Key Function

The exchange key function determines which worker processes each element:

```rust
|element| hash(element.key()) % num_workers
```

The hash must be deterministic and consistent across restarts (use a fixed-seed hash like `xxhash` or `seahash`, not `RandomState`). The key function should be derived from the operator's existing `key_fn` parameter, ensuring that the partitioning aligns with the operator's state access pattern.

### State Prefix Scheme

```
"p{process_id}/w{worker_index}/{operator_name}/{user_key}"
```

- `process_id` is omitted in Phase 1 (single process).
- `worker_index` is omitted in the current state (single worker).
- This scheme guarantees no key collisions across processes and workers, even if they share the same L3 backend on S3.

### Checkpoint Protocol

Chandy-Lamport style: checkpoint barriers are injected as special epochs into the data stream. When all operators across all workers and processes have processed epoch E, the checkpoint for E can be committed.

The protocol:

1. The coordinator (executor in Phase 1, process 0 in Phase 2, Job Manager in Phase 3) decides to checkpoint at epoch E.
2. Each worker processes all elements up to epoch E, then flushes its state to durable storage.
3. Each worker reports completion to the coordinator.
4. The coordinator commits the checkpoint ID and source offsets.
5. On recovery, all workers reload state from L3 and sources replay from the committed offsets.

### Serialization Boundary

Elements crossing worker or process boundaries must be serializable. This is enforced via trait bounds on `StreamFunction::Input` and `StreamFunction::Output`:

```rust
// Phase 1 (shared memory, no serialization needed):
type Input: Send + Clone;
type Output: Send + Clone;

// Phase 2 (TCP, serialization required):
type Input: Send + Clone + Serialize + DeserializeOwned;
type Output: Send + Clone + Serialize + DeserializeOwned;
```

Phase 1 does not strictly require serialization (shared-memory channels can move data by reference), but adding the bounds early avoids a breaking change at Phase 2.

---

## Migration Path

| Transition | Breaking changes | New dependencies |
|-----------|-----------------|-----------------|
| Current → Phase 1 | None. `--workers 1` is the default and behaves identically to `execute_directly()`. | None (Timely already supports `Config::process`). |
| Phase 1 → Phase 2 | `Serialize + DeserializeOwned` bounds on element types. Types containing `Rc`, closures, or other non-serializable fields will not compile. | None new (Timely TCP is built-in). |
| Phase 2 → Phase 3 | New infrastructure: Raft cluster (3+ Job Manager nodes), S3 or shared storage for checkpoints. CLI changes from direct `rhei run` to `rhei submit`. | `openraft`, `chitchat`, `tonic` (gRPC). |

---

## Appendix: Key Files

| File | Relevance |
|------|-----------|
| `rhei-runtime/src/executor.rs` | `execute_directly()` call site, source/sink bridge setup, operator slots |
| `rhei-runtime/src/timely_operator.rs` | `TimelyAsyncOperator` — capability management, `!Send` constraint |
| `rhei-runtime/src/bridge.rs` | `source_bridge` / `sink_bridge` — async-to-sync channel pattern |
| `rhei-runtime/src/async_operator.rs` | `AsyncOperator` — hot/cold path, stash, state context |
| `rhei-core/src/state/prefixed_backend.rs` | Per-operator key namespacing — extends to per-worker prefixing |
| `rhei-core/src/state/tiered_backend.rs` | L1/L2/L3 hierarchy — L3 shared across processes via S3 |
| `rhei-core/src/traits.rs` | `Source`, `Sink`, `StreamFunction` — serialization bound additions |
