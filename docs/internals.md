# Internals

How a `DataflowGraph` becomes running Timely operators, and what happens on each data path. This page is for people modifying Rhei or debugging it at the runtime level. For the user-facing model, read [concepts.md](concepts.md); for the high-level topology, [ARCHITECTURE.md](../ARCHITECTURE.md).

Everything here describes code on `main`, with file references so you can follow along.

---

## Layering

```text
┌─────────────────────────────────────────────────────────┐
│ PipelineController          rhei-runtime/src/controller.rs
│   configuration, cluster settings, lifecycle
│   run() / run_with_shutdown() / run_dynamic()
├─────────────────────────────────────────────────────────┤
│ TaskManager                 rhei-runtime/src/task_manager.rs
│   per-executor data, async bridges, checkpoint task,
│   watermark plumbing, DLQ drain
├─────────────────────────────────────────────────────────┤
│ DataflowExecutor            rhei-runtime/src/executor.rs
│   per-worker Timely DAG construction and execution
└─────────────────────────────────────────────────────────┘
```

Each layer owns one concern: the controller knows *what* to run, the task manager knows *how the outside world connects*, the executor knows *how Timely is wired*. A rescale rebuilds the bottom two while the controller persists — which is exactly what `run_dynamic` does.

*See also `INPROC-ARCH.md`.*

---

## From graph to dataflow

### 1. Building the logical graph

`DataflowGraph` holds nodes and edges behind interior mutability, so `Stream<'a, T>` can be `Copy` and still append to the graph. Each stream method adds a `NodeKind` and returns a new handle:

```text
NodeKind::Source | Transform | BatchOperator { name, op } | KeyBy | Merge | Sink
```

Type safety is entirely in `Stream<'a, T>`'s phantom parameter. The graph itself stores erased nodes: closures are boxed as `LazyBatchTransformNode`, operators as `TypedBatchOperatorNode`, key functions as `LazyKeyByNode`.

Why *lazy*: the closures are built per worker, not once. Each worker calls the thunk to get its own `Arc<dyn Fn>`, so no state is shared across workers by accident.

### 2. Validation

`graph.validate()` runs before anything is compiled and rejects:

| Error | Meaning |
|-------|---------|
| `EmptyGraph` | no nodes |
| `NoSources` | nothing produces data |
| `NoSinks` | nothing consumes it |
| `DanglingStreams` | a stream that never reaches a sink |

Dangling streams are worth catching: without the check, a forgotten `.sink()` produces a pipeline that runs and quietly discards a branch.

### 3. Compilation

`compiler.rs` topologically orders the graph into a `CompiledGraph`, handling fan-out (one node with several consumers), merge (several inputs), and multiple exchanges. The output is an ordering the executor can walk while guaranteeing every node's inputs exist before it is built.

### 4. Timely construction

`DataflowExecutor` walks the compiled graph inside `worker.dataflow(...)`, mapping each node to Timely operators:

| `NodeKind` | Timely construction |
|-----------|---------------------|
| `Source` | `OperatorBuilder` + `build_reschedule`, pulling from a flume channel |
| `Transform` | `unary` with `Pipeline` pact |
| `KeyBy` | `unary` (split) + `.exchange(..)` — two operators |
| `BatchOperator` | `unary_frontier` with `Pipeline` pact |
| `Merge` | `scope.concatenate(..)` |
| `Sink` | `unary` writing into a flume channel |

Every worker builds the **same** dataflow. Timely runs one copy per worker; the exchange pact is what makes them cooperate.

---

## The async bridge

Timely workers are **synchronous** threads. Sources and sinks are **async** Tokio tasks. Bridging them is where most of the runtime's subtlety lives.

```text
   async world                    │              sync world
                                  │
 Source (Tokio task)              │
   next_batch().await             │
   current_watermark()            │
        │                         │
        └──► flume::bounded(16) ──┼──► Timely source operator
                                  │      try_recv() → give(buf)
                                  │
                                  │    Timely sink operator
   Sink (Tokio task) ◄────────────┼──── blocking_send
   write_batch().await            │
                                  │
```

`DEFAULT_CHANNEL_SIZE` is **16** buffers per channel (`bridge.rs`). That is the backpressure knob: a slow sink fills its channel, the Timely operator's `blocking_send` stalls, and the stall propagates up through Timely's own scheduling to the source.

Timely itself runs inside `tokio::task::spawn_blocking`, so the whole dataflow occupies blocking-pool threads and never starves the async runtime driving sources and sinks.

### Source operator specifics

The source uses `build_reschedule` with `set_notify(false)` and drives itself via an activator. Each activation does one `try_recv`:

- **`Ok((buf, wm))`** — emit the buffer, update the source's watermark atomic, advance the epoch (`epoch.max(wm)` when a watermark exists, else `epoch += 1`), downgrade the capability, re-activate.
- **`Empty`** — re-activate and return; no blocking on the Timely thread.
- **`Disconnected`** — if the source reported `SourceExhausted`, raise the epoch to that sentinel and enter `draining`; otherwise drop the capability and finish.

Draining exists so a source that has run dry still holds a live capability while *other* sources finish — downstream windows closing at `SourceExhausted` need a valid capability to emit through.

### Sink specifics

Sinks use `blocking_send` from the Timely thread. Send failures cannot propagate out of a Timely closure, so they are logged at `error` and counted in `sink_send_errors_total`; the real error surfaces when the sink task's `JoinHandle` is awaited (KI-1).

---

## Data representation on the wire

### `RheiBuffer<T>` → `ErasedBuffer`

A typed buffer is a `RecordBatch` plus a selection mask. Erasing it keeps the batch and mask and adds a schema ID, so one Timely channel can carry heterogeneous types. Operators downcast on receipt; a mismatch is logged and the batch dropped rather than panicking.

Cross-process transport serializes with **Arrow IPC** — columnar, self-describing, and zero-copy on the read side.

### Selection vectors

`filter_fn` never copies. It computes a boolean mask over the physical rows and stores it alongside the batch. This is why two lengths exist:

- `len()` — selected rows
- `physical_len()` — rows in the underlying `RecordBatch`

Iteration skips masked-out rows; `iter().enumerate_physical()` exposes the physical index for mask construction. A chain of filters composes masks rather than materialising intermediates.

---

## The exchange, in detail

`build_batch_key_by` (`executor.rs`) builds **two** operators:

**Stage 1 — split, `Pipeline` pact.** For each buffer, `partition_for_exchange(&key_fn, &key_groups)` produces per-worker sub-buffers, each tagged with its `exchange_target`. Purely local; a columnar partition of one batch into N.

**Stage 2 — route, `Exchange` pact.**

```rust,ignore
// not-compiled: internal runtime code, shown for reference.
partitioned.exchange(|buf: &ErasedBuffer| buf.exchange_target().unwrap_or(0))
```

Splitting first means one routing decision per sub-buffer rather than per row.

### Routing must agree with state ownership

```text
key_group = seahash(key) % max_parallelism
worker    = key_group * effective_parallelism / max_parallelism
```

and the inverse:

```text
start = ceil(worker_index * max_parallelism / effective)
end   = ((worker_index + 1) * max_parallelism - 1) / effective
```

The ceiling on `start` is load-bearing. The intuitive floor-split tiles the key space correctly but disagrees with the routing formula whenever `max_parallelism` is not a multiple of the worker count — and disagreement means rows arriving at a worker that does not own their state, silently. Two tests pin this: `routing_agrees_with_ranges` across the parameter space, and `partition_key_agrees_with_state_ownership` end to end.

Surplus workers (beyond `max_parallelism`) get empty ranges. The split runs over `effective_parallelism` rather than `parallelism` so that routing — which clamps upward — and range assignment pick the same workers; splitting over all workers would leave *leading* workers empty while routing selected *trailing* ones.

*Source: `rhei-core/src/cluster/key_group.rs`.*

---

## Operator execution

Stateful operators are built with `unary_frontier`. Per activation:

1. **Process input.** Downcast each `ErasedBuffer` to `RheiBuffer<I>`, call `process`, emit outputs, route errors per `ErrorPolicy`.
2. **Advance time.** `wm = frontier_min_or_max(frontier.frontier())`; if it advanced, call `advance_time`, which invokes `on_watermark` and fires due timers.
3. **Maybe checkpoint.** `maybe_checkpoint(frontier, rt)` flushes state if the frontier moved past the last checkpointed epoch.
4. **Manage capability.** Keep one alive for watermark- and timer-driven output — including the final `SourceExhausted` flush — downgraded to the current frontier, released only when the frontier empties.

Step 4 is what lets a window emit results *after* its last input arrived.

### Async in a sync operator

`TimelyBatchOperator::process` calls:

```rust,ignore
// not-compiled: internal runtime code (timely_operator.rs).
match rt.block_on(self.op.process(input, &mut self.ctx)) { .. }
```

The Timely worker thread **blocks** on the operator future. For an L1 hit that is nanoseconds. For an L1 miss it is the full L2/L3 latency, with that worker stopped throughout.

The source comment says so directly: *"Process a type-erased input buffer. Blocks on the Tokio runtime."*

This is KI-11. `StreamFunction::process` borrows `&mut self` and `&mut StateContext` across the await, so a non-blocking path needs `'static` futures, state prefetch, or a split prepare/complete API. The scaffolding in `async_operator.rs` (`pending`, `drain_completed`, `poll_pending`) anticipates that redesign but is not on the live path.

---

## The epoch timeline

Timestamps, watermarks, and epochs share one `u64` timeline:

| Range | Meaning |
|-------|---------|
| `0 .. u64::MAX-2` | real event times, or a batch counter when sources emit no watermarks |
| `u64::MAX - 1` | `Sentinel::SourceExhausted` |
| `u64::MAX` | `Sentinel::Shutdown` |

`frontier_min_or_max` returns `u64::MAX` for an **empty** frontier — no more data will ever arrive, so everything is complete.

`compute_min_watermark` takes the minimum across source watermark atomics, skipping zeros so an unreported source does not pin the minimum at 0. It is used only by the source's draining logic; downstream operators use the frontier.

Full path: [time-and-watermarks.md](time-and-watermarks.md).

---

## State internals

### Read path

```text
KeyedState::get(&key)
  └─ encode: "{namespace}:{json(key)}"
      └─ StateContext::get_raw_keyed(partition_key, state_key)
          └─ physical_keyed → kg{group:05}/{operator}/{state_key}
              ├─ L1 dirty HashMap        hit → return
              ├─ L1 clean moka cache     hit → return
              ├─ L2 Foyer HybridCache    hit → promote to L1
              └─ L3 SlateDB              → promote
```

The key group comes from `partition_key` — the bytes the exchange routed on — not from `state_key`. Hashing `state_key` would put `counts:"alpha"` in a different group than routing put `alpha` in. Reads and writes would stay self-consistent, so nothing visibly breaks; what breaks is every per-key-group operation (warming, scanning, ownership reasoning). Fixed in `0ed4d07`.

### Write path

Writes land in the L1 dirty map and mark the key dirty. Nothing touches L2/L3 until a checkpoint. Dirty entries are never evicted — evicting them would lose uncheckpointed data — which is the unbounded-growth half of KI-7.

### Backend composition

```text
PrefixedBackend  → namespaces by operator
TieredBackend    → L2 Foyer over L3 SlateDB
ForkBackend      → copy-on-write: local writes, read-only remote fallback
LocalBackend     → filesystem, for single-process development
```

---

## Checkpoint flow

```text
frontier advances past last checkpointed epoch
    │
    ├─ TimelyBatchOperator::maybe_checkpoint
    │     rt.block_on(ctx.state.checkpoint())      L1 dirty → L2/L3
    │
    ├─ epoch sent on the flume checkpoint channel (u64, sync send)
    │
    ├─ checkpoint task (Tokio)
    │     ├─ cluster: CheckpointParticipant sends Ready{pid, epoch}
    │     │           process 0's CheckpointCoordinator collects all,
    │     │           broadcasts Committed{epoch}
    │     ├─ collect source offsets from the shared Arc<Mutex<HashMap>>
    │     └─ write the manifest
    │
    └─ sources commit offsets (on_checkpoint_complete)
```

Offsets are committed **after** the manifest, which is precisely what makes delivery at-least-once: a crash in that gap replays.

The checkpoint channel is a `flume` channel carrying `u64` epochs, chosen because the Timely thread can `send` synchronously without an await. `Sentinel::Shutdown` on that channel drives coordinated teardown so cluster processes tear down TCP together.

*Sources: `timely_operator.rs`, `task_manager.rs`, `checkpoint_coord.rs`.*

---

## Rescaling

`run_dynamic()` supervises topology generations:

1. Membership change observed (gossip, or an explicit call)
2. Debounce — `rescale_debounce_secs` of quiet, so a rolling restart is one rescale, not one per node
3. Checkpoint at the current topology
4. Tear down the Timely dataflow and the `TaskManager`
5. Recompute the key group assignment for the new worker count
6. Rebuild the `TaskManager`, restart Timely
7. Gaining workers fault their new groups in from shared L3 on first access

Step 7 is why there is a latency bump after a rescale: warming is lazy. Proactive warming is a tracked gap.

The controller survives all of this — only the bottom two layers are rebuilt.

---

## Concurrency map

| Component | Thread |
|-----------|--------|
| Timely workers | `spawn_blocking` pool, N per process |
| Source tasks | Tokio async, one per source (or per partition) |
| Sink tasks | Tokio async, one per sink per worker |
| Checkpoint task | Tokio async, one per process |
| Checkpoint coordinator | Tokio async, process 0 only |
| DLQ drain | Tokio async, one per worker |
| Gossip | Tokio async, when `chitchat` is enabled |
| HTTP server | Tokio async (axum) |

Cross-boundary channels: `flume::bounded(16)` for data, `flume` for checkpoint epochs, `tokio::sync::mpsc` for DLQ records.

---

## Where to look when debugging

| Symptom | Start at |
|---------|----------|
| Rows on the wrong worker | `key_group.rs` — `range_for_worker` vs `worker_for_key_group` |
| Windows never fire | `executor.rs` — `build_batch_source` epoch advancement |
| Throughput collapses | `timely_operator.rs:34` — the `block_on` cold path |
| State empty after restart | `checkpoint.rs` — `validate_compatible`, and the key-group prefix |
| Pipeline stalls under load | `bridge.rs` — `DEFAULT_CHANNEL_SIZE` backpressure |
| Rows silently disappear | `erased_buffer.rs` downcast failures; `sink_send_errors_total` |
| Cluster does not converge | `checkpoint_coord.rs`, and whether process 0 is alive |
