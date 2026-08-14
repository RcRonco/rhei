# Concepts and Terminology

The vocabulary you need to read the rest of the documentation, the metric names, and the source. Terms are grouped by the layer they belong to, and each entry says where the thing lives in the code.

If you are new to Rhei, read [getting-started.md](getting-started.md) first, then this page, then the [walkthrough](walkthrough.md).

---

## How Rhei works, in one paragraph

Every bolded term below has its own entry further down this page. Read this
paragraph for the shape of the system, then look up whichever term you need.

You describe a pipeline as a **dataflow graph**: sources, transforms, stateful operators, sinks. Data moves through it as **Arrow record batches**, not individual rows. The graph is compiled into a **Timely Dataflow** program that runs on N **workers**. `key_by` is the only thing that moves rows between workers; it routes each row to the worker that owns its **key group**. Operators keep **keyed state** in a three-tier hierarchy backed by object storage. As Timely's **frontier** advances, the runtime takes a **checkpoint**: dirty state is flushed and source **offsets** are committed. Recovery replays from the last checkpoint, which makes delivery **at-least-once**.

---

## Data representation

### `RheiSchema`

The trait that makes a Rust struct a valid stream element. You never implement it by hand — `#[derive(RheiSchema)]` generates the Arrow schema, a columnar builder, a zero-copy view, and typed column accessors.

```rust,ignore
// not-compiled: illustrative shape of what the derive generates.
#[derive(Clone, rhei::RheiSchema)]
struct PageView { user_id: String, path: String }
// generates: PageViewBuilder, PageViewView<'a>, PageViewColumns<'a>
```

Primitives are not stream elements. `String` does not implement `RheiSchema`; wrap it in a struct.

*Source: `rhei-core/src/arrow/schema.rs`, `rhei-macros/src/rhei_schema/`.*

### View

A **zero-copy row reference** borrowed directly out of an Arrow array — `PageViewView<'a>` above. Every closure you pass to `map`, `filter_fn`, `key_by`, `flat_map`, or `inspect` receives a view, not an owned value.

This is why doc examples call `.to_string()` on string fields: a `String` column reads back as `&'a str`. Copying it is *your* choice, made per field, rather than a cost paid on every row.

### `RheiBuffer<T>`

A typed batch: an Arrow `RecordBatch` plus a **selection vector** (a boolean mask). Filtering sets mask bits instead of copying rows, so `filter_fn` on a million-row batch allocates one bitmap rather than a new batch.

Two lengths matter, and they differ after a filter:

| Method | Meaning |
|--------|---------|
| `len()` | Number of *selected* (visible) rows |
| `physical_len()` | Number of rows in the underlying `RecordBatch` |

Iterating `&buffer` yields only selected rows. `iter().enumerate_physical()` gives you the underlying index when you need to build a mask.

*Source: `rhei-core/src/arrow/buffer.rs`.*

### `ErasedBuffer`

A `RheiBuffer<T>` with the type parameter erased — a `RecordBatch` plus mask plus a schema ID — so that Timely can transport buffers of mixed types through one channel. Serialization across processes uses **Arrow IPC**. Operators downcast back to `RheiBuffer<T>` on receipt.

An `ErasedBuffer` crossing an exchange also carries an `exchange_target`: the worker index computed for it during the split stage.

*Source: `rhei-runtime/src/erased_buffer.rs`.*

### `BufferOutput<T>`

What an operator returns: `None` (nothing to emit), `Single(buffer)`, or `Multi(vec)`. Windows use `Multi` when several windows close at once.

---

## Graph and API

### `DataflowGraph`

The container holding the topology. Streams borrow from it, so it must outlive them. `validate()` runs before execution and rejects an `EmptyGraph`, `NoSources`, `NoSinks`, or `DanglingStreams` (a stream that never reaches a sink).

### `Stream<'a, T>`

A `Copy` handle to a point in the graph. Because it is `Copy`, reusing a handle produces **fan-out** — both consumers see every row, implemented by Timely's internal tee.

There is **one** stream type. Rhei has no `KeyedStream`, so "call `key_by` before a stateful operator" is a convention the compiler does not check. See [exchange-and-partitioning.md](exchange-and-partitioning.md#the-missing-guarantee).

### `StreamFunction`

The operator trait. Required method:

```text
async fn process(&mut self, RheiBuffer<Input>, &mut OperatorContext) -> Result<BufferOutput<Output>>
```

Defaulted hooks: `open`, `close`, `on_watermark`, `on_timer`, `on_error`.

`.operator()` requires `Clone` — the operator is cloned once per worker, so each worker gets its own instance and its own state handle.

### `Source` / `Sink`

`Source::next_batch()` produces `Option<RheiBuffer<T>>`; `None` means exhausted. Sources also expose `current_watermark`, `current_offsets` / `restore_offsets`, and optional partitioning via `partition_count` / `create_partition_source`.

`Sink::write_batch()` consumes buffers, with an optional `flush`.

Both run as **async Tokio tasks**, bridged to synchronous Timely workers by bounded channels — see [internals.md](internals.md#the-async-bridge).

---

## Execution

### Worker

One Timely worker thread. Workers are shared-nothing: each has its own L1 memtable and L2 cache, and processes a disjoint set of key groups. `--workers N` sets the count per process.

### Process

One OS process running `N` workers. In a cluster, processes are numbered by `--process-id` and connect over TCP. Total workers = `processes × workers_per_process`.

### Epoch

The Timely timestamp — a `u64` attached to every batch. When a source emits watermarks, **the epoch is the watermark**. When it does not, the epoch is a monotonic batch counter. Progress on this timeline is what drives both window firing and checkpointing.

Two sentinel values sit at the top of the range:

| Sentinel | Value | Meaning |
|----------|-------|---------|
| `SourceExhausted` | `u64::MAX - 1` | A source has run dry; forces pending windows to close |
| `Shutdown` | `u64::MAX` | Teardown coordination on the checkpoint channel |

*Source: `rhei-runtime/src/executor.rs`, `enum Sentinel`.*

### Frontier

Timely's progress guarantee: the set of epochs that may still arrive on a channel. When the frontier advances past epoch `E`, no more data at `E` or earlier will appear.

This is the mechanism behind two things at once — operators read the frontier minimum as **the current watermark**, and the runtime treats frontier advancement as the **checkpoint trigger**. See [time-and-watermarks.md](time-and-watermarks.md).

### Pact

Timely's term for how a channel connects two operators.

| Pact | Behaviour | Used for |
|------|-----------|----------|
| `Pipeline` | Stays on the same worker, no serialization | every stateless transform, and stateful operators |
| `Exchange` | Routes each item to a computed worker | `key_by` only |

---

## Partitioning

### Key

The `String` returned by your `key_by` closure. It determines both which worker processes a row and where that row's state lives — those two must agree, which is why both go through key groups.

### Key group

A bucket of keys that always moves between workers as one unit:

```text
key_group = seahash(key) % max_parallelism
```

Workers own **contiguous ranges** of key groups. Key groups are the indirection that makes rescaling possible: `max_parallelism` is fixed for a pipeline's lifetime, so a key's group never changes, while the range→worker mapping is recomputed whenever the worker count changes.

The math is deliberately identical to Flink's `KeyGroupRangeAssignment`.

*Source: `rhei-core/src/cluster/key_group.rs`.*

### `max_parallelism`

The number of key groups; default **128**, upper bound 32,768. It is a hard ceiling on useful parallelism — workers beyond it own no keyed state and act as standby capacity.

Changing it re-partitions the entire key space and invalidates every existing checkpoint, so it is recorded in the checkpoint manifest and **validated on restore**.

### Rescaling

Changing the worker count between (or during) runs. Because state is addressed by key group, a worker that gains a range simply reads those groups from shared L3 — there is no state migration, only cache warming on the gaining worker.

---

## State

### `StateContext`

The per-operator state handle, reached as `ctx.state`. You never construct one; the runtime creates it per worker per operator.

### `KeyedState<K, V>`

The typed wrapper you normally use: `get(&K) -> Result<Option<V>>`, `put(&K, &V) -> Result<()>`. Values are serialized with JSON by default (`JsonEncoder`) or bincode via `with_encoder`.

`put` returns a `Result` — it is not infallible.

Other shapes: `ValueState`, `ListState`, `MapState`, and `TimerService` for event-time timers.

### State tiers

| Tier | Backend | Role |
|------|---------|------|
| L1 | `HashMap` (dirty) + `moka` W-TinyLFU cache (clean) | Hot working set, in-process |
| L2 | Foyer `HybridCache` | Local NVMe cache |
| L3 | SlateDB on object storage | Durable source of truth |

A read tries L1, then L2, then L3. **An L1 miss blocks the Timely worker thread** while the fetch runs — see [state-and-checkpointing.md](state-and-checkpointing.md#the-cold-path-blocks).

### Physical key layout

```text
kg{group:05}/{operator}/{state_key}
```

Group first, zero-padded, so a whole key group is a contiguous scan in an ordered store. The group is derived from the **partition key** (your `key_by` output), never from the storage key a state wrapper builds.

*Source: `rhei-core/src/state/key_group_addressing.rs`.*

---

## Checkpointing and recovery

### Checkpoint

A consistent snapshot: L1 dirty keys are flushed through to SlateDB, then source offsets are recorded. Triggered when the frontier advances, at most every `checkpoint_interval` batches (default 100).

### Checkpoint manifest

The JSON record of a checkpoint: `checkpoint_id`, `timestamp_ms`, operator names, `source_offsets`, and topology metadata (`n_processes`, `workers_per_process`, `max_parallelism`, `total_workers`, `cluster_members`).

`max_parallelism` mismatch on restore is rejected; a different worker count is fine — that is what rescaling means.

*Source: `rhei-core/src/checkpoint.rs`.*

### Coordinated checkpoint

In multi-process mode, process 0 runs a `CheckpointCoordinator` over TCP. Each process sends `Ready { process_id, epoch }` after its local flush; once all report, the coordinator broadcasts `Committed { epoch }` and the merged manifest is written.

*Source: `rhei-runtime/src/checkpoint_coord.rs`.*

### Delivery semantics

**At-least-once.** Offsets are committed after a checkpoint completes, so a crash between processing and checkpointing replays the affected records. Exactly-once needs a transactional sink and two-phase commit; neither is implemented.

### Fork mode

Restore a production checkpoint locally with `--from-checkpoint <url>` plus `--offset-delta N`, using a copy-on-write `ForkBackend`: local writes stay local, reads fall back to the read-only remote. Worker count must match the manifest.

---

## Time

### Event time vs processing time

Rhei windows on **event time** — the timestamp your `time_fn` extracts from the record. Processing time (wall clock) is not a windowing mode.

### Watermark

An assertion that no event with a timestamp below this value will arrive. Sources produce them via `current_watermark()`; downstream operators read the **Timely frontier minimum** as the watermark. When it advances, `on_watermark` fires and windows close.

### Allowed lateness

Grace period past `window_end` before an event is considered late. Late events are dropped and counted in `late_events_dropped_total`. Routing them to a side output is **not implemented**.

---

## Error handling

### `ErrorPolicy`

`Skip` (default) logs a warning and drops the failed batch. `SendToDlq` routes failed records to the configured `DlqSink`.

### `DlqSink`

Dead-letter destination: `FileDlqSink`, `LogDlqSink`, or `KafkaDlqSink`. Configured on the controller with `.dlq_sink(..)` — there are no per-stream `.dlq()` methods.

### `PipelineError`

Classifies errors as `Retriable`, `BadData`, or `SystemError` so a DLQ consumer can route them.

---

## Process layering

```text
PipelineController   config, lifecycle, cluster settings
        │
   TaskManager       background services, async bridges, watermark plumbing
        │
 DataflowExecutor    per-worker Timely DAG compilation and execution
```

*Detail: [internals.md](internals.md) and `INPROC-ARCH.md`.*

---

## Terms you will not find in Rhei

Worth stating, because they exist in comparable systems and readers look for them:

| Term | Status |
|------|--------|
| `KeyedStream` | No such type. One `Stream<'a, T>` |
| Job manager / scheduler / gRPC submission | Not implemented. Processes are started externally |
| Leader election, consensus store | Not implemented. Process 0 is the coordinator by convention |
| Exactly-once | Not implemented. At-least-once only |
| Side outputs for late events | Not implemented |
| Processing-time windows | Not implemented. Event time only |
| Broadcast / global state | Not implemented |
| SQL / Table API | Not implemented |
