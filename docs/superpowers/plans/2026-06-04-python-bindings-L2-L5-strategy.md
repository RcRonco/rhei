# rhei Python Bindings L2-L5 — Implementation Strategy

**Date:** 2026-06-04. Source: parallel research workflow (4 deep-dives + synthesis), verified against source.

I have fully verified the architecture. The existing rhei-python already implements L0/L1: `Buffer` pyclass (wraps `ErasedBuffer`), `Dataflow`/`Stream`/`CollectHandle` pyclasses, `ListSource`/`PrintSink` erased adapters, `map_batches` (whole-batch transform via `add_erased_transform`), `sink`/`print`, and `run`. I now have everything needed to write the strategy.

---

# rhei Python Bindings: L2–L5 Implementation Strategy

## 0. Verified ground truth (read from source, not the agent reports)

These facts are confirmed by direct reading and override any conflicting claim in the agent JSON:

- **`ErasedBatchOperator` is `pub(crate)`** (`rhei-runtime/src/erased_batch.rs:152`) and has **6 methods**, including a **mandatory `fn clone_erased(&self) -> Box<dyn ErasedBatchOperator>`** (line 177). Multi-worker execution *calls* `clone_erased()` per worker: `task_manager.rs:922` `let (name, op) = (name.clone(), op.clone_erased());`. Any Python operator wrapper MUST implement it.
- **The `OperatorContext` is created fresh, per-worker, and never held by the operator.** `task_manager.rs:923-924`: `let ctx = controller.create_context_for_worker(&name, worker_idx)?; w_contexts.insert(nid, OperatorContext::new(ctx));`. It is threaded into each lifecycle call by `&mut` from `TimelyBatchOperator`, which runs **synchronously** via `rt.block_on(...)`. There is exactly one `&mut OperatorContext` borrow alive at a time, on one thread. **The L4 agent's claim that we must refactor `OperatorContext.state` to `Arc<Mutex<StateContext>>` is WRONG and unnecessary** — see L4 below.
- **`add_erased_operator` does not exist**, but adding it is trivial and mechanical: the executor already routes `NodeKind::BatchOperator` (`executor.rs:236`), and `task_manager.rs:982` compiles it via `op.compile()`. We only need a `PreErasedBatchOperatorNode` mirroring the existing `PreErasedSourceNode`/`PreErasedSinkNode` (`dataflow.rs:113-128`).
- **The erased data plane is complete and public**: `ErasedSource`/`ErasedSink` are `pub` (`erased_batch.rs:17,123`); `ErasedBuffer::from_parts` (line 70), `ErasedBuffer::as_record_batch` (line 109), `ErasedBuffer::downcast`, and `schema_hash_of(&Schema)` (line 262) are all `pub`.
- **`key_by` already works at the erased layer**: `DataflowGraph::add_key_by(input, key_fn: KeyFn)` (`dataflow.rs:287`), and `partition_for_exchange` (`erased_buffer.rs:164`) already respects the mask (line 181) and compacts output (`mask: None`, line 208). `KeyFn = Arc<dyn Fn(&RecordBatch, usize) -> String + Send + Sync>` (`erased_buffer.rs:21`).
- **`add_merge(a, b)`** is `pub` (`dataflow.rs:295`).
- **Current rhei-python state (already shipped, L0/L1)**: `Buffer` (frozen pyclass wrapping `ErasedBuffer`), `Dataflow`/`Stream`/`CollectHandle` (`unsendable` pyclasses), `ListSource` + `PrintSink` erased adapters, `Stream::map_batches` (whole-batch via `add_erased_transform`), `Stream::sink`/`print`, `Dataflow::run`. **L2 extends this; nothing is greenfield.**

The central monomorphization fact: **everything in the typed library (`Stream<T>`, `StreamFunction<Input,Output>`, all built-in operators, `KafkaSource`/`Source<Output=KafkaMessage>`) is generic over `T: RheiSchema`, which is a compile-time witness.** Python has no compile-time `T`. Therefore the typed library is structurally unreachable from Python. Every layer must target the erased plane.

---

## L2 — Stateless row API + key_by + merge

### Verdict on monomorphization
**(b) Build fresh erased transforms on `RecordBatch` + arrow-array, plus reuse the already-erased `add_key_by`/`add_merge`.** We do NOT reuse `Stream::map`/`filter_fn`/`flat_map` — those require `T::View`, `O::builder` (`dataflow.rs:474-621`), which need a compile-time `T`. We re-implement their *bodies* against `RecordBatch` with a runtime Arrow↔Python codec. This is the largest pure-codec effort and the foundation for L3/L4.

### Implementation
All new transforms produce a `BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>` and attach via the existing `graph.add_erased_transform(handle, fn)`. **No rhei-runtime changes for L2.**

New Rust module `rhei-python/src/codec.rs` — the keystone:
- `fn read_scalar(col: &dyn Array, row: usize, dt: &DataType, py: Python) -> PyObject` — `match dt` over the MVP type set: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Boolean`, `Utf8`/`LargeUtf8`, `Binary`/`LargeBinary`, `Null`, `Date32/64`, `Timestamp`. Use `arrow_array::cast::AsArray` (`as_primitive::<Int64Type>()`, `as_string::<i32>()`, `as_boolean()`, `as_binary::<i32>()`) then `.value(row)`. Check `col.is_null(row)` first → `py.None()`. Defer `List`/`Struct`/`Dictionary`/`Decimal` (raise `NotImplementedError`).
- `fn batch_row_to_dict(batch: &RecordBatch, row: usize, py: Python) -> PyResult<Bound<PyDict>>` — iterate `batch.schema().fields()`, `read_scalar` each column.
- `fn dicts_to_batch(rows: &[Bound<PyAny>], schema: &SchemaRef, py) -> PyResult<RecordBatch>` — build one `Box<dyn ArrayBuilder>` per field (`Int64Builder`, `StringBuilder`, …), `append_to_builder(builder, dt, value)` per cell (`None` → `append_null`), then `RecordBatch::try_new(schema, arrays)`.
- `fn append_to_builder(...)` — inverse `match dt`, downcast the boxed builder via `as_any_mut().downcast_mut::<Int64Builder>()`, `extract::<i64>()`.

New `Stream` methods (PyO3) in `rhei-python/src/dataflow.rs`, mirroring the typed bodies but type-erased:

| Python method | Erased body (the load-bearing logic) |
|---|---|
| `map(self, f, out_schema)` | downcast not needed — read `buf.as_record_batch()`; for each row build dict, call `f(dict)`, collect output dicts, `dicts_to_batch(rows, out_schema)`; `ErasedBuffer::from_parts(batch, None, schema_hash_of(&out_schema))`. Mirrors `dataflow.rs:474-506`. Output schema **must** be supplied by the user (no compile-time `O`). |
| `filter(self, f)` | Replicate `filter_fn` (`dataflow.rs:509-545`): allocate `mask_vec[num_rows]`, call `f(dict)->bool` per row, build `BooleanArray`, return `ErasedBuffer::from_parts(batch.clone(), Some(mask), buf.schema_id())` (schema_id unchanged — pure filter). Return `vec![]` if none pass. |
| `flat_map(self, f, out_schema)` | Per-row `f(dict)->list[dict]`, flatten, `dicts_to_batch`. Mirrors `dataflow.rs:584-621`. |
| `inspect(self, f)` | Per-row `f(dict)`; return the buffer unchanged (`vec![buf]`). Mirrors `dataflow.rs:671-694`. |
| `key_by(self, f)` | Build `KeyFn = Arc::new(move |b: &RecordBatch, i: usize| Python::with_gil(\|py\| { let d = batch_row_to_dict(b,i,py)?; f.call1((d,))?.extract::<String>() }).unwrap_or_else(\|_\| "__error__".into()))`; call existing `graph.add_key_by(handle, key_fn)`. |
| `merge(self, other)` | call existing `graph.add_merge(self.handle, other.handle)`. |

### GIL / error policy (L2)
Acquire the GIL **once per batch** (`Python::with_gil`), loop rows inside. For `key_by`, the `KeyFn` runs inside `partition_for_exchange` on a Timely worker thread — it must `Python::with_gil` itself (per call is acceptable for MVP; batch-amortized is a later optimization). On a Python exception, log + skip the row (matches the typed library's `tracing::error!`-then-`vec![]` convention at `dataflow.rs:486`).

### Resolved open questions (L2)
- **dict vs PyRecord:** use **dict** for MVP (simplest, fastest, no per-attr GIL). A zero-copy `PyRecord` view is a post-MVP optimization.
- **Mask propagation:** `filter` emits a mask via `from_parts`; downstream `map`/`flat_map` read `buf.as_record_batch()` which is the *physical* batch — so a downstream Python op would see masked rows. **Decision:** `filter` should **compact** before emitting (apply the mask via `arrow::compute::filter_record_batch`) so downstream row-dict ops never see filtered rows. This sidesteps the mask-awareness burden in the codec. Keep mask only where it's free (none, in practice, for the Python row API). Exchange/`key_by` already handle masks, but compacting on filter makes the whole Python path mask-free and simpler.
- **flat_map builder sizing:** start at `num_rows` capacity; builders grow. Non-issue.

---

## L3 — Built-in windows / aggregations

### Verdict on monomorphization — and a RE-SCOPE
**(b) Build ONE fresh erased windowed-aggregation operator on `RecordBatch` + arrow compute, with declarative agg specs. Reusing the typed `TumblingWindow`/`SlidingWindow`/`SessionWindow`/`TemporalJoin`/`SequenceDetect` is impossible** — all are generic over `I, O, Acc, KF, TF, AF, FF: RheiSchema/closures` (`rhei-core/src/operators/*.rs`) and implement `StreamFunction<Input=I, Output=O>`. There is no compile-time `I`/`O` from Python.

**Re-scope (honesty over completeness):** Building Python-callable equivalents of all five typed operators is disproportionate. The MVP L3 is **a single `ErasedTumblingWindowAgg` operator** with:
- `window_size_ms: u64`
- a **Python `key_fn`** and **Python `time_fn`** (called per row to extract group key string and event-time millis),
- a **declarative `Vec<AggSpec>`** where `AggSpec` is a Rust `enum { Count, Sum(col), Min(col), Max(col), Avg(col) }` exposed as a PyO3 class (e.g. `Agg.count()`, `Agg.sum("amount")`). Aggregation runs in pure Rust (no Python in the hot accumulation path).
- a fixed **output schema**: `[key: Utf8, window_start: Int64, window_end: Int64, <agg columns…>]`, derived in Rust from the specs.

**Explicitly deferred:** `SlidingWindow`, `SessionWindow`, `CountWindow`, `TemporalJoin`, `SequenceDetect`, and Python-defined custom accumulators. Tumbling + declarative aggs cover the dominant use case; the others come after L4 lands custom operators (a Python user can then express sliding/session/join logic via L4 `process`/`on_timer` if needed).

### Implementation
`ErasedTumblingWindowAgg` implements the (now-public) `ErasedBatchOperator` directly. It is **stateful**, so it requires the L4 plumbing (`add_erased_operator` + `pub ErasedBatchOperator`). This is why **L3 should be sequenced after L4's runtime change** (see Build Order).
- `process(input, ctx)`: for each row, `key = py_key_fn(row)`, `ts = py_time_fn(row)`, `window = ts / window_size_ms`; fold the row's agg columns into per-`(key, window)` accumulators held in `ctx.state` (`KeyedState`/`StateContext::put`). Use `ArrowAggregator` impls (`CountAgg`/`SumAgg`/`AvgAgg`, `rhei-core/src/arrow/aggregator.rs`) — they already operate on `RecordBatch` by column name **without `RheiSchema`** (verified by the L3 agent), which is exactly the schema-agnostic primitive we need.
- `on_watermark(wm, ctx)`: emit + evict windows whose `window_end <= wm`. Build the output `RecordBatch` from accumulators using arrow builders → `ErasedBuffer::from_parts(batch, None, schema_hash_of(&output_schema))`.
- `clone_erased`: clone the Arc'd Python callables + specs.

Accumulators are Serde-serializable Rust structs (e.g. `struct AvgAcc { sum: f64, count: u64 }`), so checkpoint/restore is automatic via `ctx.state.checkpoint()`.

### Python surface
```python
stream.tumbling_window(
    window_size_ms=10_000,
    key_fn=lambda row: row["user_id"],
    time_fn=lambda row: row["ts"],
    aggs=[Agg.sum("amount"), Agg.count(), Agg.avg("value")],
)  # returns a new Stream with the fixed window-output schema
```

### Resolved open questions (L3)
- **Output shape:** fixed `[key, window_start, window_end, …aggs]`. Rust dictates it; user does not supply a schema (unlike L2 `map`).
- **Finish functions:** declarative specs only for MVP — no Python in finish. Custom Python finish is an L4 concern.
- **Watermark-driven emission** (not data-driven) for tumbling, consistent with the typed window operators.

---

## L4 — Custom Python stateful operators + state + timers + checkpoint

### Verdict on monomorphization
**Expose only the erased plane.** Python operators are wrapped as `Box<dyn ErasedBatchOperator>`. Do **not** expose `StreamFunction`/`BufferOutput<T>`/`Stream<T>` — those need compile-time `Input`/`Output`. `ErasedBatchOperator` already returns `Vec<ErasedBuffer>` with no generic wrapper, which is exactly what we want.

### The corrected design (rejecting the L4 agent's `Arc<Mutex>` refactor)
The L4 agent recommended refactoring `OperatorContext.state` into `Arc<Mutex<StateContext>>`. **This is unnecessary and should NOT be done.** Verified reason: `TimelyBatchOperator` (`timely_operator.rs`) calls every lifecycle method with `&mut self.ctx` synchronously under `rt.block_on(...)`, on a single Timely worker thread. There is never concurrent access. The `&mut OperatorContext` lives entirely within one synchronous Rust call frame; the Python callback returns control before the borrow ends.

The clean pattern: the `PyOperator` wrapper holds the `&mut OperatorContext` **on the Rust stack for the duration of one `process`/`on_timer`/`on_watermark` call**, and hands Python a short-lived `PyStateHandle` that borrows it. Because PyO3 `#[pyclass]` cannot safely hold a Rust lifetime-bound `&mut`, use the standard **scoped raw-pointer-free bridge**: wrap the `&mut StateContext` in a `PyStateHandle` that stores it for the call's duration and is **invalidated when the call returns**. Two unsafe-free options, in order of preference:

1. **Preferred (no `unsafe`, no refactor):** `PyStateHandle` wraps `Rc<RefCell<Option<&mut StateContext>>>`-style scoping is not `'static`-safe across PyO3. Instead, give `OperatorContext.state` a *temporary* move-in/move-out: the wrapper `std::mem::take`-style swaps the `StateContext` into a `Py`-held cell for the call and swaps it back after. Since `StateContext` is owned (not `Clone`), use `Option<StateContext>` ownership transfer: move the owned `StateContext` out of `OperatorContext`, into a `PyStateHandle(Rc<RefCell<Option<StateContext>>>)`, run the Python call, then move it back. This is fully safe and requires **one small rhei-core change**: make `OperatorContext.state` an `Option<StateContext>` or add `OperatorContext::take_state()/restore_state()` helpers. Minimal, unsafe-free.

   - `PyStateHandle` methods (sync from Python; block on async internally via `tokio::runtime::Handle::current().block_on(...)`): `get(key)->Option<bytes>` (wraps `get_raw`), `put(key, bytes)` (`put_raw`), `delete(key)`, `register_timer(ts, key)` (via `ctx.timers().register`, `state/context.rs:244`).

2. State methods that are `async` (`get_raw`, `restore_timers`, `checkpoint`) are blocked-on inside the GIL on the Rust side — Python sees synchronous calls. The executor already does exactly this with `rt.block_on`.

### Required rhei-runtime / rhei-core changes (smallest unsafe-free set)
1. **Promote `ErasedBatchOperator` to `pub`** (`erased_batch.rs:152`): change `pub(crate) trait` → `pub trait`. Also re-export from `rhei-runtime/src/lib.rs`. (`#![allow(dead_code)]` already at top of file.)
2. **Add `DataflowGraph::add_erased_operator`** (`dataflow.rs`), mirroring `add_erased_source`:
   ```rust
   pub fn add_erased_operator(&self, input: ErasedHandle, name: &str,
       op: Box<dyn ErasedBatchOperator>) -> ErasedHandle
   ```
   Backed by a new `struct PreErasedBatchOperatorNode(Box<dyn ErasedBatchOperator>)` impl'ing `BatchOperatorNode` (returns `self.0` from `compile()`), and a `NodeKind::BatchOperator { name, op: Box::new(PreErasedBatchOperatorNode(op)) }`. Mechanically identical to `PreErasedSourceNode` (`dataflow.rs:113`). **Zero executor/task_manager changes** — they already handle `NodeKind::BatchOperator` and call `op.compile()` + `clone_erased()`.
3. **`OperatorContext` ownership helper** for the state-handle bridge (option 1 above) — one tiny method, no API break elsewhere.

### `PyOperator` wrapper (`rhei-python/src/operator.rs`)
Holds `Py<PyAny>` (the user's operator instance). Implements `ErasedBatchOperator`:
- `process`: GIL acquire → wrap `input` as `Buffer` (existing pyclass) → move `ctx.state` into `PyStateHandle` → `obj.call_method1("process", (buffer, handle))` → expect `list[Buffer]` → restore state → unwrap to `Vec<ErasedBuffer>`.
- `on_watermark` / `on_timer` / `open` / `close`: same pattern; `on_timer` receives `(timestamp, key, handle)`. Timers fire via `TimelyBatchOperator::process_timers` → `ctx.state.timers().drain_fired(wm)` (already wired).
- **`clone_erased`:** `Box::new(PyOperator { obj: Python::with_gil(|py| self.obj.clone_ref(py)), … })`. **Critical**: this clones the *same* Python object reference across workers. For multi-worker correctness, document that Python operators must be effectively stateless-in-Python (all mutable state in `PyStateHandle`), OR call a Python `__deepcopy__`/factory. MVP: single-worker for Python custom operators; multi-worker is a documented follow-up.

### Python surface
```python
class CountOp(rhei.Operator):
    def process(self, buffer, state):
        n = int.from_bytes(state.get(b"n") or b"\x00"*8, "little") + buffer.num_rows
        state.put(b"n", n.to_bytes(8, "little"))
        state.register_timer(buffer.max_ts() + 60_000, "flush")
        return [buffer]
    def on_timer(self, ts, key, state): ...

stream.operator("count", CountOp())
```

### Resolved open questions (L4)
- **`Arc<Mutex<StateContext>>`: NO.** Use scoped ownership move-in/move-out. No interior mutability, no `unsafe`, no precedent needed.
- **StateHandle granularity:** expose a curated `PyStateHandle` (`get`/`put`/`delete`/`register_timer`), not raw `StateContext`.
- **`on_error`:** `ErasedBatchOperator` has no `on_error` (only `StreamFunction` does, `traits.rs`). Route Python errors in `process` through the controller's `ErrorPolicy`/DLQ (L5) rather than a per-operator hook for MVP.

---

## L5 — Connectors (Kafka, Python Source/Sink) + hardening

### Verdict on monomorphization
- **Kafka: (c) target the fixed concrete schema.** `KafkaSource: Source<Output=KafkaMessage>` and `KafkaSink: Sink<Input=KafkaRecord>` are monomorphized to fixed types with fixed Arrow schemas (`kafka_schema.rs:137`: `[topic, partition, offset, key, payload, timestamp, headers_json]`; `kafka_sink.rs:91`: `[key, payload, headers_json]`). Wrap them with the **existing** `SourceWrapper`/`SinkWrapper` (`erased_batch.rs:38,131`) → `ErasedSource`/`ErasedSink` → existing `add_erased_source`/`add_erased_sink`. No new monomorphization, no runtime changes. Users adapt payload via L2 `map` post-source. The `kafka` feature is behind a flag on `rhei-core` — gate the Python Kafka module behind a matching `rhei-python` feature.
  - **One runtime helper needed:** `SourceWrapper`/`SinkWrapper` are `pub(crate)`. Either (a) add small `pub` constructor fns in rhei-runtime (`pub fn erase_source<S: Source>(s: S) -> Box<dyn ErasedSource>`), or (b) make the wrappers `pub`. Prefer (a) — narrower surface.
- **Python Source/Sink: (b) implement the public erased traits directly.** rhei-python already ships `ListSource`/`PrintSink` as `ErasedSource`/`ErasedSink` adapters. Generalize to `PySource`/`PySink` adapters holding `Py<PyAny>`, calling Python `next_batch()->Optional[Buffer]` / `write_batch(Buffer)`. Bridge async: the bridge tasks (`bridge.rs:24` `local_source_bridge`, `:78` `sink_drain`) call these on the Tokio runtime; do `Python::with_gil` + call the (synchronous) Python methods. For MVP keep Python source/sink methods **synchronous** (simpler than `call_method_async`).

### Hardening
- **Ctrl-C / graceful shutdown:** export `shutdown_signal()` (`shutdown.rs:56`) and wire `run()` to `PipelineController::run_with_shutdown(graph, shutdown)` (`controller.rs:597`). Release the GIL around the blocking run via `py.allow_threads(...)` (already the pattern in the existing `run`). Add periodic `py.check_signals()` polling (or a `ShutdownTrigger` driven from a Python SIGINT handler) so a `KeyboardInterrupt` during long Rust execution triggers `ShutdownTrigger::shutdown()`.
- **DLQ + ErrorPolicy:** expose `ErrorPolicy` (`rhei-core/src/dlq.rs:35`, `Skip`/`SendToDlq`) and the three sinks (`FileDlqSink`, `LogDlqSink`, `KafkaDlqSink`) as PyO3 classes; plumb through `PipelineControllerBuilder::error_policy`/`dlq_sink` (`controller.rs:227,235`). Surface as kwargs on `Dataflow.run(..., error_policy=, dlq_sink=)`. `DlqSink` is object-safe — no monomorphization issue.
- **Metrics:** pure passthrough — `Dataflow.run(..., metrics_addr="0.0.0.0:9090")` → `PipelineControllerBuilder::metrics_addr` (`controller.rs:268`). HTTP server auto-starts. No new code.

### Resolved open questions (L5)
- **Kafka scope:** ship fixed-schema end-to-end `KafkaSource`/`KafkaSink` only. Custom payload schemas via post-source L2 transforms. A low-level producer/consumer API is out of scope.
- **Python Source/Sink:** subclass `rhei.Source`/`rhei.Sink` base classes (Python), wrapped by Rust `PySource`/`PySink` erased adapters. Synchronous methods for MVP.
- **Ctrl-C:** integrate `py.check_signals()` polling in the run loop (transparent), not manual user polling.
- **DLQ/metrics:** kwargs on `run()`.

---

## Build order & resequencing (the key strategic call)

**L4's runtime change (`pub ErasedBatchOperator` + `add_erased_operator`) is the unlock for L3.** L3's windowed aggregation is itself an `ErasedBatchOperator` and needs exactly that plumbing plus `OperatorContext`/state access. Building L3 before L4's runtime change would mean inventing throwaway scaffolding. Therefore **do the L4 runtime change first, then L3 rides on it.**

Recommended sequence:

1. **L2 (codec + row API).** No runtime changes. Highest leverage: the Arrow↔Python `codec.rs` (read_scalar / dicts_to_batch / append_to_builder) is reused by L3 (window output rows) and L4 (`Buffer` row access). `key_by`/`merge` are thin wrappers over existing public APIs. Ship this first; it makes the Python API genuinely useful (map/filter/flat_map/inspect/key_by/merge).

2. **L4 runtime change + custom operators.** Promote `ErasedBatchOperator` to `pub`; add `PreErasedBatchOperatorNode` + `add_erased_operator`; add `OperatorContext` state move-in/move-out helper; build `PyOperator` + `PyStateHandle`. This is the structural foundation for all stateful work.

3. **L3 (tumbling window agg).** Now trivially built as an `ErasedBatchOperator` on top of step 2's plumbing, reusing the L2 codec for output construction and the `ArrowAggregator` column kernels. Scope to **tumbling + declarative aggs only**; defer sliding/session/join/sequence.

4. **L5 (connectors + hardening).** Mostly passthrough/wrapping over existing public APIs (`run_with_shutdown`, `ErrorPolicy`, DLQ sinks, `metrics_addr`) + Kafka wrappers (needs the small `pub fn erase_source/erase_sink` helper) + `PySource`/`PySink` adapters generalizing the existing `ListSource`/`PrintSink`.

### Total rhei-runtime/rhei-core changes required (complete list, all unsafe-free)
1. `erased_batch.rs`: `pub(crate) trait ErasedBatchOperator` → `pub trait`; re-export in `lib.rs`. **(L3+L4)**
2. `dataflow.rs`: add `PreErasedBatchOperatorNode` + `DataflowGraph::add_erased_operator(input, name, op)`. **(L3+L4)**
3. `context.rs`: `OperatorContext` state move-out/move-in helper (e.g. `take_state()`/`set_state()` or make `state: Option<StateContext>`) for the `PyStateHandle` bridge. **(L4)**
4. rhei-runtime: `pub fn erase_source<S: Source>(s) -> Box<dyn ErasedSource>` / `erase_sink<K: Sink>(k)` (or make `SourceWrapper`/`SinkWrapper` `pub`). **(L5 Kafka)**

Notably **NOT required**: the `Arc<Mutex<StateContext>>` refactor (rejected), any executor/task_manager changes (the `NodeKind::BatchOperator` path already compiles, contextualizes, clones, and timer-drives any `ErasedBatchOperator`), and any change to checkpoint/restore (automatic via `ctx.state.checkpoint()` on frontier advance, restore via `create_context_for_worker` + `restore_timers`).

### Relevant files
- Erased plane (read for all layers): `/Users/roncohen/workspace/frisk/rhei-runtime/src/erased_buffer.rs`, `/Users/roncohen/workspace/frisk/rhei-runtime/src/erased_batch.rs`
- Graph builder to extend: `/Users/roncohen/workspace/frisk/rhei-runtime/src/dataflow.rs`
- Executor/operator wiring (read-only, no changes): `/Users/roncohen/workspace/frisk/rhei-runtime/src/executor.rs` (`build_batch_operator` ~480), `/Users/roncohen/workspace/frisk/rhei-runtime/src/task_manager.rs` (extract/clone_erased ~880-985), `/Users/roncohen/workspace/frisk/rhei-runtime/src/timely_operator.rs`
- State/context: `/Users/roncohen/workspace/frisk/rhei-core/src/arrow/context.rs`, `/Users/roncohen/workspace/frisk/rhei-core/src/state/context.rs`, `/Users/roncohen/workspace/frisk/rhei-core/src/state/timer_service.rs`
- Aggregators for L3: `/Users/roncohen/workspace/frisk/rhei-core/src/arrow/aggregator.rs`
- Connectors for L5: `/Users/roncohen/workspace/frisk/rhei-core/src/connectors/batch/kafka_source.rs`, `.../kafka_sink.rs`; `/Users/roncohen/workspace/frisk/rhei-core/src/dlq.rs`; `/Users/roncohen/workspace/frisk/rhei-runtime/src/shutdown.rs`, `/Users/roncohen/workspace/frisk/rhei-runtime/src/controller.rs`
- Python crate to extend: `/Users/roncohen/workspace/frisk/rhei-python/src/{lib.rs,dataflow.rs,buffer.rs}` (add `codec.rs`, `operator.rs`, and a `kafka.rs`/`dlq.rs` module)