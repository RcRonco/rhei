# rhei Python Bindings — Design

**Date:** 2026-06-03
**Status:** Approved (brainstorming complete; pending implementation plan)

## Context

`rhei` is a Rust columnar stream-processing framework. Today, pipelines are
authored and run in Rust: a user defines record types with `#[derive(RheiSchema)]`,
builds a graph with the generic `Stream<T>` builder, and runs it via
`PipelineController`. Execution is Arrow-columnar on Timely Dataflow across one or
more blocking worker threads, with tiered durable state (L1 memtable → L2 Foyer →
L3 SlateDB) and checkpoint/restore.

We want **Python to be a first-class surface to author *and* run complete pipelines
end-to-end**, with custom logic written in Python — not merely a config DSL or a
UDF plug-in for Rust-authored graphs. A data engineer should be able to
`pip install rhei`, write a `.py` file, and `python pipeline.py` to run a real
multi-worker, checkpointed streaming pipeline.

### Locked decisions (from brainstorming)

1. **Goal:** Author + run full pipelines from Python, with custom Python logic in
   the data path.
2. **UDF data model:** Mirror the Rust model Pythonically. The Arrow **`Buffer`**
   (a batch) is the core primitive that flows through the pipeline. A **`Record`**
   is a zero-copy *view* over one row of a `Buffer` — the analog of rhei's
   `RheiBuffer<T>` + `View<'a>`. Both a batch API (vectorized, fast) and a
   row/Record API (ergonomic, sugar over the batch path) exist.
3. **Schema declaration:** v1 base is an explicit `pyarrow.schema([...])`. A
   dataclass/pydantic decorator (`@rhei.record`) is a deliberate *later* additive
   sugar layer, not built in v1.
4. **Execution model:** In-process embedded runtime via PyO3. `python pipeline.py`
   boots the Rust tokio+Timely runtime inside the Python process. Python UDFs are
   called back under the GIL; the GIL is released for all pure-Rust work.
   Architected so a future Python 3.13 free-threaded build can parallelize UDFs
   with no API change — but that is not built now.
5. **v1 surface (FULL):** stateless transforms + sources/sinks + rhei's built-in
   stateful operators configured from Python + a Python protocol for *custom
   stateful operators*. Designed to be phased into independently shippable layers.
6. **Run API:** blocking `df.run(workers=4, checkpoint_dir="./ckpt")` that owns the
   tokio runtime internally and blocks until completion/Ctrl-C. async is a later
   escape hatch, not v1.

## The central architectural decision

**The Python bindings target rhei's *erased* layer directly, not the generic
`Stream<T>` API.** The codebase already contains a complete runtime-typed erasure
layer, built for Timely transport, that the generic API lowers to before execution:

- `rhei-runtime/src/erased_buffer.rs` — `ErasedBuffer`: a `RecordBatch` + optional
  `BooleanArray` selection mask + a `schema_id` (seahash of the Arrow schema).
  Schema is *runtime data*. Already serializes via Arrow IPC for Timely Exchange.
- `rhei-runtime/src/erased_batch.rs` — object-safe `Box<dyn>` traits over
  `ErasedBuffer`: `ErasedSource`, `ErasedSink`, `ErasedBatchOperator`.
- `rhei-runtime/src/dataflow.rs` — `Stream<T>::map/filter/flat_map` erase
  immediately; e.g. `map` produces a boxed `Fn(ErasedBuffer) -> Vec<ErasedBuffer>`
  and `downcast::<T>()`s internally. The generic `T` is a **compile-time-only proof**
  that schemas line up; the executor never sees it.

**Consequence:** there is exactly one Rust type that flows through a Python-authored
graph — `ErasedBuffer`. Python's `Buffer` is a thin handle around it. Every Python
UDF is boxed as the *same* closure / `ErasedBatchOperator` the Rust API already
produces. **No monomorphization over Python types is ever required.** The executor,
exchange, checkpointing, and state machinery work unchanged. The Python layer is a
*peer* of the typed `Stream<T>` facade — both lower to the same `DataflowGraph`
node list.

### Required Rust changes

1. **Promote a small `pub` erased-builder API** on `DataflowGraph`. Today the
   erased traits and `add_node`/`into_nodes` plumbing are `pub(crate)`. Add a
   deliberately small, documented `pub` surface (e.g. `add_erased_source`,
   `add_erased_transform`, `add_erased_operator`, `add_key_by`, `add_merge`,
   `add_erased_sink`). This is also the foundation a future JSON-graph loader or
   dynamic SQL planner would need.
2. **`erased_buffer.rs`:** expose `pub fn schema_hash_of(&Schema) -> u64`; allow
   constructing an `ErasedBuffer` from an imported `RecordBatch` + explicit
   `schema_id`.
3. **`OperatorContext` refactor (to stay `unsafe`-free):** make state reachable
   without leaking an exclusive `&mut OperatorContext` borrow into Python (e.g. a
   cloneable `Arc`-based state handle). `unsafe_code = "forbid"` is a workspace
   rule; we keep it. We explicitly reject carving an `unsafe` exception for a
   leased raw pointer.

## The Python API

### End-to-end example

```python
import pyarrow as pa
import rhei
from rhei import Dataflow, Buffer
from rhei.connectors import KafkaSource, KafkaSink

TXN   = pa.schema([("account_id", pa.string()), ("amount_cents", pa.int64()),
                   ("event_time", pa.timestamp("ms"))])
ALERT = pa.schema([("account_id", pa.string()), ("total_cents", pa.int64())])

class VelocityGuard(rhei.Operator):            # custom stateful op in pure Python
    input_schema, output_schema = TXN, ALERT
    def process(self, batch: Buffer, ctx: rhei.Context) -> rhei.Emit:
        total = ctx.value_state("total", default=0)
        for txn in batch:                       # zero-copy Record views
            total.set(total.get() + txn["amount_cents"])
        return rhei.Emit.none()
    def on_timer(self, ts, key, ctx):
        return rhei.Emit.row({"account_id": key,
                              "total_cents": ctx.value_state("total").get()})

df = Dataflow()
(df.source(KafkaSource(brokers="...", topic="txns", schema=TXN),
           event_time="event_time")
   .filter(lambda t: t["amount_cents"] > 0)
   .key_by(lambda t: t["account_id"])
   .operator("velocity", VelocityGuard())
   .sink(KafkaSink(brokers="...", topic="alerts", schema=ALERT)))

df.run(workers=4, checkpoint_dir="./ckpt")      # blocks until Ctrl-C / exhaustion
```

### Two transform APIs (both lower to the same node)

**Row / Record API** — ergonomic; `fn` sees a zero-copy `Record` view over one row:

```python
stream.map(lambda t: {"acct": t["account_id"], "usd": t["amount_cents"] / 100.0},
           schema=DOLLARS)         # schema required: output type can't be inferred
stream.filter(lambda t: t["merchant"] != "TEST")          # no schema (type unchanged)
stream.flat_map(lambda t: [t, t] if big(t) else [], schema=TXN)
stream.inspect(lambda t: print(t["account_id"]))
```

**Batch / Buffer API** — vectorized, the fast path; `Buffer.to_arrow()` is
**zero-copy** to pyarrow:

```python
import pyarrow.compute as pc
def to_dollars(batch: Buffer) -> Buffer:
    rb = batch.to_arrow()                                 # zero-copy pyarrow.RecordBatch
    usd = pc.divide(rb["amount_cents"], 100.0)
    return Buffer.from_arrow(pa.record_batch([rb["account_id"], usd], schema=DOLLARS))
stream.map_batches(to_dollars, schema=DOLLARS)
stream.filter_batches(lambda b: pc.greater(b.to_arrow()["amount_cents"], 0))  # mask, zero-copy
```

`Record` supports `t["field"]`, `t.field`, `t.to_dict()`. Indexing decodes a column
to a native Python scalar lazily (primitives copy, strings borrow) — mirroring
`RheiSchema::view` semantics. `schema=` is required exactly where Rust requires
`O: RheiSchema` (type-changing transforms); not required for `filter`/`inspect`/
`key_by`.

### Built-in stateful operators (declarative, no per-row GIL)

Built-in operators take Rust closures at construction. From Python we expose them as
declarative configs so the hot loop stays in Rust:

```python
from rhei.windows import TumblingWindow
from rhei import agg
(stream.window(TumblingWindow(size=timedelta(minutes=1), time="event_time"))
       .key_by(key="account_id")
       .aggregate(total_cents=agg.sum("amount_cents"),
                  txn_count=agg.count(),
                  merchants=agg.distinct_count("merchant")))   # output schema derived
```

`agg.sum/count/min/max/mean/distinct_count` map to existing `ReduceOp` /
`RollingAggregateOp` + Arrow compute kernels. Output schema is derived from the agg
spec + key column. This is the **pit-of-success** path: the fast thing is the easy
thing; a custom Python `process` is the escape hatch for arbitrary logic.

`TemporalJoin`, `SequenceDetect`, `SlidingWindow`, `SessionWindow`, `CountWindow`
are exposed the same way — Python supplies config + key/time column names.

### Custom Python stateful operator protocol

```python
class Operator:
    input_schema: pa.Schema      # required class attrs — Python analogue of T: RheiSchema
    output_schema: pa.Schema
    def open(self, ctx: Context) -> None: ...                          # optional
    def process(self, batch: Buffer, ctx: Context) -> Emit: ...        # required
    def on_watermark(self, watermark: int, ctx: Context) -> Emit: ...  # optional
    def on_timer(self, timestamp: int, key: str, ctx: Context) -> Emit: ...  # optional
    def close(self) -> None: ...                                       # optional
```

`Emit` is the Pythonic `BufferOutput<T>` (None / Single / Multi):

```python
Emit.none()             # BufferOutput::None
Emit.row(dict)          # one row -> a 1-row Buffer
Emit.rows(iterable)     # many rows -> Single buffer
Emit.buffer(Buffer)     # BufferOutput::Single (vectorized)
Emit.buffers([Buffer])  # BufferOutput::Multi (window-style)
```

Lenient runtime sugar: `process` returning `None` ≡ `Emit.none()`; returning a
`dict`/`list[dict]` is auto-wrapped. The `.pyi` advertises `Emit` as the documented,
greppable form.

State + timers via `Context`:

```python
class Context:
    key: str            # the key_by value for this batch ("" if un-keyed)
    watermark: int      # current event-time watermark (ms)
    timers: Timers      # ctx.timers.register(ts_ms, key)
    def value_state(self, name, *, default=None) -> ValueState: ...
    def map_state(self, name) -> MapState: ...
    def list_state(self, name) -> ListState: ...
```

State values are JSON-serializable Python objects, encoded with the existing
`KeyedState` JSON encoder.

### `@op` — functional stateless sugar (mirrors `#[rhei::op]`)

```python
@op(input=TXN, output=ALERT)
def normalize(batch: Buffer, ctx: Context) -> Emit: ...
# usage: stream.operator("normalize", normalize)
```

### Module layout

```
rhei/                         # Python package (maturin-built)
├── __init__.py               # Dataflow, Record, Buffer, Operator, Context, Emit, agg, op
├── _rhei.so                  # compiled PyO3 extension (rhei-python crate)
├── connectors/               # VecSource, PrintSink; KafkaSource/KafkaSink (kafka extra)
├── windows/                  # TumblingWindow, SlidingWindow, SessionWindow, CountWindow
├── joins/                    # TemporalJoin
├── patterns/                 # SequenceDetect, AfterMatch, Side
├── agg.py                    # sum/count/min/max/mean/distinct_count specs
├── state.py                  # ValueState/MapState/ListState typing stubs
└── op.py                     # @op decorator
```

Core signatures (`.pyi` shape):

```python
class Dataflow:
    def source(self, src, *, event_time: str | None = None,
               watermark_interval: int | None = None) -> Stream: ...
    def run(self, *, workers: int = 1, checkpoint_dir: str = "./ckpt",
            checkpoint_interval: int = 100, metrics_addr: str | None = None,
            name: str | None = None, on_error: str = "dlq") -> None: ...  # blocking
    def validate(self) -> None: ...

class Stream:
    def map(self, fn, *, schema: pa.Schema) -> Stream: ...
    def filter(self, fn) -> Stream: ...
    def flat_map(self, fn, *, schema: pa.Schema) -> Stream: ...
    def inspect(self, fn) -> Stream: ...
    def map_batches(self, fn, *, schema: pa.Schema) -> Stream: ...
    def filter_batches(self, fn) -> Stream: ...
    def key_by(self, fn=None, *, key: str | list[str] | None = None) -> Stream: ...
    def merge(self, other: Stream) -> Stream: ...
    def name(self, label: str) -> Stream: ...
    def window(self, win) -> WindowedStream: ...
    def operator(self, name: str, op: Operator) -> Stream: ...
    def join(self, other: Stream, spec) -> Stream: ...
    def detect(self, spec) -> Stream: ...
    def sink(self, sink) -> None: ...
```

## Binding architecture

### The type that flows: `PyBuffer` over `ErasedBuffer`

```rust
#[pyclass(name = "Buffer", frozen)]
pub struct PyBuffer { inner: ErasedBuffer }
```

- `__arrow_c_array__` / `from_arrow`: zero-copy via the Arrow C Data Interface
  (PyCapsule protocol). Arrow is `54.3.1` in `Cargo.lock` — FFI available. The batch
  API never round-trips through Python objects.
- `Record` holds `Arc<RecordBatch>` + a row index and decodes columns lazily — it is
  genuinely "sugar over the batch path," not a row copy.
- `schema_id`: `ErasedBuffer::downcast` checks the seahash of the Arrow schema. Every
  Python edge carries a declared `pa.Schema`; we compute the same seahash so
  Python-produced buffers interoperate with Rust operators and pass `downcast`.

### Wrapping Python callables as the closures rhei already uses

- A Python `map` becomes the same boxed `Fn(ErasedBuffer) -> Vec<ErasedBuffer>` that
  `Stream::map` produces, calling Python inside a `Python::with_gil` block, exporting
  the batch zero-copy to pyarrow and importing the result.
- A custom stateful Python operator becomes a `Box<dyn ErasedBatchOperator>` — it
  slots into the executor with **zero executor changes**. `clone_erased` is a `Py`
  refcount bump; one Python operator *instance* per worker, constructed lazily on the
  worker thread, so no cross-thread Python object sharing.

### GIL strategy

Each Timely worker is single-threaded and calls `rt.block_on(op.process(...))` on its
own thread. Therefore:

- The GIL is held **only inside** `Python::with_gil` blocks — only while a Python UDF
  actually runs.
- All pure-Rust work (Arrow IPC ser/de in Exchange, `key_by` hashing/partitioning,
  mask application, state backend I/O, Kafka I/O, Timely scheduling) runs **outside**
  the GIL.
- With `workers=N`, the N worker threads contend for the single GIL only when in a
  Python UDF. **Honest limitation:** Python UDF execution does not parallelize across
  workers under CPython's GIL. Everything else does — including the vectorized
  `agg`/`map_batches` path (one GIL acquisition per batch, amortized over thousands
  of rows; GIL released between batches).
- **Free-threaded seam:** never store Python objects assuming the GIL serializes
  access; no global mutable Python state in closures; one operator instance per
  worker. On a 3.13t (`Py_GIL_DISABLED`) build, the N workers run Python in parallel
  with no API change. Nothing is gated on it now.

### The `&mut OperatorContext` lifetime problem

`StreamFunction::process` gets `ctx: &mut OperatorContext`, valid only for the call;
Python objects outlive Rust borrows. Resolution (staying `unsafe`-free): refactor
`OperatorContext` so state is reachable via a cloneable handle (`Arc`-based) that
`PyContext` can hold, rather than leaking a raw `&mut`. A `live` flag invalidates the
`PyContext` after `process` returns, so Python code that stashes the `Context` and
touches it later gets a clean `RuntimeError`, not UB.

### Python sources/sinks

- A Python `Source` subclass implements `next_batch(self) -> Buffer | None`, wrapped
  as an `ErasedSource` and driven by the existing `bridge.rs` source bridge in a
  tokio task. v1 Python sources are single-partition.
- Built-in connectors (`VecSource`, `PrintSink`, `KafkaSource`, `KafkaSink`) are
  **not** reimplemented — exposed as `#[pyclass]` configs constructing the real Rust
  connectors with the user's `pa.Schema`. For Kafka, the fixed `KafkaMessage` schema
  is decoded into the declared schema via a small decoder transform appended after
  the source.

### Blocking `run()` owning tokio

```rust
fn run(&self, py: Python<'_>, ...) -> PyResult<()> {
    let graph = self.build_graph()?;            // lower py nodes -> DataflowGraph (validate)
    let controller = PipelineController::builder().workers(workers)
        .checkpoint_dir(checkpoint_dir).build()?;
    py.allow_threads(|| {                        // release GIL for the whole run
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        rt.block_on(async {
            tokio::select! {
                r = controller.run(graph) => r,
                _ = ctrl_c_with_python_signal_check() => Ok(()),
            }
        })
    })
}
```

Ctrl-C: a small tokio task periodically re-acquires the GIL to call
`py.check_signals()` and trips rhei's `ShutdownHandle` (`run_with_shutdown`) so the
pipeline drains + checkpoints cleanly — graceful shutdown, not a hard kill.

### Error propagation

- **Build-time** (schema mismatch on an edge, dangling stream): `run()` calls
  `graph.validate()` and translates `ValidationError` into Python `rhei.GraphError`.
  We also add Python-level edge schema checks at `.map(schema=...)` / `.operator(...)`
  time so the error fires at the offending line with a traceback.
- **Runtime UDF exception:** the `with_gil` block captures the `PyErr`, formats the
  Python traceback into the `anyhow::Error` returned from `process`. The executor
  already routes operator errors to the DLQ with `operator_name` + `error` and
  increments `dlq_items_total` — so a Python exception becomes an observable
  dead-letter record with the full traceback. A configurable `on_error` policy
  (`raise` / `skip` / `dlq`) on `Dataflow` controls behavior; stateless transforms
  route to DLQ too rather than silently dropping.

## Crate / package structure

New workspace member **`rhei-python`** (`crate-type = ["cdylib", "rlib"]`):

```
rhei-python/
├── Cargo.toml          # pyo3 (abi3-py39, extension-module); arrow (ffi);
│                       # depends on rhei-runtime, rhei-core
├── pyproject.toml      # maturin; project name "rhei"; optional-deps: kafka
├── src/
│   ├── lib.rs          # #[pymodule] rhei — registers all pyclasses
│   ├── buffer.rs       # PyBuffer + Arrow C Data Interface import/export
│   ├── record.rs       # PyRecord lazy row view + PyRecordIter
│   ├── dataflow.rs     # PyDataflow, PyStream, PyWindowedStream + lowering
│   ├── operator.rs     # PyErasedOperator, PyContext, PyEmit, PyTimers
│   ├── state.rs        # PyValueState/PyMapState/PyListState
│   ├── connectors.rs   # PyVecSource/PyPrintSink/PyKafkaSource/PyKafkaSink
│   ├── windows.rs      # window specs -> built-in operator construction
│   └── agg.rs          # agg specs -> ReduceOp/RollingAggregateOp + kernels
└── python/rhei/        # .py shim layer + .pyi stubs (typing, docstrings, agg sugar)
```

## Phasing (each layer independently shippable and testable)

- **L0 — Erased builder seam (Rust-only, zero PyO3 risk):** promote the `pub`
  erased-builder API on `DataflowGraph`; add `schema_hash_of`; `OperatorContext`
  state-handle refactor. Test: a graph built entirely through the erased API (boxed
  closures, no `Stream<T>`) runs and matches a typed-API equivalent.
- **L1 — Buffer FFI + thinnest pipeline (architecture proof):** `PyBuffer` zero-copy
  `__arrow_c_array__`/`from_arrow`; `PyDataflow`; `VecSource` (Python list → Buffer)
  → `map_batches` (one Python callable) → `PrintSink`; blocking `run(workers=1)`.
  Test: a word-count-style batch pipeline in Python produces identical output to the
  Rust example.
- **L2 — Stateless row API + structure:** `Record` lazy views; `map`/`filter`/
  `flat_map`/`inspect` row forms; `filter_batches` with mask; `key_by`; `merge`;
  multi-worker `run(workers=N)`. Test: key_by exchange routes correctly across
  workers.
- **L3 — Built-in stateful operators from Python:** `window().aggregate(agg.*)` over
  `TumblingWindow`/`SlidingWindow`/`SessionWindow`/`CountWindow`; `TemporalJoin`;
  `SequenceDetect`. Hot loops in Rust; Python supplies config only. Test: windowed
  agg matches the Rust `window_agg` example. Delivers most real-world value with no
  per-row GIL.
- **L4 — Custom Python stateful operators:** `rhei.Operator` protocol; `PyContext` +
  `ValueState`/`MapState`/`ListState`; `Timers`; `on_watermark`/`on_timer`; `Emit`.
  Test: the `VelocityGuard` example with checkpoint/restart — verify state survives a
  restart.
- **L5 — Connectors + hardening:** `KafkaSource`/`KafkaSink` (kafka extra), Python
  `Source`/`Sink` subclasses, DLQ + `on_error` policy, Ctrl-C graceful shutdown,
  `metrics_addr` passthrough.
- **L6 (additive, post-v1) — schema ergonomics:** `@rhei.record` dataclass/pydantic
  decorator that generates the `pa.Schema`. Pure sugar over L1's explicit-schema seam.

## DevEx trade-offs (acknowledged, with mitigations)

- **`schema=` on every type-changing transform.** rhei is schema-first by design
  (that's *why* it's fast), unlike Bytewax's schema-free dicts. Mitigations: L6
  decorator removes verbosity for the common case; document the design rationale; an
  opaque-binary escape hatch (`map_py`) exists for schema-free exploration but is
  explicitly slow and never the recommended path.
- **GIL parallelism.** `workers=N` is not N× for Python-heavy UDFs. Document like
  PySpark ("prefer native expressions over Python UDFs"); steer hard toward
  `map_batches`/`agg` (which *do* scale); free-threaded build is the real long-term
  fix.
- **State `.get()` can block on NVMe/S3** despite looking synchronous. Cache reads
  within a `process` call; document "read state once per batch, not per row"; the
  `agg` window path avoids it entirely.
- **`Emit` vs `yield`.** More ceremonial than Bytewax's direct return/`yield`. Lenient
  runtime (return `None`/`dict`/`list[dict]`, or `yield` from `process`) closes the
  gap while keeping `Emit` as the documented form.
- **Event time declared on the source** (`event_time=`), not the record — easy to
  forget, after which windows silently never fire. Mitigation: `validate()` raises a
  `GraphError` with the exact fix when a `window()` exists downstream of a source with
  no declared event time. Turns a silent-no-output incident into a build error.
- **`key_by` keys are `str`.** Composite keys need `key=["a","b"]` (built into a
  composite string in Rust). Matches Bytewax; documented as intentional (it's what the
  exchange hashes).

### Strategic positioning vs. Bytewax

"Bytewax ergonomics for prototyping, but every pipeline is one `map_batches`/`agg`
rewrite away from Rust-class throughput, with production-grade durable-state +
checkpoint/restart out of the box." rhei wins on zero-copy vectorized batch
processing, tiered durable state, genuine multi-worker exchange, and sharing the same
code path as the high-performance Rust framework underneath.

## Testing strategy

- **L0:** Rust unit/integration test building & running an erased-only graph.
- **L1–L2:** Python integration tests asserting output parity with the Rust examples;
  zero-copy verified by buffer-address/refcount checks.
- **L3:** windowed-aggregation parity with Rust `window_agg`.
- **L4:** checkpoint/restart test — kill mid-stream, restore, assert state continuity.
- **L5:** Kafka e2e (docker-compose), DLQ population on injected Python exceptions,
  graceful Ctrl-C drains and checkpoints.
- Cross-cutting: `validate()` error-message tests (schema mismatch, dangling stream,
  missing event time).

## Open questions for implementation planning

- Exact shape of the `OperatorContext` state-handle refactor (smallest change that
  keeps existing Rust operators untouched).
- Whether the Kafka payload→schema decode is a fixed JSON decoder in v1 or pluggable.
- Minimum supported Python version for `abi3` (proposing 3.9) and whether to ship a
  separate 3.13t free-threaded wheel later.
