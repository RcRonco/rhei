# Rhei API Reference

Reference for the public pipeline-building API. Every Rust example here is compiled by CI as a doctest (`cargo test --doc -p rhei`); blocks that cannot be compiled are marked `ignore` with a comment saying why.

For a tutorial, start with [docs/getting-started.md](docs/getting-started.md). For design rationale, see [ARCHITECTURE.md](ARCHITECTURE.md) and the [ADR/](ADR/) directory.

## Philosophy

Rhei exposes **one** API for building stream processing pipelines. There is no "simple mode" vs "advanced mode" — the same constructs handle single-threaded development, multi-worker production, and multi-process execution.

The API is built on **dataflow variables**. Each operation returns a typed stream handle that can be reused, branched, and merged. The runtime compiles the dataflow graph and determines execution strategy (exchange pacts, worker assignment, checkpointing).

Users do not interact with Timely Dataflow, exchange pacts, capabilities, or worker threads directly.

---

## Core Types

### `Stream<'a, T>`

A `Copy` handle representing a point in the dataflow graph. `T` is the `RheiSchema` element type flowing through that point.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, Stream, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Order {
    customer_id: String,
    amount: f64,
}

fn build(graph: &DataflowGraph) {
    let orders: Stream<'_, Order> = graph.source(VecSource::new(vec![Order {
        customer_id: "c1".into(),
        amount: 10.0,
    }]));

    // `Stream` is `Copy`, so reusing a handle creates fan-out.
    orders.filter_fn(|o| o.amount > 100.0).sink(PrintSink::<Order>::new());
    orders.sink(PrintSink::<Order>::new());
}
```

Both consumers receive every row from `orders`; the runtime duplicates the data via Timely's internal tee.

**There is no `KeyedStream` type.** Keying is expressed by calling `.key_by()`, which returns another `Stream<'a, T>`. Stateful operators are available on any stream — see [Stateful operators](#stateful-operators) for what that means in practice.

### `DataflowGraph`

Container for the topology. Streams borrow from it, so it must outlive them. `graph.validate()` runs before execution and rejects graphs with unreachable or dangling nodes.

### `PipelineController`

Configures workers, checkpointing, DLQ, and cluster settings, then compiles and runs a `DataflowGraph`.

```rust,no_run
use rhei::{DataflowGraph, PipelineController, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Order {
    customer_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = DataflowGraph::new();
    graph
        .source(VecSource::new(vec![Order { customer_id: "c1".into() }]))
        .sink(PrintSink::<Order>::new());

    let controller = PipelineController::builder()
        .checkpoint_dir("./checkpoints")
        .workers(4)
        .build()?;

    controller.run(graph).await?;
    Ok(())
}
```

`build()` returns `anyhow::Result<PipelineController>`. The shorthand `PipelineController::new(path)` takes a `PathBuf` and is paired with `.with_workers(n)`.

---

## Operations

### Sources

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Reading {
    value: f64,
}

fn build(graph: &DataflowGraph) {
    let readings = graph.source(VecSource::new(vec![Reading { value: 1.0 }]));
    readings.sink(PrintSink::<Reading>::new());
}
```

A source produces a `Stream<'_, S::Output>`. Multiple sources are independent and run concurrently. Sources implement the `Source` trait, which also carries watermark emission (`current_watermark`), offset tracking (`current_offsets` / `restore_offsets`), and optional partitioning (`partition_count` / `create_partition_source`).

Built-in sources: `VecSource`, `PartitionedVecSource`, and `KafkaSource` (behind the `kafka` feature).

### Stateless transforms

Closures receive a **zero-copy view** of each row, not an owned value:

| Method | Closure signature | Notes |
|--------|-------------------|-------|
| `map` | `Fn(T::View<'_>) -> O` | Per-row transform into a new schema |
| `flat_map` | `Fn(T::View<'_>) -> Vec<O>` | One-to-many |
| `filter_fn` | `Fn(&T::View<'_>) -> bool` | Builds a selection mask; no data copied |
| `filter` | takes an `Expr` | Evaluated as an Arrow kernel over the batch |
| `inspect` | `Fn(&T::View<'_>)` | Side-effect only, passes rows through |
| `distinct_by` | `Fn(&T::View<'_>) -> String` | Deduplicates by derived key |
| `limit` | `usize` | Caps total rows |
| `batch` | `usize` | Re-batches rows into a target size |
| `name` | `&str` | Labels the node for debugging and the TUI graph |

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource, col, lit_f64};

#[derive(Clone, rhei::RheiSchema)]
struct Reading {
    sensor_id: String,
    value: f64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Label {
    text: String,
}

fn build(graph: &DataflowGraph) {
    let readings = graph.source(VecSource::new(vec![Reading {
        sensor_id: "a".into(),
        value: 1.0,
    }]));

    // Closure predicate.
    let hot = readings.filter_fn(|r| r.value > 20.0);

    // Column expression, evaluated with Arrow compute kernels.
    let also_hot = readings.filter(col("value").gt(lit_f64(20.0)));

    hot.map(|r| Label { text: r.sensor_id.to_string() })
        .name("labels")
        .sink(PrintSink::<Label>::new());
    also_hot.sink(PrintSink::<Reading>::new());
}
```

Expression builders: `col`, `lit_i64`, `lit_u64`, `lit_f64`, `lit_str`, `lit_bool`, combined with `gt`, `gt_eq`, `lt`, `lt_eq`, `eq`, `not_eq`, `and`, `or`, `negate`.

Stateless transforms never move data between workers — they run wherever the data already is.

### Keying (`key_by`)

Declares how data is partitioned across workers. The key function takes a row view and returns a `String`.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Order {
    customer_id: String,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![Order { customer_id: "c1".into() }]))
        .key_by(|o| o.customer_id.to_string())
        .sink(PrintSink::<Order>::new());
}
```

**Every `key_by()` is an exchange point.** Rows are partitioned by `seahash(key) % workers` and routed via a Timely Exchange pact. With one worker the exchange exists but moves nothing between threads.

Re-keying is supported — a second `key_by()` triggers a new exchange.

### Stateful operators

`.operator(name, op)` attaches a `StreamFunction`. The operator must implement `StreamFunction<Input = T> + Clone + Send + 'static`; it is cloned once per worker.

```rust,no_run
use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};
use rhei::{DataflowGraph, KeyedState, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Order {
    customer_id: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Total {
    customer_id: String,
    orders: u64,
}

#[derive(Clone)]
struct CustomerAgg;

#[async_trait::async_trait]
impl StreamFunction for CustomerAgg {
    type Input = Order;
    type Output = Total;

    async fn process(
        &mut self,
        input: RheiBuffer<Order>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Total>> {
        let mut builder = Total::builder(input.len());
        let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "orders");

        for view in &input {
            let customer_id = view.customer_id.to_string();
            let orders = state.get(&customer_id).await?.unwrap_or(0) + 1;
            state.put(&customer_id, &orders)?;
            builder.append(Total { customer_id, orders });
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![Order { customer_id: "c1".into() }]))
        .key_by(|o| o.customer_id.to_string()) // required for correct state
        .operator("agg", CustomerAgg)
        .sink(PrintSink::<Total>::new());
}
```

> **`.operator()` is available on unkeyed streams and Rhei will not stop you.**
> This is *not* a compile-time-enforced invariant. If you attach a stateful
> operator without a preceding `key_by()`, the code compiles and runs, but with
> more than one worker each worker sees an arbitrary subset of keys and the
> state is wrong. Always key first.

The runtime automatically:

1. Creates a per-worker `StateContext` (isolated L1/L2 with a shared L3)
2. Clones the operator for each worker
3. Routes data by key so each worker owns a disjoint set of key groups

You never construct `StateContext` yourself — it arrives on `OperatorContext::state`.

Beyond `process`, `StreamFunction` has defaulted hooks: `open`, `close`, `on_watermark` (used by window operators to close windows), `on_timer`, and `on_error`.

### Built-in operators

Window and join operators are constructed with `::new(...)` taking positional closures. **They do not have builders.**

| Operator | Constructor shape |
|----------|-------------------|
| `TumblingWindow` | `new(size, key_fn, time_fn, accumulate_fn, output_fn)` |
| `SlidingWindow` | `new(size, slide, key_fn, time_fn, accumulate_fn, output_fn)` |
| `SessionWindow` | `new(gap, key_fn, time_fn, accumulate_fn, output_fn)` |
| `CountWindow` | count-triggered, same closure shape |
| `TemporalJoin` | key/join closures over a merged `Side<L, R>` stream |
| `SequenceDetect` | ordered pattern matching with `AfterMatch` semantics |
| `ReduceOp`, `RollingAggregateOp` | incremental aggregation |

See [`rhei/examples/batch_window_agg.rs`](rhei/examples/batch_window_agg.rs) and [`rhei-runtime/examples/temporal_join.rs`](rhei-runtime/examples/temporal_join.rs) for complete, compiled usages, and each operator's source in `rhei-core/src/operators/` for exact signatures.

Windows support a configurable `allowed_lateness`; events later than that are dropped and counted in `late_events_dropped_total`. Routing late events to a side output is **not implemented**.

### Fan-in (`merge`)

Combines two streams of the same type.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    id: i64,
}

fn build(graph: &DataflowGraph) {
    let a = graph.source(VecSource::new(vec![Event { id: 1 }]));
    let b = graph.source(VecSource::new(vec![Event { id: 2 }]));
    a.merge(b).sink(PrintSink::<Event>::new());
}
```

Merging discards any partitioning the inputs had, so `key_by()` again before a stateful operator.

### Fan-out

Implicit via handle reuse, because `Stream` is `Copy` — see the [`Stream`](#streama-t) example above.

### Sinks

Terminal nodes. A stream can feed multiple sinks. Sinks implement the `Sink` trait (`write_batch` plus an optional `flush`).

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    id: i64,
}

fn build(graph: &DataflowGraph) {
    let events = graph.source(VecSource::new(vec![Event { id: 1 }]));
    events.sink(PrintSink::<Event>::new().with_prefix("out"));
}
```

Built-in sinks: `PrintSink`, and `KafkaSink` (behind the `kafka` feature).

---

## Execution

### Running

`run()` validates the graph, compiles it into a Timely dataflow, and executes it. It returns when all sources are exhausted or shutdown is triggered.

```rust,ignore
// not-compiled: needs a ShutdownHandle wired to a real signal source.
controller.run_with_shutdown(graph, shutdown_handle).await?;
```

On shutdown: finish in-flight batches, checkpoint operators, commit source offsets, flush sinks, return.

`run_dynamic()` additionally supports rescaling the topology at runtime by rebuilding the `TaskManager` between topology generations.

### Delivery semantics

Rhei provides **at-least-once** delivery. Source offsets are committed after a checkpoint completes, so a crash between processing and checkpointing replays the affected records. Exactly-once would require a transactional producer sink and two-phase commit; both are on the roadmap and unimplemented.

---

## How Timely Mechanics Are Abstracted

The runtime translates the dataflow graph into a Timely execution plan:

### Exchange pact insertion

| Operation | Pact | Data movement |
|-----------|------|---------------|
| `map`, `filter`, `filter_fn`, `flat_map`, `inspect` | Pipeline | None — data stays on the current worker |
| `key_by` | Exchange | Redistributed by `seahash(key) % workers` |
| `operator` | Pipeline | None — operates on whatever the worker holds |
| `merge` | Pipeline | Both inputs feed the same downstream workers |
| `sink` | Pipeline | Each worker writes its own partition |

`key_by()` is the only operation that moves data between workers.

Note the asymmetry with the previous table row for `operator`: because there is no `KeyedStream` type, the runtime cannot verify that a stateful operator is preceded by an exchange. It compiles the graph you describe.

### Worker assignment

- **Sources**: by default a source runs on worker 0. Sources implementing `partition_count()` and `create_partition_source()` — such as `KafkaSource` and `PartitionedVecSource` — are consumed in parallel, one reader per worker with partitions assigned round-robin.
- **Stateless ops before the first `key_by`**: run on the worker holding the data.
- **After `key_by`**: all workers process their key partition.
- **Sinks**: each worker writes independently.

### State addressing

State keys are namespaced by **key group**, not by worker index:

```text
kg{key_group}/{operator_name}/{user_key}
```

Key groups decouple key ownership from worker count (the same scheme Flink uses), so the number of workers can change between runs without rewriting state. The number of key groups is fixed by `max_parallelism` at pipeline creation; restoring a checkpoint with a different `max_parallelism` is rejected.

Each worker has its own L1 memtable and L2 cache. L3 (SlateDB) is shared, with key-prefix isolation. Checkpoints are coordinated so all workers checkpoint at the same epoch boundary.

### Single worker

With `workers == 1` the pipeline still runs as a Timely dataflow — there is one worker thread rather than a special non-Timely code path. `key_by()` is present in the graph but moves no data between threads.

---

## Complete Example

Two Kafka topics, per-leg transforms, temporal join, custom stateful enrichment, Kafka sink.

<!-- not-compiled: requires the `kafka` feature and librdkafka. The compiled
     equivalents are rhei-runtime/examples/kafka_transform.rs (Kafka wiring)
     and rhei-runtime/examples/temporal_join.rs (join wiring). -->

```rust,ignore
let graph = DataflowGraph::new();

let raw_orders = graph.source(KafkaSource::new("localhost:9092", "rhei-app", &["orders"])?);
let raw_shipments = graph.source(KafkaSource::new("localhost:9092", "rhei-app", &["shipments"])?);

let orders = raw_orders.map(|msg| parse_order(msg));
let shipments = raw_shipments
    .map(|msg| parse_shipment(msg))
    .filter_fn(|s| s.status != "CANCELLED");

let joined = orders
    .merge(shipments)
    .key_by(|side| join_key(side))
    .operator("temporal_join", temporal_join);

let enriched = joined
    .key_by(|r| r.customer_id.to_string()) // re-key: new exchange
    .operator("customer_enrichment", CustomerEnrichment);

enriched
    .map(|r| to_kafka_record(r))
    .sink(KafkaSink::new("localhost:9092", "enriched-orders")?);

let controller = PipelineController::builder()
    .checkpoint_dir("./checkpoints")
    .workers(8)
    .build()?;
controller.run(graph).await?;
```

Compiled execution plan with 8 workers:

```text
Worker 0: KafkaSource("orders")    → parse ─────────┐
                                                     ├─ Exchange(join_key) → TemporalJoin
Worker 0: KafkaSource("shipments") → parse → filter ─┘         │
                                                     Exchange(customer_id)
                                                               │
Workers 0-7:                                  CustomerEnrichment → map → KafkaSink
```

(With a partitioned Kafka source, the source stages spread across workers rather than sitting on worker 0.)

---

## Alternatives Considered

### Option A: Graph-based API

Build a `StreamGraph` by adding named nodes and connecting them:

```rust,ignore
// not-compiled: a rejected design, shown for contrast. This API does not exist.
let mut graph = StreamGraph::new();
let orders = graph.source("orders", kafka_orders);
let parsed = graph.map("parse", orders, |msg| parse(msg));
graph.sink("output", parsed, kafka_sink);
executor.run_graph(graph).await?;
```

**Why not:** node handles are opaque identifiers, not typed. Connecting a `Stream<Order>` to an operator expecting `Stream<Shipment>` becomes a runtime error rather than a compile error, and string-based naming makes refactoring fragile.

### Option C: Fluent chain with combinators

```rust,ignore
// not-compiled: a rejected design, shown for contrast. This API does not exist.
executor
    .pipeline_2(source_a, source_b)
    .map_left(parse_order)
    .map_right(parse_shipment)
    .merge()
    .key_by(key_fn)
    .operator("join", join_op)
    .sink(sink)
    .await?;
```

**Why not:** it does not scale past trivial topologies. Every new shape (3 sources, fan-out to 2 sinks, diamond joins) needs new combinator methods, and DAGs with shared intermediate results cannot be written as a single chain.

### What was actually built

The current API keeps typed handles (Option A's weakness) and arbitrary DAGs (Option C's weakness) — but it drops the `KeyedStream` type that earlier drafts of this document described. That type would have made "stateful operators require keying" a compile-time guarantee. It is not implemented, so the guarantee is a convention today. See [ROADMAP.md](ROADMAP.md).
