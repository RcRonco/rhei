# Rill API Design

## Philosophy

Rill exposes **one** API for building stream processing pipelines. There is no "simple mode" vs "advanced mode" — the same constructs handle single-threaded development, multi-worker production, and eventually distributed execution.

The API is built on **dataflow variables**. Each operation returns a typed stream handle that can be reused, branched, and merged. The executor compiles the dataflow graph and determines execution strategy (exchange pacts, worker assignment, checkpointing) automatically.

Users never interact with Timely Dataflow, exchange pacts, capabilities, or worker threads directly. These are internal execution details.

---

## Core Types

### `Stream<'a, T>`

A lightweight, copyable handle representing a point in the dataflow graph. `T` is the element type flowing through that point.

```rust
let graph = DataflowGraph::new();
let orders: Stream<Order> = graph.source(kafka_orders);
let parsed: Stream<ParsedOrder> = orders.map(parse_order);
```

`Stream` is `Copy` — using it multiple times creates fan-out:

```rust
let alerts = parsed.filter(|o| o.amount > 10_000.0);
let logged = parsed.map(|o| format!("{o}"));
// Both `alerts` and `logged` consume from `parsed` — the executor handles the split.
```

### `KeyedStream<'a, T>`

A stream that has been partitioned by a key. Only `KeyedStream` supports stateful operators. Returned by `.key_by()`.

```rust
let keyed: KeyedStream<Order> = orders.key_by(|o| o.customer_id.clone());
let enriched: KeyedStream<EnrichedOrder> = keyed.operator("enrich", EnrichOp);
```

`KeyedStream` also supports stateless transforms (`.map()`, `.filter()`, `.flat_map()`), which preserve the partitioning — data stays on the same worker.

### `Executor`

Configures workers and checkpointing. Compiles and runs a `DataflowGraph`.

```rust
let executor = Executor::builder()
    .checkpoint_dir("./checkpoints")
    .workers(4)
    .build();

// ... build dataflow on a DataflowGraph ...

executor.run(graph).await?;
```

---

## Operations

### Sources

```rust
let orders = graph.source(KafkaSource::new(broker, group, &["orders"])?);
let readings = graph.source(VecSource::new(data));
```

A source produces a `Stream<S::Output>`. Multiple sources are independent — they run concurrently.

### Stateless Transforms

Available on both `Stream<T>` and `KeyedStream<T>`.

```rust
// Transform each element
let parsed = raw.map(|msg: KafkaMessage| parse(msg));

// Drop elements
let valid = parsed.filter(|r: &Record| r.is_valid());

// One-to-many
let words = lines.flat_map(|line: String| {
    line.split_whitespace().map(String::from).collect()
});
```

On `Stream`: data stays wherever it is (no exchange).
On `KeyedStream`: data stays on the same worker — **partitioning is preserved**.

### Keying (`key_by`)

Declares how data should be partitioned across workers. Returns a `KeyedStream`.

```rust
let keyed = orders.key_by(|o: &Order| o.customer_id.clone());
```

The key function returns a `String`. The executor hashes it to determine worker assignment. All elements with the same key are guaranteed to land on the same worker.

**Every `key_by()` is an exchange point.** When workers > 1, the executor redistributes data across workers at this point. When workers == 1, it's a no-op.

Re-keying is supported — a second `key_by()` triggers a new exchange:

```rust
let by_customer = orders.key_by(|o| o.customer_id.clone());
let aggregated = by_customer.operator("agg", CustomerAgg);
let by_region = aggregated.key_by(|a| a.region.clone());  // re-partition
let regional = by_region.operator("regional", RegionalAgg);
```

### Stateful Operators

**Only available on `KeyedStream`.** This is enforced at compile time — calling `.operator()` on an unkeyed `Stream` is a type error.

```rust
let keyed = readings.key_by(|r| r.sensor_id.clone());
let windowed = keyed.operator("tumbling_window", TumblingWindow::builder()
    .window_size(10)
    .key_fn(|r: &Reading| r.sensor_id.clone())
    .time_fn(|r: &Reading| r.timestamp)
    .aggregator(Avg::new(|r: &Reading| r.value))
    .build(),
);
```

The executor automatically:
1. Creates per-worker `StateContext` instances (isolated L1/L2/L3 state)
2. Clones the operator for each worker
3. Routes data by key so each worker owns a disjoint key partition

Users never create `StateContext` manually.

### Fan-In (`merge`)

Combines two streams of the same type into one. The result is an unkeyed `Stream`.

```rust
let left = orders.map(|o| JoinSide::Left(o));
let right = shipments.map(|s| JoinSide::Right(s));
let combined: Stream<JoinSide<Order, Shipment>> = left.merge(right);
```

If a stateful operator follows a merge, you must `key_by()` first.

### Fan-Out

Implicit via variable reuse:

```rust
let parsed = raw.map(parse);

// Fan-out: two independent consumers
let alerts = parsed.filter(|r| r.is_critical()).sink(alert_sink);
let archive = parsed.sink(archive_sink);
```

The executor duplicates data to both consumers.

### Sinks

Terminal nodes in the dataflow. A stream can feed multiple sinks.

```rust
enriched.sink(KafkaSink::new(broker, "output-topic")?);
enriched.sink(PrintSink::new());
```

---

## Execution

### Building and Running

```rust
let graph = DataflowGraph::new();
let orders = graph.source(kafka_source);
let processed = orders
    .map(parse)
    .key_by(|o| o.customer_id.clone())
    .operator("enrich", EnrichOp)
    .map(format_output);
processed.sink(kafka_sink);

let executor = Executor::builder()
    .checkpoint_dir("./checkpoints")
    .workers(4)
    .build();

executor.run(graph).await?;
```

`.run()` compiles the dataflow graph, validates it (every stream must reach a sink or be explicitly dropped), and executes it. The method returns when all sources are exhausted or shutdown is triggered.

### Shutdown

```rust
executor.run_with_shutdown(graph, shutdown_handle).await?;
```

On shutdown signal: finish current batches, checkpoint all operators, commit source offsets, flush sinks, return.

---

## How Timely Mechanics Are Abstracted

Users never see Timely. The executor translates the dataflow graph into an execution plan:

### Exchange Pact Insertion

The rule is simple and deterministic:

| Operation | Pact | Data Movement |
|-----------|------|---------------|
| `map`, `filter`, `flat_map` | Pipeline | None — data stays on current worker |
| `key_by` | Exchange | Data redistributed by `hash(key) % workers` |
| `operator` (on `KeyedStream`) | Pipeline | None — data already partitioned by preceding `key_by` |
| `merge` | Pipeline | Both inputs feed into same downstream workers |
| `sink` | Pipeline | Each worker writes its own partition |

**Stateless operations never move data.** They run wherever the data already is.

**`key_by()` is the only operation that moves data between workers.** It inserts an Exchange pact. If the data is already partitioned by the same key (e.g., two `key_by` calls with the same function), the executor can elide the exchange.

**Stateful operators require keyed streams.** This is a compile-time guarantee. Because `operator()` is only on `KeyedStream`, every stateful operator is guaranteed to have an exchange before it (the one from `key_by()`). The executor doesn't need to guess — the structure is explicit.

### Worker Assignment

- **Sources**: Each source runs on one designated worker (worker 0 by default). Source parallelism (e.g., Kafka partition assignment) is a future extension.
- **Stateless ops before first `key_by`**: Run on the source worker only.
- **After `key_by`**: All workers process their key partition.
- **Sinks**: Each worker writes independently. The sink implementation handles merging if needed (e.g., Kafka sink — each worker produces to the same topic).

### State Management

Per-worker state is created automatically:
- State path: `w{worker_index}/{operator_name}/{user_key}`
- Each worker has isolated L1 memtable and L2 cache
- L3 (SlateDB) is shared via `Arc` with key-prefix isolation
- Checkpointing is coordinated: all workers checkpoint at the same epoch boundary

### Single Worker (workers = 1)

When `workers == 1`, the entire pipeline runs as a simple async loop. No threads, no channels, no exchange overhead. `key_by()` is semantically present (for correctness) but triggers no data movement. This is the development/testing default.

---

## Complete Example

Two Kafka topics, per-leg transforms, temporal join on composite key, custom stateful enrichment, Kafka sink:

```rust
let graph = DataflowGraph::new();

// Sources
let raw_orders = graph.source(
    KafkaSource::new("localhost:9092", "rhei-app", &["orders"])?
);
let raw_shipments = graph.source(
    KafkaSource::new("localhost:9092", "rhei-app", &["shipments"])?
);

// Parse each leg
let orders = raw_orders.map(|msg: KafkaMessage| {
    let o: Order = serde_json::from_slice(&msg.payload.unwrap()).unwrap();
    JoinSide::Left(o)
});

let shipments = raw_shipments
    .map(|msg: KafkaMessage| {
        let s: Shipment = serde_json::from_slice(&msg.payload.unwrap()).unwrap();
        JoinSide::Right(s)
    })
    .filter(|side| match side {
        JoinSide::Right(s) => s.status != "CANCELLED",
        _ => true,
    });

// Merge and join on composite key
let joined = orders
    .merge(shipments)
    .key_by(|side: &JoinSide<Order, Shipment>| match side {
        JoinSide::Left(o) => format!("{}:{}:{}", o.customer_id, o.region, o.date),
        JoinSide::Right(s) => format!("{}:{}:{}", s.customer_id, s.region, s.date),
    })
    .operator("temporal_join", TemporalJoin::builder()
        .key_fn(|side: &JoinSide<Order, Shipment>| match side {
            JoinSide::Left(o) => format!("{}:{}:{}", o.customer_id, o.region, o.date),
            JoinSide::Right(s) => format!("{}:{}:{}", s.customer_id, s.region, s.date),
        })
        .join_fn(|order, shipment| JoinedRecord::new(order, shipment))
        .build(),
    );

// Custom stateful enrichment (re-key by customer)
let enriched = joined
    .key_by(|r: &JoinedRecord| r.customer_id.clone())
    .operator("customer_enrichment", CustomerEnrichment);

// Output
let output = enriched.map(|r: EnrichedRecord| {
    KafkaRecord::new(serde_json::to_vec(&r).unwrap())
});
output.sink(KafkaSink::new("localhost:9092", "enriched-orders")?);

// Run
let executor = Executor::builder()
    .checkpoint_dir("./checkpoints")
    .workers(8)
    .build();

executor.run(graph).await?;
```

Execution plan compiled by the executor (8 workers):

```
Worker 0: KafkaSource("orders") → parse → ─┐
                                             ├─ Exchange(composite_key) → TemporalJoin
Worker 0: KafkaSource("shipments") → parse → filter ─┘         │
                                                      Exchange(customer_id)
                                                                │
Workers 0-7:                                    CustomerEnrichment → format → KafkaSink
```

---

## Alternatives Considered

### Option A: Graph-Based API

Build a `StreamGraph` object by adding named nodes and connecting them:

```rust
let mut graph = StreamGraph::new();
let orders = graph.source("orders", kafka_orders);
let parsed = graph.map("parse", orders, |msg| parse(msg));
let joined = graph.keyed_operator("join", merged, key_fn, join_op);
graph.sink("output", joined, kafka_sink);
executor.run_graph(graph).await?;
```

**Why we didn't choose this:** Node handles are opaque identifiers, not typed. Connecting a `Stream<Order>` to an operator expecting `Stream<Shipment>` is a runtime error, not a compile-time error. In Rust, losing compile-time type safety is a significant cost. The string-based naming also makes refactoring fragile.

### Option C: Fluent Chain with Combinators

Single expression pipeline with specialized methods for multi-input topologies:

```rust
executor
    .pipeline_2(source_a, source_b)
    .map_left(parse_order)
    .map_right(parse_shipment)
    .filter_right(|s| s.status != "CANCELLED")
    .merge()
    .key_by(key_fn)
    .operator("join", join_op)
    .sink(sink)
    .await?;
```

**Why we didn't choose this:** Doesn't scale beyond trivial topologies. Every new pattern (3 sources, fan-out to 2 sinks, diamond joins) requires new combinator methods (`pipeline_3`, `map_middle`, `split`, etc.). The API surface becomes unpredictable and users can't tell what's available without reading docs. DAGs with shared intermediate results are impossible to express in a single chain.
