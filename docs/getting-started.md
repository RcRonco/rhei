# Getting Started with Rhei

This guide walks you through building your first streaming pipeline with Rhei, from installation to a running stateful application.

## Prerequisites

- Rust 1.85+ (edition 2024)
- `cargo` (comes with Rust)

## Create a New Project

```bash
rhei new my-pipeline
cd my-pipeline
```

This creates a project with a working `Cargo.toml`, `src/main.rs`, and `pipeline.toml`. You can run it immediately:

```bash
cargo run
```

If you don't have the `rhei` CLI installed, create a project manually:

```bash
cargo init my-pipeline
cd my-pipeline
cargo add rhei-core rhei-runtime anyhow async-trait serde tokio --features serde/derive,tokio/full
```

## Your First Pipeline

Replace `src/main.rs` with:

```rust
use rhei_core::connectors::print_sink::PrintSink;
use rhei_core::connectors::vec_source::VecSource;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::Executor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let events = vec![
        "sensor-1:23.5".to_string(),
        "sensor-2:18.0".to_string(),
        "sensor-1:24.1".to_string(),
    ];

    let graph = DataflowGraph::new();
    graph
        .source(VecSource::new(events))
        .map(|line: String| line.to_uppercase())
        .sink(PrintSink::<String>::new());

    let executor = Executor::builder()
        .checkpoint_dir("./checkpoints")
        .build()?;
    executor.run(graph).await?;
    Ok(())
}
```

Run it:

```bash
cargo run
```

Output:

```
SENSOR-1:23.5
SENSOR-2:18.0
SENSOR-1:24.1
```

For full control over workers, checkpointing, and clustering, see [Manual Executor Setup](#manual-executor-setup) below.

## Core Concepts

### Streams and Types

Every operation returns a typed stream handle. The Rust compiler ensures type safety across your entire pipeline:

```rust
let raw: Stream<String> = graph.source(VecSource::new(data));
let parsed: Stream<SensorReading> = raw.map(parse);  // type checked at compile time
```

### Stream vs KeyedStream

Rhei has two stream types:

| Type | Created by | Supports |
|------|-----------|----------|
| `Stream<T>` | `graph.source()`, `.map()`, `.filter()`, `.merge()` | Stateless transforms, keying, sinking |
| `KeyedStream<T>` | `.key_by()` | Everything Stream supports + stateful operators |

**Stateful operators require a `KeyedStream`.** This is enforced at compile time -- you cannot call `.operator()` on an unkeyed `Stream`.

### Exchange Points

When running with multiple workers, `key_by()` is the only operation that moves data between workers. All elements with the same key land on the same worker:

```rust
graph
    .source(source)
    .map(parse)              // runs on source worker
    .key_by(|r| r.sensor_id.clone())  // redistributes by sensor_id
    .operator("agg", MyAgg)  // runs on worker that owns this key
    .sink(output);
```

## Adding State

Stateful operators implement the `StreamFunction` trait. Use `KeyedState<K, V>` for typed key-value state:

```rust
use async_trait::async_trait;
use rhei_core::operators::keyed_state::KeyedState;
use rhei_core::state::context::StateContext;
use rhei_core::traits::StreamFunction;

struct CountEvents;

#[async_trait]
impl StreamFunction for CountEvents {
    type Input = SensorReading;
    type Output = String;

    async fn process(
        &mut self,
        input: SensorReading,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<String>> {
        let mut state = KeyedState::<String, u64>::new(ctx, "counts");
        let count = state.get(&input.sensor_id).await?.unwrap_or(0) + 1;
        state.put(&input.sensor_id, &count)?;
        Ok(vec![format!("{}: {} events", input.sensor_id, count)])
    }
}
```

Wire it into your pipeline:

```rust
graph
    .source(source)
    .key_by(|r: &SensorReading| r.sensor_id.clone())
    .operator("counter", CountEvents)
    .sink(PrintSink::new());
```

State is automatically:
- Isolated per worker (each worker owns a disjoint key partition)
- Tiered across RAM, local NVMe cache, and object storage (S3/GCS/Azure)
- Checkpointed when the dataflow frontier advances

You never create `StateContext` manually -- the executor provides it.

## Built-in Operators

Rhei includes common streaming operators so you don't have to write them:

### Tumbling Windows

Fixed-size, non-overlapping time windows:

```rust
use rhei_core::operators::{Avg, TumblingWindow};

let windowed = keyed_stream.operator(
    "avg_temp",
    TumblingWindow::builder()
        .window_size(60)  // 60-second windows
        .key_fn(|r: &Reading| r.sensor_id.clone())
        .time_fn(|r: &Reading| r.timestamp)
        .aggregator(Avg::new(|r: &Reading| r.value))
        .build(),
);
```

### Sliding Windows

Overlapping windows with configurable slide:

```rust
use rhei_core::operators::SlidingWindow;

SlidingWindow::builder()
    .window_size(300)  // 5-minute window
    .slide_size(60)    // slide every minute
    .key_fn(key_fn)
    .time_fn(time_fn)
    .aggregator(aggregator)
    .build()
```

### Session Windows

Gap-based windows that close after inactivity:

```rust
use rhei_core::operators::SessionWindow;

SessionWindow::builder()
    .gap(120)  // 2-minute inactivity gap
    .key_fn(key_fn)
    .time_fn(time_fn)
    .aggregator(aggregator)
    .build()
```

### Temporal Joins

Join two streams by key with a configurable timeout:

```rust
use rhei_core::operators::TemporalJoin;

// Merge left and right into a single stream first
let combined = orders.merge(shipments);

let joined = combined
    .key_by(|side| extract_join_key(side))
    .operator(
        "order_shipment_join",
        TemporalJoin::builder()
            .key_fn(|side| extract_join_key(side))
            .join_fn(|order, shipment| JoinedRecord::new(order, shipment))
            .build(),
    );
```

## Manual Executor Setup

For full control over workers, checkpointing, and clustering:

```rust
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = DataflowGraph::new();

    // ... build your pipeline on graph ...

    let executor = Executor::builder()
        .checkpoint_dir("./checkpoints")
        .workers(4)
        .build()?;

    executor.run(graph).await?;
    Ok(())
}
```

### Configuration

Use `pipeline.toml` for persistent configuration:

```toml
[pipeline]
name = "my-pipeline"
workers = 4
checkpoint_dir = "./checkpoints"

[metrics]
addr = "0.0.0.0:9090"
log_level = "info"
```

Run with: `rhei run --config pipeline.toml`

Environment variables override config file values (e.g., `RHEI_WORKERS=8`).

## Error Handling

### Dead Letter Queues

Route operator errors to a DLQ instead of crashing the pipeline:

```rust
// Simple: errors go to a sink
keyed_stream
    .operator("process", MyProcessor)
    .dlq(PrintSink::<String>::new())
    .sink(output);

// Advanced: transform errors before sinking
keyed_stream
    .operator("process", MyProcessor)
    .with_dlq(|errors| {
        errors
            .map(|e| format!("ALERT: {e}"))
            .sink(alert_sink)
    })
    .sink(output);
```

### Error Classification

Use `PipelineError` to classify errors for structured DLQ routing:

```rust
use rhei_core::error::{PipelineError, ErrorClassification};

// In your operator:
if input.value < 0.0 {
    return Err(PipelineError::bad_data("negative sensor value").into());
}
```

Classifications: `Retriable` (transient failures), `BadData` (malformed input), `SystemError` (infrastructure failures).

## Testing

Rhei provides in-memory test utilities so you can test pipelines without external infrastructure:

```rust
use rhei_core::testing::{TestSource, CollectSink};

#[tokio::test]
async fn test_my_pipeline() {
    let source = TestSource::from(vec![1, 2, 3, 4, 5]);
    let sink = CollectSink::<i32>::new();

    // Build and run your graph...

    sink.assert_eq(vec![2, 4]);           // order-sensitive
    sink.assert_eq_unordered(vec![4, 2]); // order-insensitive
    sink.assert_len(2);
    sink.assert_contains(&4);
    sink.assert_all(|x| x % 2 == 0);
    sink.assert_empty();  // or assert nothing was collected
}
```

## Multi-Worker and Clustering

### Multi-Thread (Single Process)

```rust
Executor::builder()
    .workers(4)  // 4 worker threads
    .build()?;
```

Or: `RHEI_WORKERS=4 cargo run`

### Multi-Process (Cluster)

```bash
# Process 0 (coordinator)
RHEI_PROCESS_ID=0 RHEI_PEERS=host1:2101,host2:2101 cargo run

# Process 1
RHEI_PROCESS_ID=1 RHEI_PEERS=host1:2101,host2:2101 cargo run
```

State lives in object storage (SlateDB on S3/GCS/Azure). Scaling out means adding processes, not migrating data.

## Observability

### TUI Dashboard

```bash
rhei run --tui --workers 4
```

Displays a live pipeline graph, throughput metrics, state cache hit rates, and per-worker logs.

### Metrics Server

```bash
rhei run --metrics-addr 0.0.0.0:9090
```

Endpoints:
- `GET /healthz` -- liveness probe
- `GET /readyz` -- readiness probe (returns 503 until pipeline is running)
- `GET /metrics` -- Prometheus metrics
- `GET /api/metrics` -- JSON metrics snapshot

### Structured Logging

```bash
rhei run --log-level debug --json-logs
```

## Troubleshooting

### "type mismatch" on `.operator()`

**Symptom:** Compiler error about `StreamFunction::Input` not matching stream type.

**Cause:** The operator's `Input` type doesn't match the element type in the stream.

**Fix:** Ensure the stream type matches. Use `.map()` to convert before `.operator()`:

```rust
// Wrong: stream has String but operator expects SensorReading
stream.key_by(|s| s.clone()).operator("op", MyOp);

// Right: parse first
stream.map(parse_reading).key_by(|r| r.id.clone()).operator("op", MyOp);
```

### "operator() is not available on Stream"

**Symptom:** No method named `operator` found for `Stream<T>`.

**Cause:** Stateful operators require a `KeyedStream`. You need `.key_by()` first.

**Fix:**

```rust
// Wrong:
stream.operator("process", MyOp);

// Right:
stream.key_by(|x| x.key.clone()).operator("process", MyOp);
```

### State not persisting across restarts

**Symptom:** State appears empty after restarting the pipeline.

**Cause:** The checkpoint directory was cleaned up, or the pipeline didn't complete a checkpoint before shutdown.

**Fix:**
1. Use a persistent `checkpoint_dir` (not `/tmp`)
2. Ensure graceful shutdown with `run_with_shutdown()` so checkpoints complete
3. Check logs for checkpoint completion messages

### Pipeline hangs with no output

**Symptom:** Pipeline starts but produces no output.

**Possible causes:**
- Source is empty or waiting for data (e.g., Kafka topic has no messages)
- A filter is dropping all elements
- Backpressure from a slow sink

**Debug steps:**
1. Run with `--log-level debug` to see per-batch progress
2. Check the TUI dashboard (`--tui`) for element counts at each stage
3. Temporarily replace your sink with `PrintSink` to verify data flow

### "No Cargo.toml found" on `rhei run`

**Cause:** You're not in a Rhei project directory.

**Fix:** `cd` into your project directory, or use `cargo run` directly.

## Next Steps

- Browse the [API reference](../API.md) for the full `Stream` and `KeyedStream` API
- Read [ARCHITECTURE.md](../ARCHITECTURE.md) for internals
- Check [examples](../rhei-runtime/examples/) for complete working pipelines
- See [ROADMAP.md](../ROADMAP.md) for planned features
