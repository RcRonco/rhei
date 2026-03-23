# Rhei

*From "Panta Rhei" (πάντα ῥεῖ) — everything flows.*

A stateful stream processing engine built on Rust, Timely Dataflow, and SlateDB. Debug locally, deploy distributed.

## Why Rhei?

**Debuggable.** Replay production state locally. Step through streaming operators in your debugger like any other Rust program. No black-box cluster to SSH into.

**No infrastructure to start.** No JVM. No ZooKeeper. No MiniCluster. `cargo run` starts the full engine on your laptop. Deploy to a cluster by setting environment variables.

**Fast.** Rust's zero-cost abstractions, tiered state caching (RAM -> NVMe -> S3/GCS/Azure Blob), and Timely Dataflow's progress tracking. Hot-path state reads resolve in microseconds without touching disk.

**Scalable.** From single-thread to multi-process clusters. State lives in object storage via SlateDB — scaling out means adding processes, not migrating terabytes of checkpoints.

## Quick Start

```bash
cargo run -p rhei --example pipeline_macro
```

```rust
use rhei::{KeyedState, PrintSink, StateContext, VecSource};

#[rhei::op]
async fn word_counter(input: String, ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
    let mut outputs = Vec::new();
    for word in input.split_whitespace() {
        let key = word.to_string();
        let mut state = KeyedState::<String, u64>::new(ctx, "count");
        let count = state.get(&key).await.unwrap_or(None).unwrap_or(0) + 1;
        state.put(&key, &count);
        outputs.push(format!("{word}: {count}"));
    }
    Ok(outputs)
}

#[rhei::pipeline]
fn main(graph: &DataflowGraph) {
    let lines = vec![
        "hello world".to_string(),
        "hello rhei".to_string(),
        "rhei is a stream processor".to_string(),
    ];

    graph
        .source(VecSource::new(lines))
        .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
        .key_by(|word: &String| word.clone())
        .operator("word_counter", WordCounter)
        .sink(PrintSink::<String>::new());
}
```

`#[rhei::pipeline]` sets up the executor and tokio runtime. `#[rhei::op]` generates the `StreamFunction` impl from a plain async function. For full control, use the builder API directly:

```rust
let executor = Executor::builder()
    .checkpoint_dir("./checkpoints")
    .workers(4)
    .build();

executor.run(graph).await?;
```

## Kafka Example

```rust
use rhei_core::connectors::kafka::source::KafkaSource;
use rhei_core::connectors::kafka::sink::KafkaSink;

let source = KafkaSource::new("localhost:9092", "my-group", &["events"])?
    .with_batch_size(200);

let sink = KafkaSink::new("localhost:9092", "alerts")?;

graph
    .source(source)
    .parse_json::<Event>()
    .filter(|e: &Event| e.severity > 5)
    .key_by(|e: &Event| e.device_id.clone())
    .operator("alerter", Alerter)
    .to_json()
    .sink(sink);
```

Kafka sources support regex topic patterns, header read/write, per-partition parallel consumption, and checkpoint-based offset tracking.

## Architecture

```
Source (async) ──> Transforms ╌╌ ◆ Exchange ◆ ╌╌> Stateful Operators ──> Sink (async)
                                    │
                      hash(key) % N workers
```

Rhei separates the dataflow graph definition from execution:

- **`DataflowGraph`** — Type-safe builder API. `Stream<T>` and `KeyedStream<T>` enforce correct types at compile time. Stateful operators require keyed streams.
- **Compiler** — Converts the logical graph into executable pipeline segments, splitting at exchange boundaries for multi-worker routing.
- **Executor** — Materializes segments into Timely Dataflow workers. Sources and sinks run as async Tokio tasks, bridged to Timely's synchronous workers via bounded channels.

### State Hierarchy

| Tier | Backend | Latency | Role |
|------|---------|---------|------|
| L1 | `HashMap` memtable | Microseconds | Hot working set. Flushed on checkpoint. |
| L2 | Foyer `HybridCache` | Milliseconds | Local NVMe cache. Avoids remote round-trips for warm keys. |
| L3 | SlateDB on S3/GCS/Azure Blob | 10-100ms | Durable source of truth. Enables stateless workers. |

State reads try L1 first. On a miss, the operator yields to the Tokio runtime to fetch from L2/L3 without blocking the Timely worker thread.

### Checkpointing

Frontier-based. When Timely's progress frontier advances past an epoch with no pending futures, the executor triggers a checkpoint: L1 dirty keys flush through to SlateDB, and source offsets are committed.

In cluster mode, a lightweight TCP coordination protocol ensures all processes have flushed state before the checkpoint manifest is committed. Mid-execution checkpoints run concurrently with the dataflow — no stop-the-world pauses.

### Clustering

Rhei scales from a single thread to a multi-process TCP cluster:

| Mode | Config | What changes |
|------|--------|-------------|
| Single-thread | `Executor::builder().build()` | One worker, local state |
| Multi-thread | `.workers(4)` | N worker threads, shared-nothing state per worker |
| Multi-process | `.from_env()` with `RHEI_PEERS`, `RHEI_PROCESS_ID` | N OS processes over TCP, coordinated checkpoints |

In multi-process mode, each process independently opens SlateDB against the same remote object store. Checkpoint coordination happens out-of-band via a separate TCP channel — process 0 acts as coordinator, collecting readiness from all participants before committing a merged manifest.

## Workspace

| Crate | Purpose |
|-------|---------|
| `rhei-core` | Traits (`StreamFunction`, `Source`, `Sink`), operator library (tumbling/sliding/session windows, temporal joins, combinators), state backends, connectors (Kafka, Vec, Print) |
| `rhei-runtime` | Dataflow graph builder, compiler, executor with Timely-backed multi-worker/multi-process execution, checkpoint coordination, async bridges, metrics, tracing |
| `rhei-cli` | CLI (`rhei run`, `rhei run --tui --workers 4`), TUI dashboard with pipeline graph, live metrics, and per-worker logs |
| `rhei` | Convenience crate with `#[rhei::pipeline]` and `#[rhei::op]` proc macros, re-exports core types |

## Operator Library

Built-in operators in `rhei-core`:

- **Windows** — `TumblingWindow`, `SlidingWindow`, `SessionWindow` with pluggable aggregators
- **Joins** — `TemporalJoin` with configurable timeout and state eviction
- **Combinators** — `Filter`, `Map`, `FlatMap`
- **State** — `KeyedState<K, V>` typed wrapper with automatic serde over `StateContext`

Custom operators implement the `StreamFunction` trait:

```rust
#[async_trait]
impl StreamFunction for MyOperator {
    type Input = Event;
    type Output = Alert;

    async fn process(&mut self, input: Event, ctx: &mut StateContext) -> anyhow::Result<Vec<Alert>> {
        let mut state = KeyedState::<String, u64>::new(ctx, "counts");
        let count = state.get(&input.key).await?.unwrap_or(0) + 1;
        state.put(&input.key, &count);

        if count > threshold {
            Ok(vec![Alert { key: input.key, count }])
        } else {
            Ok(vec![])
        }
    }
}
```

Or use the `#[rhei::op]` macro to skip the boilerplate:

```rust
#[rhei::op]
async fn my_operator(input: Event, ctx: &mut StateContext) -> anyhow::Result<Vec<Alert>> {
    // same body as above
}
```

## TUI Dashboard

Try the built-in demo pipeline with live metrics:

```bash
cargo run -p rhei-cli -- run --tui --workers 4
```

```
┌─ Pipeline ─────────────────────────────────────────────────────────────────┐
│ [SensorSource] ──▶ [RangeFilter] ╌╌ ◆ BySensorId ◆ ╌╌▶ [Window] ──▶ [Sink] │
├─ Dashboard ────────────────────────────────────────────────────────────────┤
│ Status: Running  Workers: 4  Uptime: 00:05:23                              │
│ Elements: 1.2M  Batches: 48K  Throughput: 3,800 elem/s                     │
│ L1 Hit: 94.2%   L2 Hit: 5.1%  L3 Hit: 0.7%                                 │
├─ Logs [↑↓] ────────────────────────────────────────────────────────────────┤
│ 12:34:56  INFO  Worker=0  processing batch epoch=42                        │
│ 12:34:56  INFO  Worker=1  processing batch epoch=42                        │
│ 12:34:57  INFO  Worker=0  checkpoint complete duration=12ms                │
└────────────────────────────────────────────────────────────────────────────┘
```

You can also attach to a running pipeline's metrics server or start the web dashboard:

```bash
rhei attach 127.0.0.1:9090   # connect TUI to a running pipeline
rhei demo                     # start built-in demo with HTTP dashboard
```

## Building

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --no-deps -- -D warnings
```

Kafka integration requires the `kafka` feature flag on `rhei-core` and `librdkafka` (built via cmake).

## License

Apache 2.0
