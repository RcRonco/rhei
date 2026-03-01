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
cargo run -p rhei-runtime --example word_count
```

```rust
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;

let graph = DataflowGraph::new();
let stream = graph.source(VecSource::new(lines));

stream
    .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
    .key_by(|word: &String| word.clone())
    .operator("word_counter", WordCounter)
    .map(|result: String| format!("[output] {result}"))
    .sink(PrintSink::new());

let executor = Executor::builder()
    .checkpoint_dir("./checkpoints")
    .workers(4)
    .build();

executor.run(graph).await?;
```

## Architecture

```
Source (async) ──▶ Transforms ╌╌ ◆ Exchange ◆ ╌╌▶ Stateful Operators ──▶ Sink (async)
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

## Operator Library

Built-in operators in `rhei-core`:

- **Windows** — `TumblingWindow`, `SlidingWindow`, `SessionWindow` with pluggable aggregators
- **Joins** — `TemporalJoin` with configurable timeout
- **Combinators** — `Filter`, `Map`, `FlatMap`
- **State** — `KeyedState<K, V>` typed wrapper with automatic serde over `StateContext`

Custom operators implement the `StreamFunction` trait:

```rust
#[async_trait]
impl StreamFunction for MyOperator {
    type Input = Event;
    type Output = Alert;

    async fn process(&mut self, input: Event, ctx: &mut StateContext) -> Vec<Alert> {
        // Read/write state, emit zero or more outputs
    }
}
```

## TUI Dashboard

```
rhei run --tui --workers 4
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

## Building

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --no-deps -- -D warnings
```

Kafka integration requires the `kafka` feature flag on `rhei-core` and `librdkafka` (built via cmake).

## License

Apache 2.0
