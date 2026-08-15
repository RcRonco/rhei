# Rhei

*From "Panta Rhei" (πάντα ῥεῖ) — everything flows.*

A stateful stream processing engine built on Rust, Timely Dataflow, and SlateDB. Debug locally, deploy distributed.

> **Status: pre-1.0, not production-ready.** The engine runs real pipelines, but
> delivery is at-least-once (not exactly-once) and several stability items are
> still open. See [KNOWN-ISSUES.md](KNOWN-ISSUES.md) for the tracked gaps and
> [ROADMAP.md](ROADMAP.md) for what is built versus planned.
>
> Every Rust snippet in this file is compiled by CI as a doctest. See
> [DOCS-AUDIT.md](DOCS-AUDIT.md) for how documentation accuracy is enforced.

## Why Rhei?

**Debuggable.** Replay production state locally. Step through streaming operators in your debugger like any other Rust program. No black-box cluster to SSH into.

**No infrastructure to start.** No JVM. No ZooKeeper. No MiniCluster. `cargo run` starts the full engine on your laptop. Deploy to a cluster by setting environment variables.

**Columnar.** Data moves as Apache Arrow `RecordBatch` buffers, filtering happens through selection vectors instead of copies, and hot state reads resolve from an in-process memtable.

**Scalable.** From single-thread to multi-process clusters. State lives in object storage via SlateDB — scaling out means adding processes, not migrating terabytes of checkpoints.

## Quick Start

```bash
cargo run -p rhei --example quickstart
```

The example below is the full contents of
[`rhei/examples/quickstart.rs`](rhei/examples/quickstart.rs):

```rust,no_run
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema};
use rhei::{KeyedState, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct WordIn {
    text: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct WordOut {
    text: String,
    count: u64,
}

#[rhei::op]
async fn word_counter(
    input: RheiBuffer<WordIn>,
    ctx: &mut OperatorContext,
) -> anyhow::Result<BufferOutput<WordOut>> {
    let mut builder = WordOut::builder(input.len());
    let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "count");

    for view in &input {
        let word = view.text.to_string();
        let count = state.get(&word).await?.unwrap_or(0) + 1;
        state.put(&word, &count)?;
        builder.append(WordOut { text: word, count });
    }

    Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
}

#[rhei::pipeline]
fn main(graph: &DataflowGraph) {
    let words = ["hello", "world", "hello"]
        .into_iter()
        .map(|text| WordIn { text: text.into() })
        .collect();

    graph
        .source(VecSource::new(words))
        .key_by(|w| w.text.to_string())
        .operator("word_counter", WordCounter)
        .sink(PrintSink::<WordOut>::new());
}
```

Three macros do the work:

- `#[derive(RheiSchema)]` generates the Arrow schema, a columnar builder (`WordOutBuilder`), and a zero-copy row view (`WordOutView<'a>`).
- `#[rhei::op]` turns an async function into a struct implementing `StreamFunction`. The struct is the function name in PascalCase — `word_counter` becomes `WordCounter`.
- `#[rhei::pipeline]` wraps `main` with `#[tokio::main]`, builds a `DataflowGraph`, and generates a clap CLI with `--workers`, `--checkpoint-dir`, `--metrics-addr`, `--process-id`, `--peers`, `--from-checkpoint`, and `--offset-delta` (each with a matching `RHEI_*` environment variable).

For full control, drive [`PipelineController`](rhei-runtime/src/controller.rs) yourself:

```rust,no_run
use rhei::{DataflowGraph, PipelineController, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    id: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = DataflowGraph::new();
    graph
        .source(VecSource::new(vec![Event { id: 1 }]))
        .sink(PrintSink::<Event>::new());

    let controller = PipelineController::builder()
        .checkpoint_dir("./checkpoints")
        .workers(4)
        .build()?;

    controller.run(graph).await?;
    Ok(())
}
```

Note that `PipelineController::builder().build()` returns `anyhow::Result<PipelineController>`, and `checkpoint_dir` accepts anything that converts into a `PathBuf`. The shorthand constructor `PipelineController::new(path)` takes a `PathBuf` directly.

## Kafka

Kafka support lives behind the `kafka` feature flag on `rhei-core` and requires `librdkafka`.

<!-- not-compiled: requires the `kafka` feature and librdkafka; the compiled
     equivalent is rhei-runtime/examples/kafka_transform.rs -->

```rust,ignore
use rhei_core::connectors::batch::{KafkaSink, KafkaSource};

let source = KafkaSource::new("localhost:9092", "my-group", &["events"])?
    .with_batch_size(200);
let sink = KafkaSink::new("localhost:9092", "alerts")?;

graph
    .source(source)
    .map(|msg| parse_event(msg))
    .filter_fn(|e| e.severity > 5)
    .key_by(|e| e.device_id.to_string())
    .operator("alerter", Alerter)
    .sink(sink);
```

For a compiled, runnable version see [`rhei-runtime/examples/kafka_transform.rs`](rhei-runtime/examples/kafka_transform.rs).

Kafka sources support per-partition parallel consumption, header read/write, watermark tracking, and checkpoint-based offset commits. Offsets are committed *after* a checkpoint completes, which gives **at-least-once** delivery — a transactional producer sink for exactly-once is on the roadmap but not implemented.

## Architecture

```text
Source (async) ──> Transforms ╌╌ ◆ Exchange ◆ ╌╌> Stateful Operators ──> Sink (async)
                                    │
                      seahash(key) % N workers
```

Rhei separates the dataflow graph definition from execution:

- **`DataflowGraph`** — Builder API. `Stream<'a, T>` is a `Copy` handle to a point in the graph; reusing one creates fan-out. `T` is checked at compile time, so connecting mismatched element types is a type error.
- **Compiler** — Converts the logical graph into a Timely dataflow, inserting an Exchange pact at every `key_by` node.
- **Executor** — Runs the dataflow on Timely worker threads. Sources and sinks run as async Tokio tasks, bridged to Timely's synchronous workers via bounded channels.

There is a single `Stream<'a, T>` type. Rhei does **not** have a separate `KeyedStream` type, and `.operator()` is available on any stream — placing a stateful operator on an unkeyed stream compiles, and each worker will then see an arbitrary subset of keys. Keying is your responsibility; see [Keying and state](#keying-and-state).

### State Hierarchy

| Tier | Backend | Typical latency | Role |
|------|---------|-----------------|------|
| L1 | `HashMap` memtable + `moka` W-TinyLFU cache | Sub-microsecond | Hot working set. Dirty entries flush on checkpoint. |
| L2 | Foyer `HybridCache` | Sub-millisecond to milliseconds | Local NVMe cache. Avoids remote round-trips for warm keys. |
| L3 | SlateDB on S3/GCS/Azure Blob | 10-100ms | Durable source of truth. Enables stateless workers. |

State reads try L1 first. **On a miss the Timely worker thread blocks** on the L2/L3 fetch via `tokio::runtime::Handle::block_on` (see [`rhei-runtime/src/timely_operator.rs`](rhei-runtime/src/timely_operator.rs)). A non-blocking async cold path requires an operator API redesign and is tracked as KI-11 in [KNOWN-ISSUES.md](KNOWN-ISSUES.md).

The latency figures above are order-of-magnitude expectations for each backend, not measured benchmark results. Run `just bench` for numbers from your own hardware.

### Checkpointing

Frontier-based. When Timely's progress frontier advances past an epoch, the executor triggers a checkpoint: L1 dirty keys flush through to SlateDB, and source offsets are committed. The default interval is every 100 batches, configurable with `PipelineController::builder().checkpoint_interval(n)`.

In cluster mode, a TCP coordination protocol ensures all processes have flushed state before the checkpoint manifest is committed. Mid-execution checkpoints run concurrently with the dataflow — no stop-the-world pauses.

Delivery is **at-least-once**: source offsets are committed after a checkpoint completes, so a crash replays everything since the last one. Sinks must tolerate duplicates.

### Clustering

| Mode | Config | What changes |
|------|--------|--------------|
| Single-thread | `PipelineController::builder().build()?` | One Timely worker, local state |
| Multi-thread | `.workers(4)` | N worker threads, shared-nothing state per worker |
| Multi-process | `.from_env()` with `RHEI_PEERS`, `RHEI_PROCESS_ID` | N OS processes over TCP, coordinated checkpoints |

In multi-process mode, each process independently opens SlateDB against the same remote object store. Checkpoint coordination happens out-of-band via a separate TCP channel — process 0 acts as coordinator, collecting readiness from all participants before committing a merged manifest.

Key ownership is decoupled from worker count via **key groups** (the Flink-compatible scheme): state is addressed as `kg{group}/{operator}/{key}`, so the worker count can change without rewriting state. Gossip-based membership uses `chitchat` behind the optional `chitchat` feature on `rhei-runtime`.

Rhei has **no control plane**. There is no job manager, no leader election, and no gRPC submission API — processes are started externally and told about each other via flags or environment variables. OpenRaft-backed job metadata is a planned Phase 3 item; see [CLUSTERING.md](CLUSTERING.md).

## Workspace

Five Cargo workspace members:

| Crate | Purpose |
|-------|---------|
| `rhei-core` | Traits (`StreamFunction`, `Source`, `Sink`), Arrow columnar buffer (`RheiBuffer<T>`), operator library (tumbling/sliding/session/count windows, temporal joins, sequence detection, combinators), state backends, connectors (Kafka, Vec, Print), DLQ |
| `rhei-runtime` | `DataflowGraph` builder, compiler, `PipelineController`, Timely-backed multi-worker/multi-process execution, checkpoint coordination, async bridges, metrics, tracing |
| `rhei-macros` | Proc macros: `#[rhei::op]`, `#[rhei::pipeline]`, `#[derive(RheiSchema)]` |
| `rhei` | Facade crate re-exporting core and runtime types plus the macros |
| `rhei-cli` | CLI (`rhei new`, `rhei run`, `rhei attach`, `rhei demo`) with a TUI dashboard |

Two more packages live in the repo but outside the Cargo workspace:

| Package | Purpose |
|---------|---------|
| `rhei-python` | PyO3 bindings (excluded from the workspace; built with `just py-build`) |
| `rhei-dashboard` | TypeScript/Vite web dashboard served by the metrics HTTP server |

## Operator Library

Built-in operators in `rhei-core`, all re-exported from `rhei`:

- **Windows** — `TumblingWindow`, `SlidingWindow`, `SessionWindow`, `CountWindow`
- **Joins** — `TemporalJoin` with configurable timeout and state eviction
- **Pattern matching** — `SequenceDetect` (ordered event sequence matching)
- **Aggregation** — `ReduceOp`, `RollingAggregateOp`
- **Combinators** — `MapOp`, `FlatMapOp`, `FilterOp`, `FilterFnOp`, `FilterExprOp`
- **State** — `KeyedState<K, V>`, `ValueState`, `ListState`, `MapState`, `TimerService`

Windows are built with `Window::tumbling/sliding/session/count`, which name the key/time/accumulate/finish closures and refuse to `build()` until every one is set; the positional `::new(...)` constructor remains available. Join operators take positional closures only. See [`rhei/examples/batch_window_agg.rs`](rhei/examples/batch_window_agg.rs) for a complete `Window::tumbling` setup, and [docs/operators.md](docs/operators.md#windows) for every constructor.

`#[derive(RheiSchema)]` generates a typed handle per column, so filter predicates check the column name and literal type at compile time — `stream.filter(PageView::col().dwell_ms().gt(3_000.0))`.

### Keying and state

`key_by` takes a closure over a zero-copy row view and returns the partition key as a `String`:

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    path: String,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            path: "/checkout".into(),
        }]))
        .filter_fn(|v| !v.path.starts_with("/health"))
        .key_by(|v| v.user_id.to_string())
        .sink(PrintSink::<PageView>::new());
}
```

Custom operators implement `StreamFunction` over Arrow-columnar buffers. The trait requires `Clone` at the call site, because `.operator()` clones the operator once per worker:

```rust,no_run
use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};
use rhei::KeyedState;

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    key: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Alert {
    key: String,
    count: u64,
}

#[derive(Clone)]
struct MyOperator {
    threshold: u64,
}

#[async_trait::async_trait]
impl StreamFunction for MyOperator {
    type Input = Event;
    type Output = Alert;

    async fn process(
        &mut self,
        input: RheiBuffer<Event>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Alert>> {
        let mut builder = Alert::builder(input.len());
        let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");

        for view in &input {
            let key = view.key.to_string();
            let count = state.get(&key).await?.unwrap_or(0) + 1;
            state.put(&key, &count)?;
            if count > self.threshold {
                builder.append(Alert { key, count });
            }
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}
```

`#[rhei::op]` generates exactly this impl (including `#[derive(Clone, Debug)]`) from the async function body, for the common case of an operator with no configuration fields.

## TUI Dashboard

Run the built-in demo pipeline with live metrics:

```bash
cargo run -p rhei-cli -- demo --workers 4
```

The TUI shows a pipeline graph, a metrics panel, and a scrollable log view. The layout below is illustrative — exact figures depend on your pipeline:

```text
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

Other CLI subcommands:

```bash
rhei new my-pipeline          # scaffold a new project
rhei run --tui --workers 4    # run the current project with the TUI
rhei attach 127.0.0.1:9090    # attach the TUI to a running pipeline
rhei demo                     # built-in demo with the HTTP dashboard
```

The `rhei` CLI is not published to crates.io. Install it from a checkout with `cargo install --path rhei-cli`.

## Running in Production

[docs/deployment.md](docs/deployment.md) covers configuration, probe semantics, metrics, capacity planning, and a runbook. [SECURITY.md](SECURITY.md) tracks security issues and carries a hardening checklist. [KNOWN-ISSUES.md](KNOWN-ISSUES.md) is the honest register of what is and is not finished — read all three before committing to a deployment.

```bash
docker build -f deploy/Dockerfile --build-arg BIN=my-pipeline -t my-pipeline:v1 .
kubectl apply -f deploy/kubernetes/statefulset.yaml
kubectl apply -f deploy/kubernetes/monitoring.yaml
```

The manifests in `deploy/kubernetes/` are a commented starting point: a StatefulSet with all three probes wired up, a headless Service for the data plane, Prometheus alert rules, and a PodDisruptionBudget.

Two things worth knowing before you deploy:

- **`RHEI_MAX_PARALLELISM` is permanent.** It fixes the key group count, and changing it re-partitions the entire key space. Rhei refuses to start rather than silently read empty state, and there is no migration path — pick a value above the largest worker count you will ever run.
- **The cluster data plane is unencrypted.** Ports 2101 and the checkpoint coordination port are plaintext TCP with no authentication — anyone who can reach them can read records in flight and inject frames. Keep them on a trusted network. See [SECURITY.md](SECURITY.md) SI-4.

## Building

```bash
cargo check --workspace --all-targets
cargo nextest run --workspace     # unit and integration tests
cargo test --doc --workspace      # documentation examples (nextest cannot run these)
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
```

Or `just ci` to run the whole sequence. Kafka integration requires the `kafka` feature flag on `rhei-core` and `librdkafka` (linked dynamically via Homebrew or a system package).

## Documentation

Start at **[docs/README.md](docs/README.md)** for the full map.

| Document | Contents |
|----------|----------|
| [docs/getting-started.md](docs/getting-started.md) | Install, first pipeline, core API, troubleshooting |
| [docs/concepts.md](docs/concepts.md) | The ideas Rhei is built on and what each one costs you — why the API looks the way it does |
| [docs/walkthrough.md](docs/walkthrough.md) | One clickstream pipeline built step by step: schemas → state → session windows → tests → deploy |
| [docs/operators.md](docs/operators.md) | Every operator, exact constructor, compiled example |
| [docs/time-and-watermarks.md](docs/time-and-watermarks.md) | Event time, watermarks, frontiers, when windows fire, lateness |
| [docs/exchange-and-partitioning.md](docs/exchange-and-partitioning.md) | `key_by`, key groups, `max_parallelism`, rescaling, skew |
| [docs/state-and-checkpointing.md](docs/state-and-checkpointing.md) | State tiers, key layout, checkpoint protocol, recovery, tuning |
| [docs/deployment.md](docs/deployment.md) | Config, scaling modes, metrics, runbook, operational limits |
| [docs/internals.md](docs/internals.md) | Graph → Timely, async bridge, exchange, state paths, checkpoint flow |
| [API.md](API.md) | Reference for `DataflowGraph`, `Stream`, `PipelineController` |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System topology, execution model, data flow paths |
| [CLUSTERING.md](CLUSTERING.md) | Single-thread → multi-process → control plane plan |
| [KNOWN-ISSUES.md](KNOWN-ISSUES.md) | Tracked gaps and correctness limitations |
| [SECURITY.md](SECURITY.md) | Security issue register, trust boundaries, hardening checklist |
| [ROADMAP.md](ROADMAP.md) | Built vs. planned work |
| [DOCS-AUDIT.md](DOCS-AUDIT.md) | Documentation accuracy audit and enforcement mechanism |
| [ADR/](ADR/) | Architecture decision records |

## License

Apache 2.0
