# Rhei Documentation

Every Rust example in these pages is compiled by CI as a doctest. Blocks that cannot be compiled are marked `ignore` with a stated reason. See [DOCS-AUDIT.md](../DOCS-AUDIT.md) for how that is enforced.

> **Status: pre-1.0.** Delivery is at-least-once, there is no control plane, and
> several stability items are open. [KNOWN-ISSUES.md](../KNOWN-ISSUES.md) is the
> honest list; [ROADMAP.md](../ROADMAP.md) separates built from planned.

## Start here

| Order | Page | What it covers |
|-------|------|----------------|
| 1 | [getting-started.md](getting-started.md) | Install, first pipeline, core API, troubleshooting |
| 2 | [concepts.md](concepts.md) | The ideas Rhei is built on and what each one costs you — why the API looks the way it does |
| 3 | [walkthrough.md](walkthrough.md) | One clickstream pipeline built step by step: schemas → state → session windows → tests → deploy |

## Reference

| Page | What it covers |
|------|----------------|
| [operators.md](operators.md) | Every operator, exact constructor, compiled example |
| [../API.md](../API.md) | `DataflowGraph`, `Stream`, `PipelineController` |
| [concepts.md](concepts.md#vocabulary) | Vocabulary lookup: term → meaning → where it lives |

## Semantics

| Page | What it covers |
|------|----------------|
| [time-and-watermarks.md](time-and-watermarks.md) | Event time, watermarks, frontiers, when windows fire, lateness |
| [exchange-and-partitioning.md](exchange-and-partitioning.md) | `key_by`, key groups, `max_parallelism`, rescaling, skew |
| [state-and-checkpointing.md](state-and-checkpointing.md) | State tiers, key layout, checkpoint protocol, recovery, tuning |

## Operations

| Page | What it covers |
|------|----------------|
| [deployment.md](deployment.md) | Config, scaling modes, probes, metrics, containers, runbook, operational limits |
| [../SECURITY.md](../SECURITY.md) | Security issue register, trust boundaries, hardening checklist |

## Internals

| Page | What it covers |
|------|----------------|
| [internals.md](internals.md) | Graph → Timely, the async bridge, exchange, state paths, checkpoint flow |
| [../ARCHITECTURE.md](../ARCHITECTURE.md) | System topology and component breakdown |
| [../INPROC-ARCH.md](../INPROC-ARCH.md) | Controller / TaskManager / Executor layering |
| [../CLUSTERING.md](../CLUSTERING.md) | Phased plan: multi-thread → multi-process → control plane |
| [../ADR/](../ADR/) | Architecture decision records |

## Compiled examples

Runnable code, verified by `cargo check --workspace --all-targets`:

| Example | Shows |
|---------|-------|
| [`rhei/examples/quickstart.rs`](../rhei/examples/quickstart.rs) | Minimal keyed stateful pipeline with the macros |
| [`rhei/examples/walkthrough.rs`](../rhei/examples/walkthrough.rs) | The full walkthrough pipeline: funnel tracking plus session windows |
| [`rhei/examples/batch_word_count.rs`](../rhei/examples/batch_word_count.rs) | `flat_map` into a tumbling window |
| [`rhei/examples/batch_window_agg.rs`](../rhei/examples/batch_window_agg.rs) | Per-key windowed aggregation |
| [`rhei-runtime/examples/word_count.rs`](../rhei-runtime/examples/word_count.rs) | Word count without the facade crate |
| [`rhei-runtime/examples/window_agg.rs`](../rhei-runtime/examples/window_agg.rs) | Window aggregation at the runtime level |
| [`rhei-runtime/examples/temporal_join.rs`](../rhei-runtime/examples/temporal_join.rs) | `merge` → `key_by` → `TemporalJoin` |
| [`rhei-runtime/examples/kafka_transform.rs`](../rhei-runtime/examples/kafka_transform.rs) | Kafka source and sink (`kafka` feature) |

## Common questions

| Question | Answer |
|----------|--------|
| Why does my closure get `&str` instead of `String`? | Closures receive zero-copy views borrowed from Arrow. [concepts.md](concepts.md#2-the-unit-of-work-is-a-batch-of-columns-not-a-record) |
| Why is my count wrong with 4 workers? | Missing `key_by` before a stateful operator. Nothing enforces it. [exchange-and-partitioning.md](exchange-and-partitioning.md#the-missing-guarantee) |
| Why is there no windowed output? | The source emits no watermarks, or one partition is idle. [time-and-watermarks.md](time-and-watermarks.md#when-windows-fire) |
| Why did throughput fall off a cliff? | L1 misses block the Timely worker thread. [state-and-checkpointing.md](state-and-checkpointing.md#the-cold-path-blocks) |
| Why was my checkpoint restore rejected? | `max_parallelism` differs; it is immutable per pipeline. [state-and-checkpointing.md](state-and-checkpointing.md#the-manifest) |
| Can I get exactly-once? | No. At-least-once only; make sinks idempotent |
| Where is `TumblingWindow::builder()`? | There is none — windows use `::new(...)`. [operators.md](operators.md#windows) |
| How do I test a pipeline? | `VecSource` plus a collecting sink; there is no test harness. [walkthrough.md](walkthrough.md#step-7-test-it) |
| Why does `/healthz` return 503? | A worker stopped making progress. The body names it. [deployment.md](deployment.md#the-two-probes-answer-different-questions) |
| Why did the process refuse to start? | Malformed config or an unreadable checkpoint — both fail loudly. [deployment.md](deployment.md#the-pipeline-will-not-start) |
| Is `/api/state/**` safe to expose? | It returns application data and is off unless enabled. [SECURITY.md](../SECURITY.md) |
