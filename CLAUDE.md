# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo check --workspace --all-targets
cargo nextest run --workspace               # run all tests (nextest)
cargo nextest run -p rhei-core              # test a single crate
cargo nextest run -p rhei-runtime -E 'test(word_count)'  # run a single test by name
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check                  # check formatting
cargo fmt --all                             # fix formatting
```

CI uses [cargo-nextest](https://nexte.st/) with JUnit reporting for GitHub test summaries. Install locally with `cargo install cargo-nextest`. CI also runs `cargo deny check advisories,licenses,bans` for license/advisory enforcement.

## Workspace Structure

Five crates:

- **rhei-core** — Traits (`StreamFunction`, `Source`, `Sink`), Arrow columnar buffer (`RheiBuffer<T>`, `RheiSchema`), operator library (windows, joins, combinators), state backends (L1 memtable, L2 Foyer, L3 SlateDB), connectors (Kafka, Vec, Print), DLQ (`DlqSink` trait).
- **rhei-runtime** — Dataflow graph builder (`DataflowGraph`, `Stream<T>`), compiler, executor with Timely-backed multi-worker/multi-process execution. `ErasedBuffer` transports Arrow batches through Timely. `bridge.rs` bridges async Source/Sink to sync Timely channels. `task_manager.rs` orchestrates background services.
- **rhei-macros** — Proc macros (`#[rhei::pipeline]`, `#[rhei::op]`).
- **rhei** — Convenience crate re-exporting core types and macros.
- **rhei-cli** — CLI (`rhei new`, `rhei run`, `rhei run --tui`). TUI dashboard with graph view, metrics, and log viewer.

## Architecture

**Execution model:** Arrow columnar. Data flows as `RheiBuffer<T>` (typed `RecordBatch` + selection vector), type-erased to `ErasedBuffer` for Timely transport. Timely Dataflow runs on blocking threads (`spawn_blocking`). Async sources/sinks bridged via bounded `tokio::sync::mpsc` channels. Clustering design in `CLUSTERING.md`.

**Data path:** Source produces `RheiBuffer<T>` → erased to `ErasedBuffer` → Timely operators (Pipeline/Exchange pact) → recovered to `RheiBuffer<T>` → `StreamFunction::process()` → output → Sink.

**Exchange:** `key_by` uses two-stage Timely operator: (1) split rows by `seahash(key) % workers` into per-worker sub-buffers, (2) route via Exchange pact. Serialization is Arrow IPC. Split-stage scratch (per-row keys, row-index lists) lives in a per-operator bumpalo arena (`ExchangeScratch`), reset per batch — see `ADR/bumpalo-exchange-arena.md`.

**State hierarchy:** L1 `HashMap` memtable (microseconds) → L2 Foyer `HybridCache` on NVMe (milliseconds) → L3 SlateDB on S3 (10s-100s ms). `PrefixedBackend` namespaces keys per operator as `{operator_name}/{user_key}`.

**Checkpointing:** Triggered when Timely frontier advances. L1 dirty keys flush through to SlateDB/S3. Source offsets committed after checkpoint. Checkpoint manifest tracks operators and offsets.

**Process layering:** Controller (config/lifecycle) → TaskManager (background services, bridging) → DataflowExecutor (per-worker Timely compilation). See `INPROC-ARCH.md`.

## Code Conventions

- Rust edition 2024. `unsafe` code is forbidden workspace-wide.
- Clippy `all` is deny, `pedantic` is warn.
- `rustfmt.toml`: max_width=100, edition 2024.
- Operator types implement `StreamFunction` (async trait: `process(RheiBuffer<I>, &mut OperatorContext) -> BufferOutput<O>`).
- State access goes through `StateContext` (or the typed `KeyedState<K, V>` wrapper).
- Kafka integration is behind the `kafka` feature flag on `rhei-core`.

## ADR (Architecture Decision Records)

Every big feature must include an ADR under `ADR/<feature-name>.md`. An ADR should cover: context, decision (what and why), a **Diagram** section with Mermaid diagrams illustrating data/control flow and component relationships, alternatives considered with rationale, and consequences (positive and negative). See `ADR/checkpoint-manifest.md` for the reference format.

## Design Documents

- `ARCHITECTURE.md` — Full system topology, component breakdown, data flow paths.
- `CLUSTERING.md` — Three-phase plan from single-thread to distributed (multi-thread → multi-process → control plane with OpenRaft/chitchat).
- `ROADMAP.md` — Checklist of planned work across DX, integrations, observability, performance, stability, clustering.
- `PLAN.md` — Issue-level breakdown of completed foundation work.
