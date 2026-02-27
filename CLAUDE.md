# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo test -p rhei-core                     # test a single crate
cargo test -p rhei-runtime word_count       # run a single test by name
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check                  # check formatting
cargo fmt --all                             # fix formatting
```

CI also runs `cargo deny check advisories,licenses,bans` for license/advisory enforcement.

## Workspace Structure

Three crates:

- **rhei-core** — Traits (`StreamFunction`, `Source`, `Sink`), operator library (windows, joins, combinators), state backends (L1 memtable, L2 Foyer, L3 SlateDB), logical plan builder (`StreamGraph`), connectors (Kafka, Vec, Print).
- **rhei-runtime** — Executor that materializes logical plans into Timely dataflows. `AsyncOperator` wraps `StreamFunction` with hot/cold path split. `TimelyAsyncOperator` adds capability tracking. `bridge.rs` bridges async Source/Sink to sync Timely channels. `pipeline.rs` provides the fluent builder API.
- **rhei-cli** — CLI (`rhei new`, `rhei run`, `rhei run --tui`). TUI dashboard with graph view, metrics, and log viewer.

## Architecture

**Execution model:** Timely Dataflow runs on a blocking thread (`spawn_blocking`). Async sources/sinks are bridged via bounded `tokio::sync::mpsc` channels. Currently single-threaded (`execute_directly()`). Clustering design in `CLUSTERING.md`.

**Hot/cold path:** `AsyncOperator` polls the `StreamFunction` future once synchronously. If it resolves (L1 cache hit), output is returned immediately. If pending (state miss), `block_in_place` drives the future on the Tokio runtime to fetch from L2/L3.

**State hierarchy:** L1 `HashMap` memtable (microseconds) → L2 Foyer `HybridCache` on NVMe (milliseconds) → L3 SlateDB on S3 (10s-100s ms). `PrefixedBackend` namespaces keys per operator as `{operator_name}/{user_key}`.

**Checkpointing:** Triggered when Timely frontier advances and no pending futures remain. L1 dirty keys flush through to SlateDB/S3. Source offsets committed after checkpoint.

**`TimelyAsyncOperator` is `!Send`** due to `Rc` in Timely capabilities. It must be constructed inside the worker thread, not moved across threads. The `Mutex` wrapping pattern in `executor.rs` handles this.

## Code Conventions

- Rust edition 2024. `unsafe` code is forbidden workspace-wide.
- Clippy `all` is deny, `pedantic` is warn.
- `rustfmt.toml`: max_width=100, edition 2024.
- Operator types implement `StreamFunction` (async trait with `Input`/`Output` associated types).
- State access goes through `StateContext` (or the typed `KeyedState<K, V>` wrapper).
- Kafka integration is behind the `kafka` feature flag on `rhei-core`.

## ADR (Architecture Decision Records)

Every big feature must include an ADR under `ADR/<feature-name>.md`. An ADR should cover: context, decision (what and why), a **Diagram** section with Mermaid diagrams illustrating data/control flow and component relationships, alternatives considered with rationale, and consequences (positive and negative). See `ADR/checkpoint-manifest.md` for the reference format.

## Design Documents

- `ARCHITECTURE.md` — Full system topology, component breakdown, data flow paths.
- `CLUSTERING.md` — Three-phase plan from single-thread to distributed (multi-thread → multi-process → control plane with OpenRaft/chitchat).
- `ROADMAP.md` — Checklist of planned work across DX, integrations, observability, performance, stability, clustering.
- `PLAN.md` — Issue-level breakdown of completed foundation work.
