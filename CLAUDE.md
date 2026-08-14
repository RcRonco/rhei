# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo check --workspace --all-targets
cargo nextest run --workspace               # run all tests (nextest)
cargo nextest run -p rhei-core              # test a single crate
cargo nextest run -p rhei-runtime -E 'test(word_count)'  # run a single test by name
cargo test --doc --workspace                # doc examples — nextest CANNOT run these
python3 scripts/check-doc-blocks.py         # documentation invariants
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check                  # check formatting
cargo fmt --all                             # fix formatting
```

**`cargo nextest` does not run doctests.** Any change touching a documented API
must also pass `just docs` (`cargo test --doc --workspace` plus
`scripts/check-doc-blocks.py`), or documentation drift goes unnoticed.

CI uses [cargo-nextest](https://nexte.st/) with JUnit reporting for GitHub test summaries. Install locally with `cargo install cargo-nextest`. CI also runs `cargo deny check advisories,licenses,bans` for license/advisory enforcement.

## Workspace Structure

Five Cargo workspace members:

- **rhei-core** — Traits (`StreamFunction`, `Source`, `Sink`), Arrow columnar buffer (`RheiBuffer<T>`, `RheiSchema`), operator library (windows, joins, combinators), state backends (L1 memtable, L2 Foyer, L3 SlateDB), connectors (Kafka, Vec, Print), DLQ (`DlqSink` trait).
- **rhei-runtime** — Dataflow graph builder (`DataflowGraph`, `Stream<T>`), compiler, executor with Timely-backed multi-worker/multi-process execution. `ErasedBuffer` transports Arrow batches through Timely. `bridge.rs` bridges async Source/Sink to sync Timely channels. `task_manager.rs` orchestrates background services.
- **rhei-macros** — Proc macros (`#[rhei::pipeline]`, `#[rhei::op]`).
- **rhei** — Convenience crate re-exporting core types and macros.
- **rhei-cli** — CLI (`rhei new`, `rhei run`, `rhei run --tui`, `rhei attach`, `rhei demo`). TUI dashboard with graph view, metrics, and log viewer.

Two packages live in the repo but outside the Cargo workspace: **rhei-python**
(PyO3 bindings, `exclude`d in the root manifest, built via `just py-build`) and
**rhei-dashboard** (TypeScript/Vite web dashboard).

## Architecture

**Execution model:** Arrow columnar. Data flows as `RheiBuffer<T>` (typed `RecordBatch` + selection vector), type-erased to `ErasedBuffer` for Timely transport. Timely Dataflow runs on blocking threads (`spawn_blocking`). Async sources/sinks bridged via bounded `tokio::sync::mpsc` channels. Clustering design in `CLUSTERING.md`.

**Data path:** Source produces `RheiBuffer<T>` → erased to `ErasedBuffer` → Timely operators (Pipeline/Exchange pact) → recovered to `RheiBuffer<T>` → `StreamFunction::process()` → output → Sink.

**Exchange:** `key_by` uses two-stage Timely operator: (1) split rows by `seahash(key) % workers` into per-worker sub-buffers, (2) route via Exchange pact. Serialization is Arrow IPC.

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

## Documentation Accuracy

Documentation in this repo is machine-verified, and changes must keep it that way.

- `README.md`, `API.md`, and `docs/getting-started.md` are included as doctests
  from `rhei/src/lib.rs`, so **every Rust block in them is compiled by CI**.
- A ```rust block may opt out with `ignore` only alongside a `not-compiled:
  <reason>` comment. `scripts/check-doc-blocks.py` fails the build otherwise.
- The README quick start must stay byte-identical to the anchored region of
  `rhei/examples/quickstart.rs`; the same script enforces this.
- Distinguish built from planned. Aspirational designs belong in ADRs,
  `ROADMAP.md`, or `CLUSTERING.md` — or under an explicit **PLANNED** marker —
  never in the present tense in `README.md`, `API.md`, or `ARCHITECTURE.md`.
- When resolving a `KNOWN-ISSUES.md` entry, update the matching `ROADMAP.md`
  checkbox in the same change. The two contradicting each other is a bug.

See `DOCS-AUDIT.md` for the audit that established these rules.

## Design Documents

- `ARCHITECTURE.md` — Full system topology, component breakdown, data flow paths.
- `CLUSTERING.md` — Three-phase plan from single-thread to distributed (multi-thread → multi-process → control plane with OpenRaft/chitchat).
- `ROADMAP.md` — Checklist of planned work across DX, integrations, observability, performance, stability, clustering.
- `PLAN.md` — Issue-level breakdown of completed foundation work.
