# Documentation Trustworthiness Audit

**Scope:** documentation accuracy only — whether what the docs claim matches what
the code does. This is not a production-readiness or code-quality review.

**Baseline:** `31c6191` (merge of #31).
**Method:** every factual claim in the user-facing docs was checked against the
source. Every code sample was compiled.

**Prior assessment: 4/10.** That score was earned. The headline finding is not
that individual sentences were wrong — it is that **nothing in any document was
ever compiled or checked**, so the docs drifted freely from the code and there
was no mechanism that could have noticed.

---

## Why the score was 4/10

### Root cause: zero verification

| Fact | Consequence |
|------|-------------|
| All 16 Rust code blocks in rustdoc comments were fenced ```` ```ignore ```` | No doc example anywhere in the workspace was compiled |
| Markdown docs were not wired into any test target | README/API/tutorial code was never even parsed |
| CI runs `cargo nextest run`, which **cannot execute doctests** | Even if doctests had existed, CI would have skipped them |

With no feedback loop, the docs recorded *intent at time of writing*. The engine
was rewritten from a row-based execution model to an Arrow columnar one, and the
tutorial and API reference were simply left behind describing the old design.

### The three failure modes

**1. Documented APIs that do not exist.** Not "renamed" — absent.

| Documented | Reality |
|-----------|---------|
| `Executor`, `Executor::builder()` | No such type. It is `PipelineController::builder()` |
| `KeyedStream<'a, T>` | No such type. There is one `Stream<'a, T>` |
| `TumblingWindow::builder()`, `SlidingWindow::builder()`, `SessionWindow::builder()`, `TemporalJoin::builder()` | No builders. All use `::new(...)` with positional closures |
| `rhei_core::testing::{TestSource, CollectSink}` and six `assert_*` helpers | **No `testing` module exists at all** |
| `.dlq(sink)` / `.with_dlq(|errors| ...)` stream methods | Do not exist. DLQ is configured on the controller |
| `BufferOutput::from_builder` | Does not exist |
| `rhei_core::traits::StreamFunction` | No `traits` module; it is `rhei_core::arrow` |
| `rhei_core::connectors::print_sink` / `::vec_source` | Path is `rhei_core::connectors::batch` |
| `Avg` aggregator | Does not exist |
| `#[rhei::main]` (ROADMAP) | The macro is `#[rhei::pipeline]` |
| `rhei run --attach <url>` (ROADMAP) | `rhei attach <addr>` is its own subcommand |

**2. Code samples that could not compile.** Every Rust sample in `README.md`,
`API.md`, and `docs/getting-started.md` failed for at least one reason:

- The tutorial's "Your First Pipeline" streamed `String` elements. Stream
  elements must derive `RheiSchema`; `String` does not implement it.
- The tutorial's `StreamFunction` impl used the pre-Arrow signature
  `process(&mut self, input: T, ctx: &mut StateContext) -> Vec<O>`. The real one
  is `process(&mut self, RheiBuffer<I>, &mut OperatorContext) -> BufferOutput<O>`.
- `map`/`filter_fn`/`key_by` closures were written over owned values; they
  receive zero-copy `T::View<'_>` borrows.
- The README quick start called `state.get(view.text)` where `get` takes `&K`
  (`&String`) and `view.text` is `&str`; discarded the `Result` from
  `state.put`; imported `RheiBuilder` but not the trait providing `builder()`;
  and used the nonexistent `BufferOutput::from_builder`.
- The README's manual `StreamFunction` impl omitted `Clone`, which
  `.operator()` requires.
- `README.md`'s quick-start command ran `--example pipeline_macro`, which did
  not exist. The examples were `batch_word_count` and `batch_window_agg`.
- `ARCHITECTURE.md` called `PipelineController::new("./checkpoints")`; `new`
  takes a `PathBuf`, and `&str` does not coerce.

**3. Claims contradicted by the code, and by each other.** These are the most
damaging, because they are the claims a reader uses to make design decisions.

- **"Stateful operators require keyed streams. This is a compile-time
  guarantee."** (`API.md`, `docs/getting-started.md`) — False, and the inverse
  of a safety-relevant truth. `.operator()` is available on any `Stream`.
  Attaching a stateful operator without `key_by()` compiles, runs, and silently
  produces wrong results on more than one worker.
- **"the operator yields to the Tokio runtime to fetch from L2/L3 without
  blocking the Timely worker thread."** (`README.md`) — False.
  `rhei-runtime/src/timely_operator.rs:34` calls `rt.block_on(...)`; the source
  comment right above it reads "Blocks on the Tokio runtime." The project's own
  `KNOWN-ISSUES.md` KI-11 also contradicted the README.
- **`block_in_place`** (`ARCHITECTURE.md` cold path) — that function is not
  called anywhere in the workspace.
- **A control plane that does not exist.** `ARCHITECTURE.md`'s "System Topology"
  showed a JobManager with a gRPC API, job scheduler, and OpenRaft consensus as
  present-tense architecture. `openraft` and `tonic` are not dependencies of any
  crate; `ROADMAP.md` lists them as unchecked Phase 3 items.
- **Three mutually inconsistent state key schemes** across docs:
  `w{worker}/{op}/{key}` (API.md), `p{pid}/w{idx}/{op}` (KNOWN-ISSUES/CLUSTERING),
  and `kg{group}/{op}/{key}` (ROADMAP Phase 3). Only the last is current.
- **ROADMAP vs KNOWN-ISSUES on KI-7.** ROADMAP marked memtable eviction done;
  KNOWN-ISSUES described the memtable as "a plain `HashMap`... no capacity
  limit, eviction policy". Both were wrong: clean entries *are* evicted via a
  `moka` W-TinyLFU cache bounded by `MemTableConfig`, but dirty entries are
  never evicted, so the unbounded-growth risk is real for write-heavy loads.
- **"Source parallelism is a future extension"** (`API.md`) — partitioned source
  consumption is implemented (`PartitionedVecSource`, `KafkaSource`,
  `ADR/partitioned-source.md`) and ROADMAP marks it done.
- **"When `workers == 1`, the entire pipeline runs as a simple async loop. No
  threads, no channels"** (`API.md`) — single-worker runs as a Timely dataflow
  like any other.
- **No delivery-semantics statement anywhere.** The docs described
  checkpointing and offset commits in detail without ever saying that the
  resulting guarantee is at-least-once, while `ROADMAP.md` listed exactly-once
  as unimplemented.
- **Unmarked aspiration presented as fact**, including "Scalable", "instant
  autoscaling", and latency figures ("Microseconds", "94.2% L1 hit") given
  without any indication they were illustrative rather than measured.

**4. Structural inaccuracies.**

- `README.md` and `CLAUDE.md` described the repo as four/five crates, never
  mentioning `rhei-macros` in the workspace table, nor `rhei-python` and
  `rhei-dashboard`, which exist in the repo outside the Cargo workspace.
- `docs/getting-started.md` told readers to run `cargo add rhei-core
  rhei-runtime`. Nothing is published to crates.io; they are path dependencies.
- `rhei new` was presented as available with no way to obtain the CLI.
- `README.md` advertised the TUI via `rhei run --tui` for a demo pipeline, but
  `run` operates on the current project; `rhei demo` is the built-in one.

---

## What changed

### Corrected

| File | Change |
|------|--------|
| `README.md` | Rewritten. Quick start now quotes the compiled `rhei/examples/quickstart.rs`; `Executor` → `PipelineController`; `KeyedStream` claim removed and replaced with an explicit warning; blocking cold path stated plainly; workspace table completed; delivery semantics and pre-1.0 status stated up front |
| `API.md` | Rewritten as a reference to the API that exists. `KeyedStream` and `Executor` sections replaced; builder-pattern operator examples replaced with real `::new` signatures; view-based closure signatures tabulated; state addressing corrected to key groups; worker assignment corrected for partitioned sources; a delivery-semantics section added. The rejected-alternatives section now also records that the `KeyedStream` safety property was never built |
| `docs/getting-started.md` | Rewritten against the Arrow columnar API. Real `StreamFunction` signature, real window constructors, real DLQ configuration. The fabricated testing module is replaced with the collecting-sink pattern the repo's own tests use. Troubleshooting entries now cover errors readers will actually hit |
| `ARCHITECTURE.md` | Control plane marked **PLANNED** in prose and diagram, with an explicit note that `openraft`/`tonic` are not dependencies; cold path corrected to `block_on` and described as blocking; L1 tier corrected; key-group addressing documented; latency figures labelled as expectations, not measurements |
| `ROADMAP.md` | `#[rhei::main]` → `#[rhei::pipeline]`; `rhei run --attach` → `rhei attach`; `Executor::builder` → `PipelineController::builder`; `KeyedStream` moved from done to an open item; KI-7 split into the resolved and unresolved halves |
| `KNOWN-ISSUES.md` | KI-7 rewritten to match the code (clean entries evicted, dirty entries unbounded); KI-8 API name corrected; KI-11 retitled to state the blocking cold path directly, since other docs now reference it |
| `CLAUDE.md` | Notes that nextest cannot run doctests; documents the packages outside the workspace; adds a Documentation Accuracy section stating the rules future changes must follow |

### Made verifiable

Correcting the docs once does not stop them drifting again. Four mechanisms now
close the loop:

1. **Markdown is compiled.** `rhei/src/lib.rs` includes `README.md`, `API.md`,
   and `docs/getting-started.md` under `#[cfg(doctest)]`, so every Rust block in
   them is a real doctest. Twenty-four blocks compile today. A rename that
   breaks a documented API now breaks the build.

2. **`cargo test --doc --workspace` runs in CI**, in a new `Documentation` job.
   This was the missing step: nextest silently skips doctests, so the workspace
   had no job that compiled documentation at all.

3. **Opting out requires a reason.** `scripts/check-doc-blocks.py` fails if a
   ```` ```rust ```` block is marked `ignore` without a `not-compiled: <reason>`
   comment. Six blocks are excluded today — all needing Kafka, `librdkafka`, or
   showing deliberately-rejected designs — and each carries its justification
   and a pointer to the compiled equivalent.

4. **The quick start cannot drift.** The README quick start must stay
   byte-identical to the anchored region of `rhei/examples/quickstart.rs`, which
   `cargo check --workspace --all-targets` compiles and `cargo run -p rhei
   --example quickstart` runs. The same script enforces the match.

Run all of it with `just docs`.

---

## Assessment

**Documentation trustworthiness: 9/10.**

What that buys: a reader can copy any Rust sample from the three user-facing docs
and it will compile against the current tree, because CI proves it on every push.
Claims about compile-time guarantees, delivery semantics, blocking behaviour, and
the control plane now match the code, and the two places where docs contradicted
each other have been reconciled against the source.

It is not 10/10, and claiming otherwise in this document would be the same
failure the audit is about. The residual gap:

- **Prose is not machine-checkable.** Compilation proves the samples build. It
  does not prove that "delivery is at-least-once" or "key groups are fixed by
  `max_parallelism`" remains true after a refactor. Those depend on review
  discipline, now written into `CLAUDE.md`.
- **Six blocks remain uncompiled.** They are justified and point at compiled
  equivalents, but the Kafka snippets in particular are only verified by
  inspection against `rhei-runtime/examples/kafka_transform.rs`. Running
  `cargo test --doc --features kafka` in the E2E workflow, which already has a
  broker and `librdkafka`, would close this.
- **Not every document is wired in.** `CLUSTERING.md`, `INPROC-ARCH.md`,
  `PLAN.md`, the 27 ADRs, and the `docs/` design notes were reviewed for the
  contradictions reported above but are not compiled, and several are dated
  design records rather than current descriptions. Marking each with its status
  and the commit it describes is the obvious next step.
- **Numbers are still unmeasured.** Latency tiers and TUI figures are now
  labelled as illustrative rather than presented as measurements. Replacing them
  with output from the existing criterion benches would be strictly better than
  labelling them.

The honest summary: the mechanism that lets documentation rot silently is gone,
and the accumulated rot has been cleared. Keeping the score requires that the
`Documentation` CI job stay mandatory and that resolved issues update
`ROADMAP.md` and `KNOWN-ISSUES.md` together.
