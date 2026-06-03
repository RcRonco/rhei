# Python Bindings L0 — Erased-Builder Seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote a small, documented `pub` "erased-builder" API on `rhei-runtime` so a dataflow graph can be built and run entirely through type-erased `ErasedBuffer` nodes (boxed closures), with zero `RheiSchema`-typed code — the foundation the Python bindings will target.

**Architecture:** `rhei-runtime` already lowers the generic `Stream<T>` API to a runtime-typed layer (`ErasedBuffer` + `ErasedSource`/`ErasedSink`/`ErasedBatchOperator` + `BatchTransformFn`) before execution. Today that layer is `pub(crate)`. This plan promotes a minimal subset to `pub`, adds a runtime constructor for `ErasedBuffer` (no compile-time `T`), and adds `pub` builder methods on `DataflowGraph` that wrap pre-erased sources/transforms/sinks/key_by/merge into the existing `NodeKind` variants. No executor, compiler, or checkpoint code changes. The proof is an integration test (in the external `tests/` crate, so it can only touch `pub` items) that builds a source→transform→sink graph through the new API and runs it on `PipelineController`.

**Tech Stack:** Rust (edition 2024), Arrow 54 (`arrow`, `arrow-array`, `arrow-schema`), `seahash`, `cargo nextest`. Workspace lints: `unsafe_code = "forbid"`, clippy `all`=deny / `pedantic`=warn, `#![warn(missing_docs)]` on `rhei-runtime` (every new `pub` item MUST have a doc comment or clippy `-D warnings` fails).

**Branch:** `ronco/python-bindings` (already checked out; the spec + ADR live there).

---

## Background: exact current state (verified)

- `rhei-runtime/src/erased_buffer.rs`
  - `ErasedBuffer { batch: RecordBatch, mask: Option<BooleanArray>, schema_id: u64, exchange_target: Option<u64> }` — all fields private.
  - Constructed only via `pub fn from_typed<T: RheiSchema>(buffer: RheiBuffer<T>) -> Self`.
  - `schema_id` is computed by the private `fn schema_hash<T: RheiSchema>() -> u64` (line ~230) as `seahash::hash(format!("{:?}", T::arrow_schema()).as_bytes())`.
  - `pub(crate) type KeyFn = Arc<dyn Fn(&RecordBatch, usize) -> String + Send + Sync>;` (line ~21).
  - Already has `pub fn as_record_batch(&self) -> &RecordBatch`, `pub fn num_rows(&self) -> usize`, `pub fn schema_id(&self) -> u64`.
- `rhei-runtime/src/erased_batch.rs`
  - `pub(crate) mod` (declared in `lib.rs` line ~27 as `pub(crate) mod erased_batch;`), file starts with `#![allow(dead_code)]`.
  - `pub(crate) trait ErasedSource: Send` (line ~17), `pub(crate) trait ErasedSink: Send` (line ~123), `pub(crate) trait ErasedBatchOperator: Send` (line ~152).
- `rhei-runtime/src/dataflow.rs`
  - `pub(crate) type BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>;` (line ~61).
  - `pub(crate) enum NodeKind { Source(Box<dyn SourceNode>), Transform(LazyBatchTransformNode), BatchOperator { name, op }, Sink(Box<dyn SinkNode>), KeyBy(LazyKeyByNode), Merge }`.
  - `pub(crate) struct NodeId(pub(crate) usize)`.
  - `pub(crate) fn add_node(&self, kind: NodeKind, inputs: Vec<NodeId>) -> NodeId`.
  - `pub(crate) trait SourceNode { fn compile(self: Box<Self>) -> Box<dyn ErasedSource>; }` and same shape for `SinkNode`/`BatchOperatorNode`.
  - `LazyBatchTransformNode(pub(crate) Box<dyn FnOnce() -> BatchTransformFn + Send>)`, `LazyKeyByNode(pub(crate) Box<dyn FnOnce() -> KeyFn + Send>)`.
- `rhei-runtime/src/controller.rs` — `PipelineController::new(dir).with_workers(n)` then `ctrl.run(graph).await` is the canonical run path. `run()` calls `graph.validate()` then `run_graph` → `compile_graph(graph.into_nodes())`.
- Run/test idiom (from `tests/convenience.rs`): `let checkpoint_dir = tempfile::tempdir().unwrap();` … `let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1); ctrl.run(graph).await.unwrap();`

## File structure (this plan)

- Modify: `rhei-runtime/src/erased_buffer.rs` — add `pub fn schema_hash_of`, `pub fn from_parts`, promote `KeyFn` to `pub`, refactor `schema_hash<T>` to call `schema_hash_of`.
- Modify: `rhei-runtime/src/erased_batch.rs` — promote `ErasedSource`, `ErasedSink` to `pub trait`.
- Modify: `rhei-runtime/src/lib.rs` — `pub(crate) mod erased_batch;` → `pub mod erased_batch;`.
- Modify: `rhei-runtime/src/dataflow.rs` — promote `BatchTransformFn` to `pub`; add `pub struct ErasedHandle`; add crate-private adapter node structs (`PreErasedSourceNode`, `PreErasedSinkNode`); add `pub` builder methods (`add_erased_source`, `add_erased_transform`, `add_erased_sink`, `add_key_by`, `add_merge`).
- Create: `rhei-runtime/tests/erased_builder.rs` — end-to-end proof test.

No new files in `rhei-core`. No executor/compiler/checkpoint changes.

---

### Task 1: `schema_hash_of` and `ErasedBuffer::from_parts`

**Files:**
- Modify: `rhei-runtime/src/erased_buffer.rs`

- [ ] **Step 1: Write the failing unit tests**

Add these two tests inside the existing `#[cfg(test)] mod tests { ... }` block in `rhei-runtime/src/erased_buffer.rs` (the block already has the `#[allow(...)]` attributes covering `unwrap`/`expect`). Place them after the existing `roundtrip_typed_to_erased_and_back` test:

```rust
    #[test]
    fn schema_hash_of_matches_typed_hash() {
        // schema_hash_of(&T::arrow_schema()) must equal the id produced by from_typed::<T>.
        let mut builder = TestRow::builder(1);
        builder.append(TestRow {
            id: 7,
            name: "z".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let computed = super::schema_hash_of(&TestRow::arrow_schema());
        assert_eq!(erased.schema_id(), computed);
    }

    #[test]
    fn from_parts_roundtrips_via_downcast() {
        // A buffer constructed from raw parts (no compile-time T) must downcast
        // back to T when the schema_id is computed from T's schema.
        let schema = TestRow::arrow_schema();
        let id_array = arrow_array::Int64Array::from(vec![11_i64, 22]);
        let name_array = arrow_array::StringArray::from(vec!["p", "q"]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let schema_id = super::schema_hash_of(&schema);
        let erased = ErasedBuffer::from_parts(batch, None, schema_id);
        assert_eq!(erased.num_rows(), 2);

        let typed: RheiBuffer<TestRow> = erased.downcast().unwrap();
        assert_eq!(typed.len(), 2);
        let v = TestRow::view(typed.as_record_batch(), 1);
        assert_eq!(v.id, 22);
        assert_eq!(v.name, "q");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo nextest run -p rhei-runtime -E 'test(schema_hash_of_matches_typed_hash) + test(from_parts_roundtrips_via_downcast)'`
Expected: FAIL — compile error `no function or associated item named 'schema_hash_of' / 'from_parts'`.

- [ ] **Step 3: Add `schema_hash_of` and refactor `schema_hash`**

In `rhei-runtime/src/erased_buffer.rs`, replace the existing private `schema_hash` function (around line 230):

```rust
/// Compute a stable hash for a `RheiSchema` type based on its Arrow schema.
fn schema_hash<T: RheiSchema>() -> u64 {
    let schema = T::arrow_schema();
    let schema_str = format!("{schema:?}");
    seahash::hash(schema_str.as_bytes())
}
```

with:

```rust
/// Compute the stable schema id for an Arrow [`Schema`].
///
/// This is the same hash [`ErasedBuffer::from_typed`] derives from a
/// `RheiSchema` type, exposed for callers that only have a runtime
/// [`Schema`] (e.g. dynamically-typed graph builders). A buffer built via
/// [`ErasedBuffer::from_parts`] with `schema_hash_of(&schema)` will
/// [`downcast`](ErasedBuffer::downcast) to any `T` whose
/// `arrow_schema()` equals `schema`.
pub fn schema_hash_of(schema: &Schema) -> u64 {
    let schema_str = format!("{schema:?}");
    seahash::hash(schema_str.as_bytes())
}

/// Compute a stable hash for a `RheiSchema` type based on its Arrow schema.
fn schema_hash<T: RheiSchema>() -> u64 {
    schema_hash_of(&T::arrow_schema())
}
```

- [ ] **Step 4: Add `ErasedBuffer::from_parts`**

In the `impl ErasedBuffer { ... }` block, immediately after the `from_typed` method (around line 61), add:

```rust
    /// Construct an `ErasedBuffer` directly from its parts, without a
    /// compile-time `RheiSchema` type.
    ///
    /// `schema_id` must be computed with [`schema_hash_of`] from an Arrow
    /// [`Schema`] equal to `batch.schema()` for the buffer to be
    /// [`downcast`](Self::downcast)-able to a typed `RheiBuffer<T>`. This is
    /// the runtime-typed entry point used by dynamic graph builders.
    pub fn from_parts(batch: RecordBatch, mask: Option<BooleanArray>, schema_id: u64) -> Self {
        Self {
            batch,
            mask,
            schema_id,
            exchange_target: None,
        }
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo nextest run -p rhei-runtime -E 'test(schema_hash_of_matches_typed_hash) + test(from_parts_roundtrips_via_downcast)'`
Expected: PASS (2 tests).

- [ ] **Step 6: Verify the whole crate still compiles and lints clean**

Run: `cargo clippy -p rhei-runtime --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors. (Confirms the new `pub fn`s have adequate docs for `missing_docs`.)

- [ ] **Step 7: Commit**

```bash
git add rhei-runtime/src/erased_buffer.rs
git commit -m "feat(runtime): add schema_hash_of and ErasedBuffer::from_parts

Runtime-typed entry points for building ErasedBuffers without a
compile-time RheiSchema, for the Python bindings erased-builder seam.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Promote `ErasedSource` / `ErasedSink` and the `erased_batch` module to `pub`

**Files:**
- Modify: `rhei-runtime/src/lib.rs:27`
- Modify: `rhei-runtime/src/erased_batch.rs`
- Modify: `rhei-runtime/src/erased_buffer.rs` (promote `KeyFn`)
- Modify: `rhei-runtime/src/dataflow.rs` (promote `BatchTransformFn`)

No test in this task — it is pure visibility promotion. The proof is that the crate compiles clean with `-D warnings` (missing_docs already satisfied: the traits and their methods already carry doc comments) and that nothing downstream broke.

- [ ] **Step 1: Make the module public**

In `rhei-runtime/src/lib.rs`, change line ~27 from:

```rust
/// Type-erased batch operator/source/sink wrappers for Arrow columnar execution.
pub(crate) mod erased_batch;
```

to:

```rust
/// Type-erased batch operator/source/sink wrappers for Arrow columnar execution.
pub mod erased_batch;
```

- [ ] **Step 2: Promote the two traits**

In `rhei-runtime/src/erased_batch.rs`:

- Change `pub(crate) trait ErasedSource: Send {` (line ~17) to `pub trait ErasedSource: Send {`.
- Change `pub(crate) trait ErasedSink: Send {` (line ~123) to `pub trait ErasedSink: Send {`.

Leave `ErasedBatchOperator` as `pub(crate)` (no consumer until the L4 custom-operator plan) and leave the wrapper structs (`SourceWrapper`, `SinkWrapper`, `BatchOperatorWrapper`, `DynSourceWrapper`) as `pub(crate)`. Leave the `#![allow(dead_code)]` at the top of the file unchanged.

- [ ] **Step 3: Promote `KeyFn`**

In `rhei-runtime/src/erased_buffer.rs`, change line ~21 from:

```rust
/// Type-erased key extraction function: given a `RecordBatch` and row index,
/// returns the key string for that row. Used by `key_by` exchange.
pub(crate) type KeyFn = Arc<dyn Fn(&RecordBatch, usize) -> String + Send + Sync>;
```

to:

```rust
/// Type-erased key extraction function: given a `RecordBatch` and row index,
/// returns the key string for that row. Used by `key_by` exchange.
pub type KeyFn = Arc<dyn Fn(&RecordBatch, usize) -> String + Send + Sync>;
```

- [ ] **Step 4: Promote `BatchTransformFn`**

In `rhei-runtime/src/dataflow.rs`, change line ~61 from:

```rust
/// A batch-level transform: `ErasedBuffer` → `Vec<ErasedBuffer>`.
pub(crate) type BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>;
```

to:

```rust
/// A batch-level transform: `ErasedBuffer` → `Vec<ErasedBuffer>`.
pub type BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>;
```

- [ ] **Step 5: Verify the whole workspace compiles and lints clean**

Run: `cargo clippy --workspace --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors.

If `missing_docs` fires on any newly-reachable item, add a `///` doc comment to that item describing its role, then re-run. (The two traits, all their methods, `KeyFn`, and `BatchTransformFn` already have doc comments, so none is expected.)

- [ ] **Step 6: Run the full runtime test suite to confirm no regressions**

Run: `cargo nextest run -p rhei-runtime`
Expected: PASS (all existing tests still green).

- [ ] **Step 7: Commit**

```bash
git add rhei-runtime/src/lib.rs rhei-runtime/src/erased_batch.rs rhei-runtime/src/erased_buffer.rs rhei-runtime/src/dataflow.rs
git commit -m "feat(runtime): expose ErasedSource/ErasedSink, KeyFn, BatchTransformFn

Promote the type-erased execution traits and function aliases to pub so
external crates (rhei-python) can build dataflow graphs through them.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `ErasedHandle` + `add_erased_source` / `add_erased_transform` / `add_erased_sink`

**Files:**
- Modify: `rhei-runtime/src/dataflow.rs`

These are the core builder methods. They wrap a pre-erased `Box<dyn ErasedSource>` / `BatchTransformFn` / `Box<dyn ErasedSink>` into the existing `NodeKind` variants via tiny adapter node types. The test that proves them lives in Task 5 (it must be an external `tests/` crate to prove `pub`-only reachability).

- [ ] **Step 1: Add the adapter node structs**

In `rhei-runtime/src/dataflow.rs`, after the `TypedSinkNode` impl block (around line 100, just before `/// Deferred batch transform:`), add:

```rust
// ── Pre-erased node adapters (for the public erased-builder API) ─────

/// Wraps an already-erased [`ErasedSource`] so it slots into a `Source` node.
struct PreErasedSourceNode(Box<dyn ErasedSource>);

impl SourceNode for PreErasedSourceNode {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSource> {
        self.0
    }
}

/// Wraps an already-erased [`ErasedSink`] so it slots into a `Sink` node.
struct PreErasedSinkNode(Box<dyn ErasedSink>);

impl SinkNode for PreErasedSinkNode {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSink> {
        self.0
    }
}
```

- [ ] **Step 2: Add the `ErasedHandle` type**

In `rhei-runtime/src/dataflow.rs`, after the `NodeId` definition (around line 41), add:

```rust
/// Opaque handle to a node when building a graph through the public
/// erased-builder API ([`DataflowGraph::add_erased_source`] and friends).
///
/// Unlike [`Stream<T>`], an `ErasedHandle` carries no compile-time schema
/// type — the buffers flowing through it are runtime-typed [`ErasedBuffer`]s.
#[derive(Debug, Clone, Copy)]
pub struct ErasedHandle(NodeId);
```

- [ ] **Step 3: Add the three builder methods**

In `rhei-runtime/src/dataflow.rs`, inside `impl DataflowGraph { ... }`, after the `into_nodes` method (around line 221), add:

```rust
    /// Add a pre-erased data source. Returns an [`ErasedHandle`].
    ///
    /// The public, schema-erased counterpart to [`source`](Self::source):
    /// the caller supplies a `Box<dyn ErasedSource>` directly instead of a
    /// typed `Source`.
    pub fn add_erased_source(&self, source: Box<dyn ErasedSource>) -> ErasedHandle {
        let id = self.add_node(NodeKind::Source(Box::new(PreErasedSourceNode(source))), vec![]);
        ErasedHandle(id)
    }

    /// Add a stateless batch transform (`ErasedBuffer` → `Vec<ErasedBuffer>`)
    /// downstream of `input`. Returns the new [`ErasedHandle`].
    pub fn add_erased_transform(
        &self,
        input: ErasedHandle,
        transform: BatchTransformFn,
    ) -> ErasedHandle {
        let node = LazyBatchTransformNode(Box::new(move || transform));
        let id = self.add_node(NodeKind::Transform(node), vec![input.0]);
        ErasedHandle(id)
    }

    /// Add a pre-erased sink consuming the stream at `input` (terminal).
    pub fn add_erased_sink(&self, input: ErasedHandle, sink: Box<dyn ErasedSink>) {
        self.add_node(
            NodeKind::Sink(Box::new(PreErasedSinkNode(sink))),
            vec![input.0],
        );
    }
```

- [ ] **Step 4: Add the imports the new code needs**

At the top of `rhei-runtime/src/dataflow.rs`, the existing `use crate::erased_batch::{...}` already imports `ErasedSink` and `ErasedSource` (verify line ~32). No new import is required — `BatchTransformFn`, `LazyBatchTransformNode`, `NodeKind`, `NodeId` are all defined in this file. If the build reports `ErasedSink`/`ErasedSource` unused-before, that is expected to resolve once they are referenced by the new methods.

- [ ] **Step 5: Verify it compiles and lints clean**

Run: `cargo clippy -p rhei-runtime --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors.

- [ ] **Step 6: Commit**

```bash
git add rhei-runtime/src/dataflow.rs
git commit -m "feat(runtime): add erased-builder source/transform/sink methods

ErasedHandle + DataflowGraph::add_erased_source/add_erased_transform/
add_erased_sink wrap pre-erased nodes into the existing NodeKind variants.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: `add_key_by` and `add_merge`

**Files:**
- Modify: `rhei-runtime/src/dataflow.rs`

Rounds out the erased-builder seam with the two topology operations L2 will need. Trivial wrappers over `NodeKind::KeyBy` / `NodeKind::Merge`.

- [ ] **Step 1: Add the two builder methods**

In `rhei-runtime/src/dataflow.rs`, inside `impl DataflowGraph { ... }`, immediately after `add_erased_sink` (from Task 3), add:

```rust
    /// Add a key-based exchange downstream of `input`: partitions rows by the
    /// hash of `key_fn(batch, row)` and routes them so equal keys land on the
    /// same worker. Returns the new [`ErasedHandle`].
    pub fn add_key_by(&self, input: ErasedHandle, key_fn: KeyFn) -> ErasedHandle {
        let node = LazyKeyByNode(Box::new(move || key_fn));
        let id = self.add_node(NodeKind::KeyBy(node), vec![input.0]);
        ErasedHandle(id)
    }

    /// Merge two erased streams of the same schema into one. Returns the new
    /// [`ErasedHandle`].
    pub fn add_merge(&self, a: ErasedHandle, b: ErasedHandle) -> ErasedHandle {
        let id = self.add_node(NodeKind::Merge, vec![a.0, b.0]);
        ErasedHandle(id)
    }
```

- [ ] **Step 2: Add the `KeyFn` import**

At the top of `rhei-runtime/src/dataflow.rs`, the existing import is `use crate::erased_buffer::{ErasedBuffer, KeyFn};` (verify line ~35) — `KeyFn` is already imported. `LazyKeyByNode` is defined in this file. No new import needed.

- [ ] **Step 3: Verify it compiles and lints clean**

Run: `cargo clippy -p rhei-runtime --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors.

- [ ] **Step 4: Commit**

```bash
git add rhei-runtime/src/dataflow.rs
git commit -m "feat(runtime): add erased-builder key_by/merge methods

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: End-to-end proof test — build & run an erased-only graph

**Files:**
- Create: `rhei-runtime/tests/erased_builder.rs`

This is the architecture proof. It lives in the external `tests/` crate, so it can use **only `pub` items** — if it compiles and passes, the L0 surface is provably sufficient for the Python bindings. It builds `source → transform → sink` purely through `ErasedBuffer`/`ErasedHandle` (no `RheiSchema`, no `Stream<T>`), runs it on `PipelineController`, and asserts the sink received the transformed rows.

- [ ] **Step 1: Write the failing test file**

Create `rhei-runtime/tests/erased_builder.rs` with exactly:

```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Proof that a dataflow graph can be built and run entirely through the
//! public erased-builder API — no `RheiSchema`-typed code at all. This is the
//! foundation the Python bindings target.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::{BatchTransformFn, DataflowGraph};
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

/// A single-Int64-column schema: `{ n: Int64 }`.
fn n_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

/// Build a one-column `ErasedBuffer` from a slice of i64s.
fn erased_from(values: &[i64]) -> ErasedBuffer {
    let schema = n_schema();
    let col = Int64Array::from(values.to_vec());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
    ErasedBuffer::from_parts(batch, None, schema_hash_of(&schema))
}

/// Read the single Int64 column out of an `ErasedBuffer`.
fn values_of(buf: &ErasedBuffer) -> Vec<i64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    let batch = buf.as_record_batch();
    let col = batch.column(0).as_primitive::<Int64Type>();
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// A source that emits a fixed list of batches, then `None`.
struct ListSource {
    batches: std::collections::VecDeque<ErasedBuffer>,
}

#[async_trait]
impl ErasedSource for ListSource {
    async fn next_batch(&mut self) -> Option<ErasedBuffer> {
        self.batches.pop_front()
    }
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
    async fn restore_offsets(
        &mut self,
        _offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn partition_count(&self) -> Option<usize> {
        None
    }
    fn create_partition_source(&self, _assigned: &[usize]) -> Option<Box<dyn ErasedSource>> {
        None
    }
    fn current_watermark(&self) -> Option<u64> {
        None
    }
}

/// A sink that collects every batch's values into a shared Vec.
struct CollectSink {
    out: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl ErasedSink for CollectSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        self.out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(values_of(&buf));
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn erased_source_transform_sink_runs() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    // Source: two batches of i64s.
    let source = ListSource {
        batches: std::collections::VecDeque::from(vec![
            erased_from(&[1, 2, 3]),
            erased_from(&[4, 5]),
        ]),
    };

    // Transform: double every value. Rebuilds the buffer with the same schema id.
    let schema = n_schema();
    let schema_id = schema_hash_of(&schema);
    let transform: BatchTransformFn = Arc::new(move |buf: ErasedBuffer| {
        let doubled: Vec<i64> = values_of(&buf).into_iter().map(|v| v * 2).collect();
        let col = Int64Array::from(doubled);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
        vec![ErasedBuffer::from_parts(batch, None, schema_id)]
    });

    let sink = CollectSink {
        out: collected.clone(),
    };

    // Build the graph purely through the erased API.
    let graph = DataflowGraph::new();
    let src = graph.add_erased_source(Box::new(source));
    let mapped = graph.add_erased_transform(src, transform);
    graph.add_erased_sink(mapped, Box::new(sink));

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut got = collected
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    got.sort_unstable();
    assert_eq!(got, vec![2, 4, 6, 8, 10]);
}
```

- [ ] **Step 2: Run the test to verify it fails (or compiles-then-passes)**

Run: `cargo nextest run -p rhei-runtime -E 'test(erased_source_transform_sink_runs)'`
Expected before Tasks 1–4 are present: compile error (missing `schema_hash_of` / `from_parts` / `add_erased_*` / non-`pub` traits). After Tasks 1–4: this should now compile. If it compiles and PASSES on first run, that is the success condition — there is no separate "make it pass" implementation step because the production code was written in Tasks 1–4. If it FAILS on an assertion, debug using superpowers:systematic-debugging before proceeding.

- [ ] **Step 3: Confirm the test passes**

Run: `cargo nextest run -p rhei-runtime -E 'test(erased_source_transform_sink_runs)'`
Expected: PASS (1 test). This proves a source→transform→sink graph built entirely from `pub` erased primitives runs on the real executor.

- [ ] **Step 4: Lint the test**

Run: `cargo clippy -p rhei-runtime --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors.

- [ ] **Step 5: Commit**

```bash
git add rhei-runtime/tests/erased_builder.rs
git commit -m "test(runtime): prove erased-builder graph runs end-to-end

Builds source->transform->sink purely through the public ErasedBuffer/
ErasedHandle API (no RheiSchema, no Stream<T>) and runs it on
PipelineController. The L0 architecture proof for the Python bindings.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Final verification & fmt

**Files:** none (verification only)

- [ ] **Step 1: Format the workspace**

Run: `cargo fmt --all`
Then verify: `cargo fmt --all -- --check`
Expected: clean (no diff).

- [ ] **Step 2: Full workspace check**

Run: `cargo check --workspace --all-targets`
Expected: success.

- [ ] **Step 3: Full clippy gate (matches CI)**

Run: `cargo clippy --workspace --all-targets --no-deps -- -D warnings`
Expected: no warnings, no errors.

- [ ] **Step 4: Full test suite**

Run: `cargo nextest run --workspace`
Expected: all PASS (existing tests + the 2 new unit tests + the 1 new integration test).

- [ ] **Step 5: Commit any fmt changes**

```bash
git add -A
git commit -m "style: cargo fmt after L0 erased-builder seam" || echo "nothing to format"
```

---

## Self-Review

**Spec coverage (against the L0 row of the design spec + ADR):**
- "Promote `pub` erased-builder API on `DataflowGraph` (`add_erased_source`, `add_erased_transform`, `add_erased_operator`, `add_key_by`, `add_merge`, `add_erased_sink`)" — Tasks 3 & 4 cover source/transform/sink/key_by/merge. **`add_erased_operator` is intentionally deferred** to the L4 custom-operator plan: it requires `ErasedBatchOperator` to be `pub` *and* the `OperatorContext` state-handle refactor that only the Python operator wrapper needs, and there is no consumer of it in L0/L1/L2/L3. Building it now would be speculative (YAGNI). Noted here so the deferral is explicit, not a gap.
- "`schema_hash_of`" — Task 1. ✓
- "construct an `ErasedBuffer` from an imported `RecordBatch` + explicit `schema_id`" — Task 1 (`from_parts`). ✓
- "`OperatorContext` state-handle refactor" — deferred to L4 (only needed for Python custom operators / state access; not exercised by L0/L1). ✓ (explicit deferral)
- "L0 test: an erased-only graph runs and matches a typed-API equivalent" — Task 5. ✓

**Placeholder scan:** No TBD/TODO. Every code step shows complete code. Commands are exact with expected output.

**Type consistency:** `ErasedHandle` wraps `NodeId` and is returned by `add_erased_source`/`add_erased_transform`/`add_key_by`/`add_merge`, consumed by `add_erased_transform`/`add_erased_sink`/`add_key_by`/`add_merge` — consistent across Tasks 3–5. `schema_hash_of(&Schema) -> u64` and `ErasedBuffer::from_parts(RecordBatch, Option<BooleanArray>, u64)` signatures match between Task 1 (definition) and Task 5 (usage). `BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>` matches its use in Task 5. The `ErasedSource`/`ErasedSink` method sets implemented in Task 5 match the trait definitions in `erased_batch.rs` (verified against the current source: 7 source methods, 2 sink methods).

**Scope:** L0 only. L1 (PyO3 `rhei-python` crate) is a separate plan whose first task resolves exact `pyo3` / `arrow` (`pyarrow` feature) / `maturin` versions against the registry — those cannot be pinned accurately in advance and must not be guessed here.

---

## Next plan (not in scope here)

**L1 — Buffer FFI + thinnest Python pipeline.** Create the `rhei-python` crate (PyO3 + maturin), `PyBuffer` with zero-copy `__arrow_c_array__`/`from_arrow`, `PyDataflow` lowering to the L0 erased-builder API, a Python list source → `map_batches` → print/collect sink, and a blocking `run(workers=1)`. First task of that plan: pin `pyo3` + `arrow`'s `pyarrow` feature + `maturin` versions and stand up a hello-world extension that imports in Python.
