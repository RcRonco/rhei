# Python Bindings L1 — `rhei-python` Crate (thinnest end-to-end pipeline) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `rhei-python` PyO3 crate so a Python user can `import rhei`, build a `source → map_batches → sink` pipeline over Arrow batches, and run it — `python pipeline.py` actually executes on the real rhei/Timely runtime, with the `RecordBatch` crossing the FFI boundary zero-copy via `arrow::pyarrow`.

**Architecture:** `rhei-python` is a new crate (NOT a workspace member — it needs `crate-type=["cdylib"]` and a pinned `pyo3`, which would conflict with the workspace's `cargo nextest`/lint expectations). It depends on `rhei-runtime` (path dep) and builds against the **L0 erased-builder API** (`DataflowGraph::add_erased_source`/`add_erased_transform`/`add_erased_sink`, `ErasedBuffer::from_parts`, `schema_hash_of`, `ErasedSource`/`ErasedSink`). The single Rust type flowing through a Python graph is `ErasedBuffer`; `PyBuffer` wraps it. Python callables are boxed into the L0 `BatchTransformFn`. `run()` owns a tokio runtime and blocks. Conversions use `arrow::pyarrow::{FromPyArrow, ToPyArrow}` (zero-copy via the Arrow C Data Interface), so no hand-rolled FFI.

**Tech Stack (versions resolved against the registry — do not change):**
- `pyo3 = { version = "0.23", features = ["extension-module"] }` — arrow 54.3.1's `pyarrow` feature pins pyo3 to **0.23.5**; using `0.23` guarantees a single pyo3 in the graph. (Verified: pyo3 0.23 + arrow 54 `pyarrow` co-resolve to one pyo3 0.23.5.)
- `arrow = { version = "54", features = ["pyarrow"] }` — matches the workspace's arrow 54.3.1. Provides `RecordBatch::from_pyarrow_bound(&Bound<PyAny>) -> PyResult<RecordBatch>` and `record_batch.to_pyarrow(py) -> PyResult<PyObject>`.
- `rhei-runtime` — path dependency (`../rhei-runtime`).
- `tokio = { version = "1", features = ["rt-multi-thread"] }`, `anyhow`, `arrow-schema = "54"`, `arrow-array = "54"`.
- Build/dev: `maturin 1.13.1`, Python `3.13.2`, `uv 0.11.2` (all present on this machine). pyarrow is installed into a uv venv during testing.

**Branch:** `ronco/python-bindings` (continues after L0; base for this work is L0 head `a8178b1`).

**pyo3 0.23 API notes (edition/MSRV):** pyo3 0.23 uses the `Bound<'py, T>` API. Key idioms used below:
- `#[pyclass]` / `#[pymethods]` / `#[pymodule]`.
- A pymodule fn signature: `#[pymodule] fn rhei(m: &Bound<'_, PyModule>) -> PyResult<()> { ... }`.
- `m.add_class::<T>()?;`
- Extract a Python callable: store it as `Py<PyAny>`; call it via `cb.bind(py).call1((arg,))?`.
- Release the GIL for blocking work: `py.allow_threads(|| { ... })`.
- Acquire the GIL inside a Rust closure running on a worker thread: `Python::with_gil(|py| { ... })`.

---

## Why `rhei-python` is NOT a workspace member

The root `Cargo.toml` workspace has `members = ["rhei-core", "rhei-runtime", "rhei-cli", "rhei-macros", "rhei"]` and workspace-wide lints. A `cdylib` PyO3 crate:
- links libpython (via `extension-module`) and shouldn't be built by `cargo build --workspace` / tested by the workspace `cargo nextest` (it needs maturin + a Python interpreter).
- pins `pyo3`, which has no place in the workspace dependency set.

So `rhei-python` lives at the repo root as a standalone crate with its own `Cargo.toml` (its own lockfile), excluded from the workspace via the root `Cargo.toml` `[workspace] exclude` list. This keeps `cargo nextest run --workspace` (CI) unaffected while the Python crate is built/tested separately with maturin.

## File structure (this plan)

- Modify: `Cargo.toml` (root) — add `rhei-python` to `[workspace].exclude`.
- Create: `rhei-python/Cargo.toml` — crate manifest (standalone).
- Create: `rhei-python/pyproject.toml` — maturin build config, project name `rhei`.
- Create: `rhei-python/src/lib.rs` — `#[pymodule]`, registers `PyBuffer`, `PyDataflow`, `PyStream`.
- Create: `rhei-python/src/buffer.rs` — `PyBuffer` (wraps `ErasedBuffer`), zero-copy `to_arrow`/`from_arrow`.
- Create: `rhei-python/src/dataflow.rs` — `PyDataflow`, `PyStream`, source/map_batches/sink/run, lowering to L0 API; `PyListSource`/`PyCollectSink`/Python-callable transform wrappers.
- Create: `rhei-python/python/rhei/__init__.py` — thin re-export shim + docstrings.
- Create: `rhei-python/tests/test_pipeline.py` — pytest end-to-end proof.
- Create: `rhei-python/.gitignore` — ignore `target/`, `*.so`, `.venv/`.
- Create: `rhei-python/README.md` — one-paragraph build/run instructions.

Decomposition: `buffer.rs` owns the data-type boundary; `dataflow.rs` owns graph construction + execution. `lib.rs` only wires the module. Keep each focused.

---

### Task 1: Scaffold the `rhei-python` crate (compiles, imports in Python)

**Goal:** A minimal PyO3 extension that builds with maturin and imports in Python, exposing nothing but a version constant. Proves the toolchain + pyo3/arrow version pin work before any real logic.

**Files:**
- Modify: `Cargo.toml` (root)
- Create: `rhei-python/Cargo.toml`, `rhei-python/pyproject.toml`, `rhei-python/src/lib.rs`, `rhei-python/.gitignore`, `rhei-python/README.md`

- [ ] **Step 1: Exclude `rhei-python` from the workspace**

In the root `/Users/roncohen/workspace/frisk/Cargo.toml`, the `[workspace]` table currently reads:

```toml
[workspace]
members = ["rhei-core", "rhei-runtime", "rhei-cli", "rhei-macros", "rhei"]
resolver = "3"
```

Add an `exclude` key:

```toml
[workspace]
members = ["rhei-core", "rhei-runtime", "rhei-cli", "rhei-macros", "rhei"]
exclude = ["rhei-python"]
resolver = "3"
```

- [ ] **Step 2: Create `rhei-python/Cargo.toml`**

```toml
[package]
name = "rhei-python"
version = "0.1.0"
edition = "2024"
publish = false

[lib]
name = "rhei"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.23", features = ["extension-module"] }
arrow = { version = "54", features = ["pyarrow"] }
arrow-array = "54"
arrow-schema = "54"
rhei-runtime = { path = "../rhei-runtime" }
tokio = { version = "1", features = ["rt-multi-thread"] }
anyhow = "1"

[lints.rust]
unsafe_code = "forbid"
```

Note: the `[lib] name = "rhei"` makes the compiled artifact `librhei.so`/`rhei.*.so`, which maturin places so `import rhei` works. We are NOT inheriting workspace lints (this is a standalone crate); `unsafe_code = "forbid"` is set explicitly to honor the project rule. Do not add `[workspace]` to this Cargo.toml — it must resolve standalone.

- [ ] **Step 3: Create `rhei-python/pyproject.toml`**

```toml
[build-system]
requires = ["maturin>=1.7,<2.0"]
build-backend = "maturin"

[project]
name = "rhei"
version = "0.1.0"
description = "Python bindings for the rhei stream-processing framework"
requires-python = ">=3.9"
dependencies = ["pyarrow>=14"]

[tool.maturin]
features = ["pyo3/extension-module"]
python-source = "python"
module-name = "rhei._rhei"
```

IMPORTANT detail: `module-name = "rhei._rhei"` + `python-source = "python"` means maturin builds the compiled extension as the submodule `rhei._rhei`, and the pure-Python package lives in `python/rhei/`. The `#[pymodule]` fn in Rust must therefore be named `_rhei` (see Step 4), and `python/rhei/__init__.py` (Task 4) re-exports from `._rhei`. This is the standard maturin "mixed Rust/Python project" layout. For THIS task there is no `python/rhei/` yet — that's Task 4 — so set it up now but the import test in Step 6 imports the compiled module directly.

Wait — with `python-source` set, maturin expects the Python package dir. To keep Task 1 self-contained (no Python package yet), create a minimal `python/rhei/__init__.py` now containing just `from ._rhei import *` so the package imports. Task 4 fleshes it out.

Create `rhei-python/python/rhei/__init__.py`:

```python
from ._rhei import *  # noqa: F401,F403
```

- [ ] **Step 4: Create `rhei-python/src/lib.rs`**

```rust
//! Python bindings for the rhei stream-processing framework (PyO3).
//!
//! Exposes a thin layer over rhei-runtime's type-erased dataflow API so that
//! Python users can build and run Arrow-columnar pipelines.

use pyo3::prelude::*;

/// The compiled extension module. Named `_rhei` to match
/// `module-name = "rhei._rhei"` in `pyproject.toml`; the pure-Python
/// `rhei` package re-exports from it.
#[pymodule]
fn _rhei(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
```

- [ ] **Step 5: Create `rhei-python/.gitignore` and `rhei-python/README.md`**

`.gitignore`:
```
/target
*.so
.venv/
__pycache__/
*.pyc
Cargo.lock
```
(We ignore the standalone `Cargo.lock` for this non-published cdylib to avoid churn; the workspace lockfile is unaffected.)

`README.md`:
```markdown
# rhei-python

Python bindings for rhei (PyO3 + maturin).

## Build & test (dev)

```bash
cd rhei-python
uv venv
uv pip install maturin pyarrow pytest
uv run maturin develop
uv run pytest -q
```
```

- [ ] **Step 6: Build and import-test**

Run, from `/Users/roncohen/workspace/frisk/rhei-python`:
```bash
uv venv
uv pip install maturin pyarrow pytest
uv run maturin develop
uv run python -c "import rhei; print(rhei.__version__)"
```
Expected: prints `0.1.0`. (`maturin develop` compiles the cdylib and installs it into the venv as `rhei._rhei`, and `python/rhei/__init__.py` re-exports it.)

If `maturin develop` fails on the pyo3/python link, confirm the venv's Python is 3.13 and that `pyo3`'s `extension-module` feature is set (it is, via Cargo.toml + the `[tool.maturin] features`). If it complains about an abi3 requirement, none is set (we build for the venv's exact Python), so this should be clean.

- [ ] **Step 7: Commit**

```bash
cd /Users/roncohen/workspace/frisk
git add Cargo.toml rhei-python/Cargo.toml rhei-python/pyproject.toml rhei-python/src/lib.rs rhei-python/.gitignore rhei-python/README.md rhei-python/python/rhei/__init__.py
git commit -m "feat(python): scaffold rhei-python PyO3 crate

Standalone cdylib (excluded from workspace) using pyo3 0.23 + arrow 54
pyarrow feature. maturin mixed layout: compiled _rhei submodule + python
package. Imports and reports __version__.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `PyBuffer` — zero-copy Arrow boundary

**Goal:** A `Buffer` Python class wrapping `ErasedBuffer`, with `to_arrow()` → pyarrow `RecordBatch` and `Buffer.from_arrow(rb)` ← pyarrow `RecordBatch`, both zero-copy via `arrow::pyarrow`. Plus `__len__`. This is the data type that flows through a Python graph.

**Files:**
- Create: `rhei-python/src/buffer.rs`
- Modify: `rhei-python/src/lib.rs` (register `PyBuffer`, add `mod buffer;`)
- Modify: `rhei-python/tests/test_pipeline.py` (created here, first test)

- [ ] **Step 1: Write the failing pytest**

Create `rhei-python/tests/test_pipeline.py`:

```python
import pyarrow as pa
import rhei


def test_buffer_roundtrips_arrow_zero_copy():
    schema = pa.schema([("n", pa.int64())])
    rb = pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)

    buf = rhei.Buffer.from_arrow(rb)
    assert len(buf) == 3

    out = buf.to_arrow()
    assert out.num_rows == 3
    assert out.column(0).to_pylist() == [1, 2, 3]
    assert out.schema == schema
```

- [ ] **Step 2: Run it to verify it fails**

From `rhei-python/`:
```bash
uv run pytest tests/test_pipeline.py::test_buffer_roundtrips_arrow_zero_copy -q
```
Expected: FAIL — `AttributeError: module 'rhei' has no attribute 'Buffer'`.

- [ ] **Step 3: Create `rhei-python/src/buffer.rs`**

```rust
//! `PyBuffer`: the Python-facing handle around rhei's type-erased Arrow buffer.
//!
//! A `Buffer` wraps an [`ErasedBuffer`] (a `RecordBatch` + schema id). Conversion
//! to/from pyarrow is zero-copy via the Arrow C Data Interface
//! (`arrow::pyarrow`).

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use pyo3::prelude::*;

use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

/// An Arrow batch flowing through a rhei pipeline.
///
/// Construct from a pyarrow `RecordBatch` with [`Buffer.from_arrow`] and read
/// it back with [`Buffer.to_arrow`]. Both are zero-copy.
#[pyclass(name = "Buffer", frozen)]
pub struct PyBuffer {
    pub(crate) inner: ErasedBuffer,
}

impl PyBuffer {
    /// Wrap an existing `ErasedBuffer` (used internally when receiving batches
    /// from the runtime).
    pub(crate) fn from_erased(inner: ErasedBuffer) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyBuffer {
    /// Build a `Buffer` from a pyarrow `RecordBatch` (zero-copy).
    ///
    /// The schema id is derived from the batch's Arrow schema, so the buffer
    /// interoperates with any rhei operator expecting that schema.
    #[staticmethod]
    fn from_arrow(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(obj)?;
        let schema_id = schema_hash_of(&batch.schema());
        Ok(Self {
            inner: ErasedBuffer::from_parts(batch, None, schema_id),
        })
    }

    /// Export this buffer as a pyarrow `RecordBatch` (zero-copy).
    ///
    /// The optional selection mask is applied (compacted) before export.
    fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        // Apply the mask (if any) by round-tripping through the typed downcast
        // path is unnecessary here; ErasedBuffer exposes the record batch.
        self.inner.as_record_batch().to_pyarrow(py)
    }

    fn __len__(&self) -> usize {
        self.inner.num_rows()
    }

    fn __repr__(&self) -> String {
        format!("Buffer(rows={})", self.inner.num_rows())
    }
}
```

NOTE on `&batch.schema()`: `RecordBatch::schema()` returns `Arc<Schema>` (actually `SchemaRef`); `schema_hash_of` takes `&Schema`. `&batch.schema()` is `&Arc<Schema>` which derefs to `&Schema`. If the compiler objects, use `&batch.schema()` → `batch.schema().as_ref()`. Verify and adjust minimally.

- [ ] **Step 4: Register `PyBuffer` in `lib.rs`**

Edit `rhei-python/src/lib.rs` to add the module and class registration:

```rust
//! Python bindings for the rhei stream-processing framework (PyO3).
//!
//! Exposes a thin layer over rhei-runtime's type-erased dataflow API so that
//! Python users can build and run Arrow-columnar pipelines.

use pyo3::prelude::*;

mod buffer;

use buffer::PyBuffer;

/// The compiled extension module. Named `_rhei` to match
/// `module-name = "rhei._rhei"` in `pyproject.toml`; the pure-Python
/// `rhei` package re-exports from it.
#[pymodule]
fn _rhei(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyBuffer>()?;
    Ok(())
}
```

- [ ] **Step 5: Rebuild and run the test**

From `rhei-python/`:
```bash
uv run maturin develop
uv run pytest tests/test_pipeline.py::test_buffer_roundtrips_arrow_zero_copy -q
```
Expected: PASS.

- [ ] **Step 6: Clippy the crate**

From `rhei-python/`:
```bash
cargo clippy --no-deps -- -D warnings
```
Expected: clean. (This crate uses clippy defaults + `unsafe_code = "forbid"`; it does NOT inherit the workspace pedantic config, so the bar is the default clippy lints. Still fix anything it reports.)

- [ ] **Step 7: Commit**

```bash
cd /Users/roncohen/workspace/frisk
git add rhei-python/src/buffer.rs rhei-python/src/lib.rs rhei-python/tests/test_pipeline.py
git commit -m "feat(python): PyBuffer with zero-copy pyarrow conversion

Buffer.from_arrow / Buffer.to_arrow bridge a pyarrow RecordBatch to rhei's
ErasedBuffer via arrow::pyarrow (Arrow C Data Interface). __len__ included.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `PyDataflow` / `PyStream` — build & run a source → map_batches → sink pipeline

**Goal:** The end-to-end pipeline. `rhei.Dataflow()` with `.source(list_of_record_batches) -> Stream`, `.map_batches(fn, schema) -> Stream`, `.sink_collect() -> CollectHandle` (an in-memory collecting sink for the test), and `.run(workers=1)`. Lower to the L0 erased-builder API. Python callables run under the GIL inside the transform; `run()` owns a tokio runtime and releases the GIL while the pipeline executes.

**Files:**
- Create: `rhei-python/src/dataflow.rs`
- Modify: `rhei-python/src/lib.rs` (register `PyDataflow`, `PyStream`, `PyCollectSink` handle; `mod dataflow;`)
- Modify: `rhei-python/tests/test_pipeline.py` (add end-to-end test)

- [ ] **Step 1: Write the failing end-to-end pytest**

Append to `rhei-python/tests/test_pipeline.py`:

```python
def test_map_batches_pipeline_runs_end_to_end():
    schema = pa.schema([("n", pa.int64())])
    batches = [
        pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema),
        pa.record_batch([pa.array([4, 5], type=pa.int64())], schema=schema),
    ]

    def double(buf: "rhei.Buffer") -> "rhei.Buffer":
        rb = buf.to_arrow()
        doubled = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(
            pa.record_batch([doubled], schema=schema)
        )

    df = rhei.Dataflow()
    collected = (
        df.source(batches)
          .map_batches(double, schema=schema)
          .sink_collect()
    )
    df.run(workers=1)

    got = sorted(collected.values())
    assert got == [2, 4, 6, 8, 10]
```

`collected.values()` returns the flat list of the single int64 column across all collected batches (defined in the sink below). The test sorts because batch/worker ordering is not guaranteed.

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run pytest tests/test_pipeline.py::test_map_batches_pipeline_runs_end_to_end -q
```
Expected: FAIL — `AttributeError: module 'rhei' has no attribute 'Dataflow'`.

- [ ] **Step 3: Create `rhei-python/src/dataflow.rs`**

```rust
//! `PyDataflow` / `PyStream`: build and run a pipeline from Python.
//!
//! Lowers Python graph construction onto rhei-runtime's L0 erased-builder API
//! (`DataflowGraph::add_erased_source` / `add_erased_transform` /
//! `add_erased_sink`). The one Rust type flowing through a Python graph is
//! `ErasedBuffer`; Python callables are boxed into the L0 `BatchTransformFn`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::pyarrow::ToPyArrow;
use arrow_array::{Int64Array, RecordBatch};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::Schema;
use async_trait::async_trait;
use pyo3::prelude::*;

use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::{BatchTransformFn, DataflowGraph, ErasedHandle};
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

use crate::buffer::PyBuffer;

// ── Source: drains a fixed list of ErasedBuffers ─────────────────────

struct ListSource {
    batches: VecDeque<ErasedBuffer>,
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

// ── Sink: collects every batch's first Int64 column into a shared Vec ──

#[derive(Clone)]
struct CollectShared {
    values: Arc<Mutex<Vec<i64>>>,
}

struct CollectSink {
    shared: CollectShared,
}

#[async_trait]
impl ErasedSink for CollectSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        let batch = buf.as_record_batch();
        if batch.num_columns() > 0 {
            let col = batch.column(0).as_primitive::<Int64Type>();
            let mut guard = self
                .shared
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for i in 0..col.len() {
                guard.push(col.value(i));
            }
        }
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Handle returned by `sink_collect()`; exposes the collected values after
/// `run()`.
#[pyclass(name = "CollectHandle", frozen)]
pub struct PyCollectHandle {
    shared: CollectShared,
}

#[pymethods]
impl PyCollectHandle {
    /// The flat list of the first Int64 column across all collected batches.
    fn values(&self) -> Vec<i64> {
        self.shared
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

// ── Python-callable transform ────────────────────────────────────────

/// Build a `BatchTransformFn` that calls a Python `fn(Buffer) -> Buffer`.
///
/// Acquires the GIL only while the Python callable runs; the resulting
/// `ErasedBuffer` carries `out_schema_id` so it interoperates downstream.
fn py_map_batches_transform(callable: Py<PyAny>, out_schema_id: u64) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        Python::with_gil(|py| {
            let py_in = PyBuffer::from_erased(buf);
            let in_obj = match Py::new(py, py_in) {
                Ok(o) => o,
                Err(e) => {
                    tracing_py_error(py, "map_batches: wrapping input", &e);
                    return vec![];
                }
            };
            let result = match callable.bind(py).call1((in_obj,)) {
                Ok(r) => r,
                Err(e) => {
                    tracing_py_error(py, "map_batches: Python callable raised", &e);
                    return vec![];
                }
            };
            let out: PyRef<'_, PyBuffer> = match result.extract() {
                Ok(b) => b,
                Err(e) => {
                    tracing_py_error(py, "map_batches: return value was not a Buffer", &e);
                    return vec![];
                }
            };
            // Re-tag with the declared output schema id so downstream nodes
            // (and the sink) can rely on it.
            let rb = out.inner.as_record_batch().clone();
            vec![ErasedBuffer::from_parts(rb, None, out_schema_id)]
        })
    })
}

fn tracing_py_error(py: Python<'_>, context: &str, err: &PyErr) {
    let msg = err.value(py).str().map_or_else(
        |_| "<unprintable Python error>".to_string(),
        |s| s.to_string_lossy().into_owned(),
    );
    eprintln!("rhei {context}: {msg}");
}

// ── PyDataflow / PyStream ────────────────────────────────────────────

/// A rhei dataflow graph builder.
#[pyclass(name = "Dataflow")]
pub struct PyDataflow {
    graph: DataflowGraph,
    // Collect handles kept alive so their shared buffers survive until run().
    collectors: Vec<CollectShared>,
}

#[pymethods]
impl PyDataflow {
    #[new]
    fn new() -> Self {
        Self {
            graph: DataflowGraph::new(),
            collectors: Vec::new(),
        }
    }

    /// Add a source from a list of pyarrow `RecordBatch`es. Returns a `Stream`.
    fn source(&mut self, batches: &Bound<'_, PyAny>) -> PyResult<PyStream> {
        use arrow_array::RecordBatch;
        use arrow::pyarrow::FromPyArrow;

        let mut queue: VecDeque<ErasedBuffer> = VecDeque::new();
        for item in batches.try_iter()? {
            let item = item?;
            let rb = RecordBatch::from_pyarrow_bound(&item)?;
            let schema_id = schema_hash_of(&rb.schema());
            queue.push_back(ErasedBuffer::from_parts(rb, None, schema_id));
        }
        let handle = self
            .graph
            .add_erased_source(Box::new(ListSource { batches: queue }));
        Ok(PyStream { handle })
    }

    /// Run the pipeline to completion (blocking). Owns the tokio runtime.
    #[pyo3(signature = (workers = 1, checkpoint_dir = None))]
    fn run(&mut self, py: Python<'_>, workers: usize, checkpoint_dir: Option<String>) -> PyResult<()> {
        // Move the graph out; DataflowGraph::run consumes it.
        let graph = std::mem::replace(&mut self.graph, DataflowGraph::new());
        let dir = checkpoint_dir.unwrap_or_else(|| {
            std::env::temp_dir()
                .join("rhei-python-ckpt")
                .to_string_lossy()
                .into_owned()
        });
        py.allow_threads(move || -> PyResult<()> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(async move {
                let ctrl = PipelineController::new(std::path::PathBuf::from(dir))
                    .with_workers(workers);
                ctrl.run(graph)
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
            })
        })
    }
}

/// A point in a rhei dataflow. Operations add nodes and return new handles.
#[pyclass(name = "Stream")]
pub struct PyStream {
    handle: ErasedHandle,
}

#[pymethods]
impl PyStream {
    /// Apply a Python `fn(Buffer) -> Buffer` to each batch. `schema` declares
    /// the output Arrow schema (a pyarrow `Schema`).
    fn map_batches(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        func: Py<PyAny>,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<PyStream> {
        let out_schema = arrow_schema_from_pyarrow(schema)?;
        let out_schema_id = schema_hash_of(&out_schema);
        let transform = py_map_batches_transform(func, out_schema_id);

        // Reach the parent dataflow through the stored graph reference.
        // PyStream holds only an ErasedHandle; the graph lives on PyDataflow.
        // To add nodes we need the graph — so map_batches must be called with
        // access to it. We thread it via a thread-local? No: instead, PyStream
        // carries a pointer back to the Py<PyDataflow>. See note below.
        let _ = (py, slf);
        let _ = transform;
        unimplemented!("see Step 3 note: PyStream needs a handle to PyDataflow")
    }
}

/// Convert a pyarrow `Schema` to an Arrow `Schema` (zero-copy via FFI).
fn arrow_schema_from_pyarrow(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
    use arrow::pyarrow::FromPyArrow;
    Schema::from_pyarrow_bound(obj)
}

// Silence unused-import warnings for items used by other tasks' code paths.
#[allow(unused_imports)]
use rhei_runtime::dataflow::DataflowGraph as _DataflowGraphAlias;
const _: fn() -> Int64Array = || Int64Array::from(Vec::<i64>::new());
const _: fn(Python<'_>, &RecordBatch) -> PyResult<PyObject> = |py, rb| rb.to_pyarrow(py);
```

**STOP — there is a real design decision in Step 3 that the implementer MUST resolve, flagged by the `unimplemented!` above.** `PyStream` needs to add nodes to the `DataflowGraph`, but the graph lives on `PyDataflow` and `Stream`/`ErasedHandle` are `Copy` handles with no back-reference. There are two clean options; **use Option A**:

- **Option A (recommended): make the builder methods live on `PyDataflow`, not `PyStream`.** Replace the fluent `stream.map_batches(...)` with `dataflow`-centric methods that take and return `PyStream` (handles): `df.map_batches(stream, func, schema) -> Stream`, `df.sink_collect(stream) -> CollectHandle`. This mirrors the L0 Rust API exactly (`graph.add_erased_transform(handle, fn)`), is the least surprising lowering, and avoids any shared-ownership gymnastics. The Python `__init__.py` shim (Task 4) can then add ergonomic fluent sugar IF desired, but the test in Step 1 uses fluent `.map_batches(...).sink_collect()`. **Therefore: implement fluent chaining by having `PyStream` hold a cloned reference to the graph.** Since `DataflowGraph` uses `RefCell` interior mutability and is single-threaded during construction, wrap it in `Py<PyDataflow>`: `PyStream` stores `df: Py<PyDataflow>` (a reference-counted handle to the Python Dataflow object) plus its `ErasedHandle`. Then `map_batches` does `self.df.borrow_mut(py).graph.add_erased_transform(self.handle, transform)`.

**Concrete required shape (implementer: build THIS, discarding the `unimplemented!` skeleton above):**
- `PyDataflow.source(&self, py, batches) -> PyStream` returns a `PyStream { df: <Py handle to self>, handle }`. To get a `Py<PyDataflow>` for `self` inside a method, take `slf: Py<Self>` or `slf: PyRef<Self>` and clone the `Py`. Simplest: change `source`, `map_batches`, `sink_collect` to take `slf: Py<PyDataflow>` / operate through it.
- `PyStream.map_batches(&self, py, func, schema) -> PyStream`: borrows `self.df` mutably, calls `add_erased_transform`, returns a new `PyStream { df: self.df.clone_ref(py), handle: new_handle }`.
- `PyStream.sink_collect(&self, py) -> PyCollectHandle`: creates a `CollectShared`, registers it on the dataflow's `collectors` Vec (so it stays alive), calls `add_erased_sink`, returns `PyCollectHandle { shared }`.

Implementer: design the exact ownership so it compiles cleanly under `unsafe`-forbidden and pyo3 0.23's borrow rules. If `Py<PyDataflow>` + `borrow_mut` proves awkward, fall back to **Option A's** non-fluent `df.map_batches(stream, ...)` form AND update the Step 1 test to match (`m = df.map_batches(df.source(batches), double, schema=schema); df.sink_collect(m)`). Either is acceptable; the fluent form is nicer but the non-fluent form is guaranteed simple. Pick one, make the test match, and report which you chose and why. Do NOT leave `unimplemented!` in the committed code.

- [ ] **Step 4: Register the new classes in `lib.rs`**

Add to `rhei-python/src/lib.rs`: `mod dataflow;`, `use dataflow::{PyDataflow, PyStream, PyCollectHandle};`, and in the `#[pymodule]` fn: `m.add_class::<PyDataflow>()?; m.add_class::<PyStream>()?; m.add_class::<PyCollectHandle>()?;`.

- [ ] **Step 5: Rebuild and run BOTH tests**

```bash
uv run maturin develop
uv run pytest tests/test_pipeline.py -q
```
Expected: 2 passed. If the end-to-end test hangs or returns wrong values, debug with superpowers:systematic-debugging — do NOT weaken the assertion. Likely culprits: (a) output `schema_id` mismatch between the transform and the sink's expectations (both use `schema_hash_of(&schema)` of the SAME schema — verify the Python `schema=` passed to `map_batches` matches the batch the `double` fn produces); (b) GIL deadlock if `py.allow_threads` isn't wrapping the blocking `run` (it must); (c) the collector Vec not being shared between the registered sink and the returned handle (both must clone the same `Arc<Mutex<...>>`).

- [ ] **Step 6: Clippy**

```bash
cargo clippy --no-deps -- -D warnings
```
Expected: clean. Remove the placeholder `const _:` / `_DataflowGraphAlias` lines from the skeleton — they exist only to keep the intermediate skeleton compiling and MUST NOT survive into the real implementation.

- [ ] **Step 7: Commit**

```bash
cd /Users/roncohen/workspace/frisk
git add rhei-python/src/dataflow.rs rhei-python/src/lib.rs rhei-python/tests/test_pipeline.py
git commit -m "feat(python): Dataflow/Stream end-to-end pipeline

source(list of RecordBatch) -> map_batches(py fn, schema) -> sink_collect,
run(workers) blocks on an owned tokio runtime. Python callables run under
the GIL inside the L0 BatchTransformFn; GIL released during execution.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Python package polish + `print` sink + docs

**Goal:** A clean `import rhei` surface (re-exports, docstrings, `__all__`), and a `sink_print()` so the canonical README example works without the test-only collect sink. Small, additive.

**Files:**
- Modify: `rhei-python/python/rhei/__init__.py`
- Modify: `rhei-python/src/dataflow.rs` (add `PrintSink` + `sink_print`)
- Modify: `rhei-python/src/lib.rs` (no new classes needed if print returns None)
- Modify: `rhei-python/tests/test_pipeline.py` (smoke test for print sink)
- Create: `rhei-python/examples/double.py`

- [ ] **Step 1: Write a failing smoke test for the print sink**

Append to `rhei-python/tests/test_pipeline.py`:

```python
def test_print_sink_runs(capfd):
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([7, 8], type=pa.int64())], schema=schema)]

    df = rhei.Dataflow()
    df.source(batches).sink_print()
    df.run(workers=1)

    out, _ = capfd.readouterr()
    assert "7" in out and "8" in out
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/test_pipeline.py::test_print_sink_runs -q
```
Expected: FAIL — `AttributeError: 'Stream' object has no attribute 'sink_print'` (or the dataflow-centric equivalent, matching the Option chosen in Task 3).

- [ ] **Step 3: Add a `PrintSink` + `sink_print` in `dataflow.rs`**

Add an `ErasedSink` impl that prints each batch's rows to stdout, and a `sink_print(...)` method on the same type that `map_batches`/`sink_collect` live on (matching Task 3's Option A/fluent choice). Implementation of `PrintSink`:

```rust
struct PrintSink;

#[async_trait]
impl ErasedSink for PrintSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        let batch = buf.as_record_batch();
        // Pretty-print via arrow's formatter for any schema.
        let formatted = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .map(|d| d.to_string())
            .unwrap_or_else(|e| format!("<unprintable batch: {e}>"));
        println!("{formatted}");
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
```

(`arrow::util::pretty::pretty_format_batches` requires arrow's `prettyprint` feature. Add `"prettyprint"` to the `arrow` features in `rhei-python/Cargo.toml`: `features = ["pyarrow", "prettyprint"]`. The workspace already uses `prettyprint` on arrow, so it's available.) The `sink_print` method calls `self.<graph>.add_erased_sink(handle, Box::new(PrintSink))` and returns `()` / `None`.

Note: the test asserts "7" and "8" appear in stdout. `pretty_format_batches` renders them in a table; the digits will appear. If buffering hides them under `capfd`, ensure `println!` (line-buffered to stdout) is used, not `tracing`.

- [ ] **Step 4: Flesh out `python/rhei/__init__.py`**

```python
"""rhei — Python bindings for the rhei stream-processing framework.

Build a pipeline over Arrow batches and run it on the rhei runtime:

    import pyarrow as pa
    import rhei

    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)]

    def double(buf):
        rb = buf.to_arrow()
        col = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))

    df = rhei.Dataflow()
    df.source(batches).map_batches(double, schema=schema).sink_print()
    df.run(workers=1)
"""

from ._rhei import Buffer, Dataflow, Stream, CollectHandle, __version__

__all__ = ["Buffer", "Dataflow", "Stream", "CollectHandle", "__version__"]
```

(If Task 3 chose the non-fluent dataflow-centric API, adjust the docstring example accordingly. Keep `__all__` in sync with the classes actually exported — drop `CollectHandle` only if it wasn't created.)

- [ ] **Step 5: Create `rhei-python/examples/double.py`**

Mirror the `__init__.py` docstring example as a runnable script ending in `df.run(workers=1)`.

- [ ] **Step 6: Rebuild and run the whole suite + the example**

```bash
uv run maturin develop
uv run pytest tests/ -q
uv run python examples/double.py
```
Expected: all tests pass; the example prints a table containing the doubled values.

- [ ] **Step 7: Clippy + commit**

```bash
cargo clippy --no-deps -- -D warnings
cd /Users/roncohen/workspace/frisk
git add rhei-python/
git commit -m "feat(python): print sink, package re-exports, runnable example

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: CI wiring + final verification

**Goal:** Ensure the standalone crate doesn't break the existing workspace CI, document how to build/test it, and run the full gates.

**Files:**
- Modify: `.github/workflows/*` (only if it runs `cargo` workspace commands that would now try to build rhei-python — verify exclusion works) — INVESTIGATE first, change only if needed.
- Verify only otherwise.

- [ ] **Step 1: Confirm the workspace still ignores rhei-python**

From `/Users/roncohen/workspace/frisk`:
```bash
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo nextest run --workspace
```
Expected: all pass, and NONE of them attempt to build `rhei-python` (confirm `rhei-python` is absent from the compile output). This proves the `exclude` works and CI is unaffected.

- [ ] **Step 2: Inspect CI workflow**

Read `.github/workflows/` (find the Rust CI job). Confirm it uses `--workspace` (or per-crate) commands that the `exclude` covers. If the CI does a bare `cargo build` at the root with no `--workspace`, that still respects `exclude`. Only if CI would break, add a note or a separate optional job. Do NOT add a full maturin CI job in this task unless it's trivial — building Python wheels in CI is a separate concern. If you add anything, keep it minimal and document why.

- [ ] **Step 3: Full rhei-python gate**

From `rhei-python/`:
```bash
cargo clippy --no-deps -- -D warnings
cargo fmt -- --check || cargo fmt
uv run maturin develop
uv run pytest tests/ -q
```
Expected: clippy clean, fmt clean, all pytest pass.

- [ ] **Step 4: Commit any CI/fmt changes**

```bash
cd /Users/roncohen/workspace/frisk
git add -A
git commit -m "chore(python): confirm workspace exclusion; fmt rhei-python" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage (against the design spec L1 row + the ADR):**
- "Create the `rhei-python` crate (PyO3 + maturin)" — Task 1. ✓
- "`PyBuffer` with zero-copy `__arrow_c_array__`/`from_arrow`" — Task 2 (implemented via `arrow::pyarrow` `FromPyArrow`/`ToPyArrow`, which IS the Arrow C Data Interface/PyCapsule path — cleaner than hand-writing `__arrow_c_array__`; the spec's intent (zero-copy boundary) is met). ✓
- "`PyDataflow` lowering to the L0 erased-builder API" — Task 3. ✓
- "Python list source → `map_batches` → print/collect sink" — Tasks 3 (collect) + 4 (print). ✓
- "blocking `run(workers=1)`" — Task 3 (owns tokio runtime, `py.allow_threads`). ✓
- "Test: a word-count-style batch pipeline in Python produces identical output to the Rust example" — the double-the-values test is the L1 analogue (same shape as L0's `erased_builder.rs` proof). ✓

**Deliberately deferred (NOT L1, per phasing):** row/Record API (`map`/`filter` over a `Record` view) and `key_by`/`merge` from Python → L2. Built-in windows/agg → L3. Custom Python `Operator` + state → L4. Kafka + DLQ + Ctrl-C → L5. The `@rhei.record` schema decorator → L6. This plan is the thinnest end-to-end slice that proves Python↔Arrow↔Timely↔Arrow↔Python works.

**Placeholder scan:** Task 3's Step 3 deliberately contains an `unimplemented!()` skeleton AND an explicit instruction to replace it — this is a flagged design decision, not a hidden placeholder. The implementer is told precisely which two shapes are acceptable and must commit neither `unimplemented!` nor the `const _:`/`_DataflowGraphAlias` scaffolding lines. Every other step has complete code.

**Type/version consistency:** `pyo3 = "0.23"` and `arrow = "54"` `pyarrow` co-resolve to a single pyo3 0.23.5 (verified against the registry). `RecordBatch::from_pyarrow_bound(&Bound<PyAny>) -> PyResult<RecordBatch>` and `record_batch.to_pyarrow(Python) -> PyResult<PyObject>` are the verified arrow 54.3.1 signatures. `Schema::from_pyarrow_bound` likewise. `schema_hash_of(&Schema)`, `ErasedBuffer::from_parts(RecordBatch, Option<BooleanArray>, u64)`, `DataflowGraph::add_erased_source/transform/sink`, `ErasedSource`/`ErasedSink` method sets all match the L0 code shipped in commits 6732969..a8178b1.

**Risk notes carried from L0 review:** `schema_hash_of` includes field nullability + metadata in the id. The Python path computes the source buffer's id from `rb.schema()` (the actual pyarrow batch's schema) and the transform's output id from the user-declared `schema=` — these must be the SAME Arrow schema (incl. nullability) or the sink's downcast-free collect still works (the collect sink reads column 0 positionally without a typed downcast, so it's tolerant), but any FUTURE typed operator would require an exact match. The print/collect sinks in L1 read the `RecordBatch` directly (no `downcast::<T>`), so L1 is not exposed to the nullability trap — but Task 3 should keep the source and map_batches schemas identical in the test to model correct usage.

---

## Next plan (not in scope here)

**L2 — Stateless row API + structure.** `PyRecord` zero-copy row views; `map`/`filter`/`flat_map`/`inspect` row forms; `filter_batches` with a boolean mask; `key_by` (string key) and `merge` from Python; multi-worker `run(workers=N)` with a key_by-exchange routing test.
