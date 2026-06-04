//! `PyDataflow` / `PyStream`: build and run a pipeline from Python.
//!
//! Lowers Python graph construction onto rhei-runtime's L0 erased-builder API
//! (`DataflowGraph::add_erased_source` / `add_erased_transform` /
//! `add_erased_sink`). The one Rust type flowing through a Python graph is
//! `ErasedBuffer`; Python callables are boxed into the L0 `BatchTransformFn`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::pyarrow::FromPyArrow;
use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::{BatchTransformFn, DataflowGraph, ErasedHandle};
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, KeyFn, schema_hash_of};

use crate::buffer::PyBuffer;
use crate::codec::{batch_row_to_dict, dicts_to_batch};

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
        if batch.num_columns() == 0 {
            return Ok(());
        }
        let column = batch.column(0);
        let col = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "sink_collect requires an Int64 first column, got {:?}",
                    column.data_type()
                )
            })?;
        let mut guard = self
            .shared
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for i in 0..col.len() {
            guard.push(col.value(i));
        }
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

// ── Sink: pretty-print each batch to stdout ──────────────────────────

struct PrintSink;

#[async_trait]
impl ErasedSink for PrintSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        let batch = buf.as_record_batch();
        let formatted = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .map_or_else(|e| format!("<unprintable batch: {e}>"), |d| d.to_string());
        println!("{formatted}");
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Handle returned by `Stream.sink_collect()`; exposes the collected values
/// after `run()`. Holds the shared buffer the sink writes into, so it stays
/// alive independently of the graph.
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

    fn __repr__(&self) -> String {
        let n = self
            .shared
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        format!("CollectHandle(collected={n})")
    }
}

// ── Pipeline error reporting ─────────────────────────────────────────

/// Shared slot recording the first fatal error raised inside a Python
/// transform. The L0 `BatchTransformFn` signature has no error channel, so a
/// transform that fails records its message here and returns no output; `run()`
/// checks the slot after the pipeline drains and re-raises it as a Python
/// exception. This turns a green run with silently-dropped batches into a loud
/// failure.
type ErrorSlot = Arc<Mutex<Option<String>>>;

fn record_error(slot: &ErrorSlot, message: String) {
    let mut guard = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if guard.is_none() {
        *guard = Some(message);
    }
}

fn format_py_error(py: Python<'_>, context: &str, err: &PyErr) -> String {
    let msg = err.value(py).str().map_or_else(
        |_| "<unprintable Python error>".to_string(),
        |s| s.to_string_lossy().into_owned(),
    );
    format!("{context}: {msg}")
}

// ── Python-callable transform ────────────────────────────────────────

/// Build a `BatchTransformFn` that calls a Python `fn(Buffer) -> Buffer`.
///
/// Acquires the GIL only while the Python callable runs. The Python function
/// MUST return a `rhei.Buffer` whose Arrow schema matches the `schema=` declared
/// on `map_batches` (validated by schema id). Any failure — an exception, a
/// wrong return type, or a schema mismatch — is recorded in `error_slot` and
/// aborts the run, rather than silently dropping the batch.
fn py_map_batches_transform(
    callable: Py<PyAny>,
    out_schema_id: u64,
    error_slot: ErrorSlot,
) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        Python::with_gil(|py| {
            let in_obj = match Py::new(py, PyBuffer::from_erased(buf)) {
                Ok(o) => o,
                Err(e) => {
                    record_error(
                        &error_slot,
                        format_py_error(py, "map_batches: wrapping input", &e),
                    );
                    return vec![];
                }
            };
            let result = match callable.bind(py).call1((in_obj,)) {
                Ok(r) => r,
                Err(e) => {
                    record_error(
                        &error_slot,
                        format_py_error(py, "map_batches: Python callable raised", &e),
                    );
                    return vec![];
                }
            };
            let buf_obj = match result.downcast::<PyBuffer>() {
                Ok(b) => b,
                Err(_) => {
                    record_error(
                        &error_slot,
                        format!(
                            "map_batches: callable must return a rhei.Buffer, got {}",
                            result
                                .get_type()
                                .name()
                                .map_or_else(|_| "<unknown>".to_string(), |n| n.to_string())
                        ),
                    );
                    return vec![];
                }
            };
            let rb = buf_obj.borrow().inner.as_record_batch().clone();
            // Validate the produced schema against the declared one. Tagging an
            // ErasedBuffer with a schema id that doesn't match its real layout
            // would let a downstream typed downcast misinterpret the data, so we
            // reject the mismatch with an actionable error instead.
            let actual_id = schema_hash_of(rb.schema_ref());
            if actual_id != out_schema_id {
                record_error(
                    &error_slot,
                    format!(
                        "map_batches: returned batch schema {:?} does not match the declared \
                         schema= (note: field nullability and metadata are significant)",
                        rb.schema()
                    ),
                );
                return vec![];
            }
            vec![ErasedBuffer::from_parts(rb, None, out_schema_id)]
        })
    })
}

// ── Row-oriented transforms (codec-backed) ───────────────────────────

/// Compact an `ErasedBuffer` to a plain `RecordBatch`, applying any selection
/// mask so the row codec never sees logically-filtered rows.
fn compact_to_batch(buf: &ErasedBuffer) -> RecordBatch {
    let batch = buf.as_record_batch();
    match buf.mask() {
        Some(mask) => {
            arrow::compute::filter_record_batch(batch, mask).unwrap_or_else(|_| batch.clone())
        }
        None => batch.clone(),
    }
}

/// `map`: call a Python `fn(dict) -> dict` per row, build an output batch with
/// the declared schema. Mirrors the typed `Stream::map`, type-erased.
fn py_row_map_transform(
    callable: Py<PyAny>,
    out_schema: SchemaRef,
    out_schema_id: u64,
    error_slot: ErrorSlot,
) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        let batch = compact_to_batch(&buf);
        Python::with_gil(|py| {
            let mut out_rows: Vec<Bound<'_, PyAny>> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let dict = match batch_row_to_dict(py, &batch, row) {
                    Ok(d) => d,
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "map: reading row", &e));
                        return vec![];
                    }
                };
                match callable.bind(py).call1((dict,)) {
                    Ok(r) => out_rows.push(r),
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "map: callable raised", &e));
                        return vec![];
                    }
                }
            }
            match dicts_to_batch(&out_rows, &out_schema) {
                Ok(rb) => vec![ErasedBuffer::from_parts(rb, None, out_schema_id)],
                Err(e) => {
                    record_error(&error_slot, format_py_error(py, "map: building output", &e));
                    vec![]
                }
            }
        })
    })
}

/// `filter`: call a Python `fn(dict) -> bool` per row; emit a compacted batch of
/// the rows that passed (schema unchanged).
fn py_row_filter_transform(callable: Py<PyAny>, error_slot: ErrorSlot) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        let batch = compact_to_batch(&buf);
        let schema_id = buf.schema_id();
        Python::with_gil(|py| {
            let mut keep = vec![false; batch.num_rows()];
            let mut any = false;
            for (row, slot) in keep.iter_mut().enumerate() {
                let dict = match batch_row_to_dict(py, &batch, row) {
                    Ok(d) => d,
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "filter: reading row", &e));
                        return vec![];
                    }
                };
                match callable
                    .bind(py)
                    .call1((dict,))
                    .and_then(|r| r.extract::<bool>())
                {
                    Ok(b) => {
                        *slot = b;
                        any |= b;
                    }
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "filter: callable", &e));
                        return vec![];
                    }
                }
            }
            if !any {
                return vec![];
            }
            let mask = BooleanArray::from(keep);
            match arrow::compute::filter_record_batch(&batch, &mask) {
                Ok(rb) => vec![ErasedBuffer::from_parts(rb, None, schema_id)],
                Err(e) => {
                    record_error(&error_slot, format!("filter: applying mask: {e}"));
                    vec![]
                }
            }
        })
    })
}

/// `flat_map`: call a Python `fn(dict) -> list[dict]` per row; flatten into one
/// output batch with the declared schema.
fn py_row_flat_map_transform(
    callable: Py<PyAny>,
    out_schema: SchemaRef,
    out_schema_id: u64,
    error_slot: ErrorSlot,
) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        let batch = compact_to_batch(&buf);
        Python::with_gil(|py| {
            let mut out_rows: Vec<Bound<'_, PyAny>> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let dict = match batch_row_to_dict(py, &batch, row) {
                    Ok(d) => d,
                    Err(e) => {
                        record_error(
                            &error_slot,
                            format_py_error(py, "flat_map: reading row", &e),
                        );
                        return vec![];
                    }
                };
                let produced = match callable.bind(py).call1((dict,)) {
                    Ok(r) => r,
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "flat_map: callable", &e));
                        return vec![];
                    }
                };
                let iter = match produced.try_iter() {
                    Ok(it) => it,
                    Err(e) => {
                        record_error(
                            &error_slot,
                            format_py_error(py, "flat_map: return value not iterable", &e),
                        );
                        return vec![];
                    }
                };
                for item in iter {
                    match item {
                        Ok(d) => out_rows.push(d),
                        Err(e) => {
                            record_error(
                                &error_slot,
                                format_py_error(py, "flat_map: iterating", &e),
                            );
                            return vec![];
                        }
                    }
                }
            }
            if out_rows.is_empty() {
                return vec![];
            }
            match dicts_to_batch(&out_rows, &out_schema) {
                Ok(rb) => vec![ErasedBuffer::from_parts(rb, None, out_schema_id)],
                Err(e) => {
                    record_error(
                        &error_slot,
                        format_py_error(py, "flat_map: building output", &e),
                    );
                    vec![]
                }
            }
        })
    })
}

/// `inspect`: call a Python `fn(dict)` per row for side effects; pass the buffer
/// through unchanged.
fn py_row_inspect_transform(callable: Py<PyAny>, error_slot: ErrorSlot) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        let batch = compact_to_batch(&buf);
        let schema_id = buf.schema_id();
        Python::with_gil(|py| {
            for row in 0..batch.num_rows() {
                let dict = match batch_row_to_dict(py, &batch, row) {
                    Ok(d) => d,
                    Err(e) => {
                        record_error(&error_slot, format_py_error(py, "inspect: reading row", &e));
                        return vec![];
                    }
                };
                if let Err(e) = callable.bind(py).call1((dict,)) {
                    record_error(&error_slot, format_py_error(py, "inspect: callable", &e));
                    return vec![];
                }
            }
            vec![ErasedBuffer::from_parts(batch, None, schema_id)]
        })
    })
}

/// Wrap a Python `fn(dict) -> str` as a `KeyFn` for `key_by`. A Python error
/// routes the row to a sentinel key so the run can surface it rather than panic.
fn py_key_fn(callable: Py<PyAny>, error_slot: ErrorSlot) -> KeyFn {
    Arc::new(move |batch: &RecordBatch, row: usize| {
        Python::with_gil(|py| {
            let dict = match batch_row_to_dict(py, batch, row) {
                Ok(d) => d,
                Err(e) => {
                    record_error(&error_slot, format_py_error(py, "key_by: reading row", &e));
                    return "__rhei_key_error__".to_string();
                }
            };
            match callable
                .bind(py)
                .call1((dict,))
                .and_then(|r| r.extract::<String>())
            {
                Ok(k) => k,
                Err(e) => {
                    record_error(&error_slot, format_py_error(py, "key_by: callable", &e));
                    "__rhei_key_error__".to_string()
                }
            }
        })
    })
}

// ── PyDataflow / PyStream ────────────────────────────────────────────

/// A rhei dataflow graph builder.
///
/// `unsendable`: the underlying `DataflowGraph` uses `RefCell` interior
/// mutability and is only ever built from the thread that created it (the
/// Python thread). pyo3 0.23 requires `#[pyclass]` types to be `Sync` unless
/// marked `unsendable`.
#[pyclass(name = "Dataflow", unsendable)]
pub struct PyDataflow {
    graph: DataflowGraph,
    /// Records the first fatal error from any Python transform, checked by
    /// `run()`. Shared with every transform closure built on this dataflow.
    error_slot: ErrorSlot,
    /// Set once `run()` has consumed the graph, so a second `run()` fails with
    /// a clear message instead of the misleading "empty graph" validation error.
    has_run: bool,
}

#[pymethods]
impl PyDataflow {
    #[new]
    fn new() -> Self {
        Self {
            graph: DataflowGraph::new(),
            error_slot: Arc::new(Mutex::new(None)),
            has_run: false,
        }
    }

    /// Add a source from an iterable of pyarrow `RecordBatch`es. Returns a
    /// `Stream`.
    fn source(slf: Py<Self>, py: Python<'_>, batches: &Bound<'_, PyAny>) -> PyResult<PyStream> {
        let mut queue: VecDeque<ErasedBuffer> = VecDeque::new();
        for item in batches.try_iter()? {
            let item = item?;
            let rb = RecordBatch::from_pyarrow_bound(&item)?;
            let schema_id = schema_hash_of(rb.schema_ref());
            queue.push_back(ErasedBuffer::from_parts(rb, None, schema_id));
        }
        let handle = slf
            .bind(py)
            .borrow()
            .graph
            .add_erased_source(Box::new(ListSource { batches: queue }));
        Ok(PyStream { df: slf, handle })
    }

    /// Run the pipeline to completion (blocking). Owns the tokio runtime and
    /// releases the GIL while the pipeline executes.
    #[pyo3(signature = (workers = 1, checkpoint_dir = None))]
    fn run(
        &mut self,
        py: Python<'_>,
        workers: usize,
        checkpoint_dir: Option<String>,
    ) -> PyResult<()> {
        if self.has_run {
            return Err(PyRuntimeError::new_err(
                "this Dataflow has already been run; build a new rhei.Dataflow() to run again",
            ));
        }
        self.has_run = true;
        // DataflowGraph::run consumes the graph; swap in a fresh empty one.
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
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(async move {
                let ctrl =
                    PipelineController::new(std::path::PathBuf::from(dir)).with_workers(workers);
                ctrl.run(graph)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })?;
        // Surface the first error any Python transform recorded during the run.
        if let Some(msg) = self
            .error_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(PyRuntimeError::new_err(msg));
        }
        Ok(())
    }
}

/// A point in a rhei dataflow. Operations add nodes and return new handles.
///
/// `unsendable`: holds a `Py<PyDataflow>` to an `unsendable` graph builder and
/// is only used during single-threaded graph construction.
#[pyclass(name = "Stream", unsendable)]
pub struct PyStream {
    df: Py<PyDataflow>,
    handle: ErasedHandle,
}

impl PyStream {
    /// Clone the dataflow's shared error slot (for building transforms).
    fn error_slot(&self, py: Python<'_>) -> ErrorSlot {
        self.df.bind(py).borrow().error_slot.clone()
    }

    /// Attach a transform to the graph and return the downstream handle.
    fn add_transform(&self, py: Python<'_>, transform: BatchTransformFn) -> PyStream {
        let new_handle = self
            .df
            .bind(py)
            .borrow()
            .graph
            .add_erased_transform(self.handle, transform);
        PyStream {
            df: self.df.clone_ref(py),
            handle: new_handle,
        }
    }
}

#[pymethods]
impl PyStream {
    /// Apply a Python `fn(Buffer) -> Buffer` to each batch. `schema` is the
    /// output Arrow schema (a pyarrow `Schema`).
    fn map_batches(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<PyStream> {
        let out_schema = Schema::from_pyarrow_bound(schema)?;
        let out_schema_id = schema_hash_of(&out_schema);
        let transform = py_map_batches_transform(func, out_schema_id, self.error_slot(py));
        Ok(self.add_transform(py, transform))
    }

    /// Apply a Python `fn(dict) -> dict` to each row. `schema` is the output
    /// Arrow schema (a pyarrow `Schema`).
    fn map(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<PyStream> {
        let out_schema: SchemaRef = Arc::new(Schema::from_pyarrow_bound(schema)?);
        let out_schema_id = schema_hash_of(&out_schema);
        let transform = py_row_map_transform(func, out_schema, out_schema_id, self.error_slot(py));
        Ok(self.add_transform(py, transform))
    }

    /// Keep rows for which a Python `fn(dict) -> bool` returns `True`.
    fn filter(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        let transform = py_row_filter_transform(func, self.error_slot(py));
        Ok(self.add_transform(py, transform))
    }

    /// Apply a Python `fn(dict) -> list[dict]` to each row, flattening the
    /// results. `schema` is the output Arrow schema (a pyarrow `Schema`).
    fn flat_map(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<PyStream> {
        let out_schema: SchemaRef = Arc::new(Schema::from_pyarrow_bound(schema)?);
        let out_schema_id = schema_hash_of(&out_schema);
        let transform =
            py_row_flat_map_transform(func, out_schema, out_schema_id, self.error_slot(py));
        Ok(self.add_transform(py, transform))
    }

    /// Call a Python `fn(dict)` for each row (side effects only); the stream is
    /// unchanged.
    fn inspect(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        let transform = py_row_inspect_transform(func, self.error_slot(py));
        Ok(self.add_transform(py, transform))
    }

    /// Repartition by a Python `fn(dict) -> str` key so equal keys land on the
    /// same worker.
    fn key_by(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        let key_fn = py_key_fn(func, self.error_slot(py));
        let new_handle = self
            .df
            .bind(py)
            .borrow()
            .graph
            .add_key_by(self.handle, key_fn);
        Ok(PyStream {
            df: self.df.clone_ref(py),
            handle: new_handle,
        })
    }

    /// Merge this stream with another of the same schema into one.
    fn merge(&self, py: Python<'_>, other: &PyStream) -> PyResult<PyStream> {
        let new_handle = self
            .df
            .bind(py)
            .borrow()
            .graph
            .add_merge(self.handle, other.handle);
        Ok(PyStream {
            df: self.df.clone_ref(py),
            handle: new_handle,
        })
    }

    /// Terminal: collect every batch's first Int64 column into a
    /// `CollectHandle` (test/inspection sink).
    fn sink_collect(&self, py: Python<'_>) -> PyCollectHandle {
        let shared = CollectShared {
            values: Arc::new(Mutex::new(Vec::new())),
        };
        self.df.bind(py).borrow().graph.add_erased_sink(
            self.handle,
            Box::new(CollectSink {
                shared: shared.clone(),
            }),
        );
        PyCollectHandle { shared }
    }

    /// Terminal: pretty-print each batch to stdout.
    fn sink_print(&self, py: Python<'_>) {
        self.df
            .bind(py)
            .borrow()
            .graph
            .add_erased_sink(self.handle, Box::new(PrintSink));
    }
}
