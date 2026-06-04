//! `PyDataflow` / `PyStream`: build and run a pipeline from Python.
//!
//! Lowers Python graph construction onto rhei-runtime's L0 erased-builder API
//! (`DataflowGraph::add_erased_source` / `add_erased_transform` /
//! `add_erased_sink`). The one Rust type flowing through a Python graph is
//! `ErasedBuffer`; Python callables are boxed into the L0 `BatchTransformFn`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::pyarrow::FromPyArrow;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::Schema;
use async_trait::async_trait;
use pyo3::exceptions::PyRuntimeError;
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

// ── Python-callable transform ────────────────────────────────────────

/// Build a `BatchTransformFn` that calls a Python `fn(Buffer) -> Buffer`.
///
/// Acquires the GIL only while the Python callable runs; the resulting
/// `ErasedBuffer` carries `out_schema_id` so it interoperates downstream.
fn py_map_batches_transform(callable: Py<PyAny>, out_schema_id: u64) -> BatchTransformFn {
    Arc::new(move |buf: ErasedBuffer| {
        Python::with_gil(|py| {
            let in_obj = match Py::new(py, PyBuffer::from_erased(buf)) {
                Ok(o) => o,
                Err(e) => {
                    report_py_error(py, "map_batches: wrapping input", &e);
                    return vec![];
                }
            };
            let result = match callable.bind(py).call1((in_obj,)) {
                Ok(r) => r,
                Err(e) => {
                    report_py_error(py, "map_batches: Python callable raised", &e);
                    return vec![];
                }
            };
            let buf_obj = match result.downcast::<PyBuffer>() {
                Ok(b) => b,
                Err(e) => {
                    report_py_error(
                        py,
                        "map_batches: return value was not a rhei.Buffer",
                        &PyErr::from(e),
                    );
                    return vec![];
                }
            };
            // Re-tag with the declared output schema id so downstream nodes
            // (and the sink) can rely on it.
            let rb = buf_obj.borrow().inner.as_record_batch().clone();
            vec![ErasedBuffer::from_parts(rb, None, out_schema_id)]
        })
    })
}

fn report_py_error(py: Python<'_>, context: &str, err: &PyErr) {
    let msg = err.value(py).str().map_or_else(
        |_| "<unprintable Python error>".to_string(),
        |s| s.to_string_lossy().into_owned(),
    );
    eprintln!("rhei {context}: {msg}");
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
}

#[pymethods]
impl PyDataflow {
    #[new]
    fn new() -> Self {
        Self {
            graph: DataflowGraph::new(),
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
        })
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
        let transform = py_map_batches_transform(func, out_schema_id);
        let new_handle = self
            .df
            .bind(py)
            .borrow()
            .graph
            .add_erased_transform(self.handle, transform);
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
