//! `Pipeline` — the top-level Python class for building and running stream
//! processing pipelines.
//!
//! Records sources, transforms, and sinks as an intermediate plan, then
//! materializes it into a [`DataflowGraph`] and executes it via the Rhei
//! runtime when [`Pipeline::run()`] is called.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use rhei_core::connectors::print_sink::PrintSink;
use rhei_core::connectors::vec_source::VecSource;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

use crate::connectors::{PyPrintSink, PyVecSource};
use crate::sink::PySink;
use crate::source::{PySource, json_to_py, py_to_json};
use crate::stream::{NodeId, PlanNode, PlanOp, PyStream, SharedPlan};

type Value = serde_json::Value;

/// Metadata about a source node registered with the pipeline.
struct SourceEntry {
    /// Plan node ID for this source.
    node_id: NodeId,
    /// The source object (either built-in or custom Python).
    kind: SourceKind,
}

enum SourceKind {
    Vec(Vec<Value>),
    Python(PyObject),
}

/// Either an unkeyed or keyed stream handle into the dataflow graph.
enum StreamHandle<'a> {
    Unkeyed(rhei_runtime::dataflow::Stream<'a, Value>),
    Keyed(rhei_runtime::dataflow::KeyedStream<'a, Value>),
    Done,
}

/// A stream processing pipeline builder.
///
/// ```python
/// pipeline = rhei.Pipeline()
/// source = pipeline.source("input", rhei.VecSource([1, 2, 3]))
/// mapped = source.map(lambda x: x * 2)
/// mapped.sink("output", rhei.PrintSink())
/// pipeline.run(workers=1)
/// ```
#[pyclass]
pub struct Pipeline {
    plan: SharedPlan,
    sources: Arc<Mutex<Vec<SourceEntry>>>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline").finish_non_exhaustive()
    }
}

#[pymethods]
impl Pipeline {
    /// Create a new empty pipeline.
    #[new]
    fn new() -> Self {
        Self {
            plan: Arc::new(Mutex::new(Vec::new())),
            sources: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a data source to the pipeline. Returns a `Stream` handle.
    ///
    /// The `source` argument can be a built-in `VecSource` or a custom Python
    /// object with a `next_batch()` method.
    fn source(&self, _name: &str, source: &Bound<'_, PyAny>) -> PyStream {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::Source,
            parent: None,
        });

        let kind = if let Ok(vec_src) = source.extract::<PyRef<'_, PyVecSource>>() {
            SourceKind::Vec(vec_src.items.clone())
        } else {
            SourceKind::Python(source.clone().unbind())
        };

        self.sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(SourceEntry { node_id: id, kind });

        PyStream {
            plan: self.plan.clone(),
            node_id: id,
        }
    }

    /// Build and execute the pipeline.
    ///
    /// `workers` controls the number of parallel workers (default: 1).
    #[pyo3(signature = (workers = 1))]
    fn run(&self, workers: usize, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            rt.block_on(async { self.execute(workers).await })
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

impl Pipeline {
    /// Materialize the plan into a `DataflowGraph` and execute it.
    #[allow(clippy::too_many_lines)]
    async fn execute(&self, workers: usize) -> anyhow::Result<()> {
        // Snapshot the plan, cloning `PyObjects` under the GIL.
        let plan: Vec<PlanNode> = Python::with_gil(|py| {
            let p = self
                .plan
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            p.iter()
                .map(|node| PlanNode {
                    id: node.id,
                    op: match &node.op {
                        PlanOp::Source => PlanOp::Source,
                        PlanOp::Map(f) => PlanOp::Map(f.clone_ref(py)),
                        PlanOp::Filter(f) => PlanOp::Filter(f.clone_ref(py)),
                        PlanOp::FlatMap(f) => PlanOp::FlatMap(f.clone_ref(py)),
                        PlanOp::KeyBy(f) => PlanOp::KeyBy(f.clone_ref(py)),
                        PlanOp::Sink(s) => PlanOp::Sink(s.clone_ref(py)),
                    },
                    parent: node.parent,
                })
                .collect()
        });

        let sources: Vec<SourceEntry> = {
            let mut s = self
                .sources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *s)
        };

        let graph = DataflowGraph::new();
        let mut handles: Vec<Option<StreamHandle<'_>>> = (0..plan.len()).map(|_| None).collect();

        // Create all sources.
        for src in &sources {
            let stream = match &src.kind {
                SourceKind::Vec(items) => graph.source(VecSource::new(items.clone())),
                SourceKind::Python(obj) => {
                    Python::with_gil(|py| graph.source(PySource::new(obj.clone_ref(py))))
                }
            };
            handles[src.node_id] = Some(StreamHandle::Unkeyed(stream));
        }

        // Process remaining nodes in dependency order.
        for node in &plan {
            if handles[node.id].is_some() {
                continue;
            }

            let parent_id = node
                .parent
                .ok_or_else(|| anyhow::anyhow!("non-source node {} has no parent", node.id))?;

            let parent = handles[parent_id]
                .take()
                .ok_or_else(|| anyhow::anyhow!("parent {parent_id} already consumed"))?;

            let new_handle = Self::materialize_node(&node.op, parent, &graph)?;
            handles[node.id] = Some(new_handle);
        }

        let dir = tempfile::tempdir()
            .map_err(|e| anyhow::anyhow!("failed to create temp checkpoint dir: {e}"))?;

        let controller = PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(workers)
            .build()?;

        controller.run(graph).await
    }

    /// Materialize a single plan node into the dataflow graph.
    fn materialize_node<'a>(
        op: &PlanOp,
        parent: StreamHandle<'a>,
        _graph: &'a DataflowGraph,
    ) -> anyhow::Result<StreamHandle<'a>> {
        match op {
            PlanOp::Source => Ok(parent),
            PlanOp::Map(callable) => {
                let f = Python::with_gil(|py| Arc::new(callable.clone_ref(py)));
                Self::apply_map(parent, f)
            }
            PlanOp::Filter(callable) => {
                let f = Python::with_gil(|py| Arc::new(callable.clone_ref(py)));
                Self::apply_filter(parent, f)
            }
            PlanOp::FlatMap(callable) => {
                let f = Python::with_gil(|py| Arc::new(callable.clone_ref(py)));
                Self::apply_flat_map(parent, f)
            }
            PlanOp::KeyBy(callable) => {
                let f = Python::with_gil(|py| Arc::new(callable.clone_ref(py)));
                Self::apply_key_by(parent, f)
            }
            PlanOp::Sink(sink_obj) => {
                Self::apply_sink(parent, sink_obj)?;
                Ok(StreamHandle::Done)
            }
        }
    }

    fn apply_map(parent: StreamHandle<'_>, f: Arc<PyObject>) -> anyhow::Result<StreamHandle<'_>> {
        match parent {
            StreamHandle::Unkeyed(s) => Ok(StreamHandle::Unkeyed(s.map(move |val: Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, &val);
                    let result = f.call1(py, (py_val,)).unwrap_or_else(|e| {
                        tracing::error!("map callable failed: {e}");
                        py.None()
                    });
                    py_to_json(py, &result).unwrap_or(Value::Null)
                })
            }))),
            StreamHandle::Keyed(s) => Ok(StreamHandle::Keyed(s.map(move |val: Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, &val);
                    let result = f.call1(py, (py_val,)).unwrap_or_else(|e| {
                        tracing::error!("map callable failed: {e}");
                        py.None()
                    });
                    py_to_json(py, &result).unwrap_or(Value::Null)
                })
            }))),
            StreamHandle::Done => Err(anyhow::anyhow!("cannot chain after a sink")),
        }
    }

    fn apply_filter(
        parent: StreamHandle<'_>,
        f: Arc<PyObject>,
    ) -> anyhow::Result<StreamHandle<'_>> {
        match parent {
            StreamHandle::Unkeyed(s) => Ok(StreamHandle::Unkeyed(s.filter(move |val: &Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, val);
                    f.call1(py, (py_val,))
                        .and_then(|r| r.extract::<bool>(py))
                        .unwrap_or(false)
                })
            }))),
            StreamHandle::Keyed(s) => Ok(StreamHandle::Keyed(s.filter(move |val: &Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, val);
                    f.call1(py, (py_val,))
                        .and_then(|r| r.extract::<bool>(py))
                        .unwrap_or(false)
                })
            }))),
            StreamHandle::Done => Err(anyhow::anyhow!("cannot chain after a sink")),
        }
    }

    fn apply_flat_map(
        parent: StreamHandle<'_>,
        f: Arc<PyObject>,
    ) -> anyhow::Result<StreamHandle<'_>> {
        match parent {
            StreamHandle::Unkeyed(s) => Ok(StreamHandle::Unkeyed(s.flat_map(move |val: Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, &val);
                    let result = f.call1(py, (py_val,)).unwrap_or_else(|e| {
                        tracing::error!("flat_map callable failed: {e}");
                        py.None()
                    });
                    let list: Vec<PyObject> = result.extract(py).unwrap_or_default();
                    list.iter()
                        .filter_map(|item| py_to_json(py, item).ok())
                        .collect()
                })
            }))),
            StreamHandle::Keyed(s) => Ok(StreamHandle::Keyed(s.flat_map(move |val: Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, &val);
                    let result = f.call1(py, (py_val,)).unwrap_or_else(|e| {
                        tracing::error!("flat_map callable failed: {e}");
                        py.None()
                    });
                    let list: Vec<PyObject> = result.extract(py).unwrap_or_default();
                    list.iter()
                        .filter_map(|item| py_to_json(py, item).ok())
                        .collect()
                })
            }))),
            StreamHandle::Done => Err(anyhow::anyhow!("cannot chain after a sink")),
        }
    }

    fn apply_key_by(
        parent: StreamHandle<'_>,
        f: Arc<PyObject>,
    ) -> anyhow::Result<StreamHandle<'_>> {
        match parent {
            StreamHandle::Unkeyed(s) => Ok(StreamHandle::Keyed(s.key_by(move |val: &Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, val);
                    f.call1(py, (py_val,))
                        .and_then(|r| r.extract::<String>(py))
                        .unwrap_or_default()
                })
            }))),
            StreamHandle::Keyed(s) => Ok(StreamHandle::Keyed(s.key_by(move |val: &Value| {
                Python::with_gil(|py| {
                    let py_val = json_to_py(py, val);
                    f.call1(py, (py_val,))
                        .and_then(|r| r.extract::<String>(py))
                        .unwrap_or_default()
                })
            }))),
            StreamHandle::Done => Err(anyhow::anyhow!("cannot chain after a sink")),
        }
    }

    fn apply_sink(parent: StreamHandle<'_>, sink_obj: &PyObject) -> anyhow::Result<()> {
        let is_print = Python::with_gil(|py| {
            let bound = sink_obj.bind(py);
            bound.is_instance_of::<PyPrintSink>()
        });

        match parent {
            StreamHandle::Unkeyed(s) => {
                if is_print {
                    s.sink(PrintSink::<Value>::new());
                } else {
                    Python::with_gil(|py| {
                        s.sink(PySink::new(sink_obj.clone_ref(py)));
                    });
                }
            }
            StreamHandle::Keyed(s) => {
                if is_print {
                    s.sink(PrintSink::<Value>::new());
                } else {
                    Python::with_gil(|py| {
                        s.sink(PySink::new(sink_obj.clone_ref(py)));
                    });
                }
            }
            StreamHandle::Done => {
                return Err(anyhow::anyhow!("cannot chain after a sink"));
            }
        }

        Ok(())
    }
}
