//! `PyStream` — a lightweight handle representing a point in the pipeline.
//!
//! Operations on `PyStream` record transforms into the shared `Pipeline`
//! plan. The actual dataflow graph is materialized when `Pipeline.run()` is
//! called.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

/// Unique identifier for a node in the Python-side pipeline plan.
pub(crate) type NodeId = usize;

/// The kind of operation a plan node represents.
pub(crate) enum PlanOp {
    /// A data source (built-in `VecSource` or custom Python source).
    Source,
    /// A Python map callable.
    Map(PyObject),
    /// A Python filter callable.
    Filter(PyObject),
    /// A Python `flat_map` callable.
    FlatMap(PyObject),
    /// A Python `key_by` callable.
    KeyBy(PyObject),
    /// A sink with its Python object.
    Sink(PyObject),
}

impl std::fmt::Debug for PlanOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "Source"),
            Self::Map(_) => write!(f, "Map(<callable>)"),
            Self::Filter(_) => write!(f, "Filter(<callable>)"),
            Self::FlatMap(_) => write!(f, "FlatMap(<callable>)"),
            Self::KeyBy(_) => write!(f, "KeyBy(<callable>)"),
            Self::Sink(_) => write!(f, "Sink"),
        }
    }
}

/// A node in the Python-side pipeline plan.
pub(crate) struct PlanNode {
    pub id: NodeId,
    pub op: PlanOp,
    /// ID of the parent node (None for sources).
    pub parent: Option<NodeId>,
}

impl std::fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanNode")
            .field("id", &self.id)
            .field("op", &self.op)
            .field("parent", &self.parent)
            .finish()
    }
}

/// Shared, mutable plan that `PyStream` handles append to.
pub(crate) type SharedPlan = Arc<Mutex<Vec<PlanNode>>>;

/// A lightweight, cloneable handle representing a point in the pipeline.
///
/// Calling `.map()`, `.filter()`, etc. adds a new node to the shared plan
/// and returns a new `PyStream` pointing at that node.
#[pyclass(name = "Stream")]
#[derive(Debug, Clone)]
pub struct PyStream {
    /// The plan shared with the parent `Pipeline`.
    pub(crate) plan: SharedPlan,
    /// This stream's node ID in the plan.
    pub(crate) node_id: NodeId,
}

#[pymethods]
impl PyStream {
    /// Transform each element with a Python callable.
    fn map(&self, callable: PyObject) -> PyStream {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::Map(callable),
            parent: Some(self.node_id),
        });
        PyStream {
            plan: self.plan.clone(),
            node_id: id,
        }
    }

    /// Filter elements with a Python predicate.
    fn filter(&self, callable: PyObject) -> PyStream {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::Filter(callable),
            parent: Some(self.node_id),
        });
        PyStream {
            plan: self.plan.clone(),
            node_id: id,
        }
    }

    /// One-to-many transform with a Python callable that returns a list.
    fn flat_map(&self, callable: PyObject) -> PyStream {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::FlatMap(callable),
            parent: Some(self.node_id),
        });
        PyStream {
            plan: self.plan.clone(),
            node_id: id,
        }
    }

    /// Partition elements by key for stateful processing.
    fn key_by(&self, callable: PyObject) -> PyStream {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::KeyBy(callable),
            parent: Some(self.node_id),
        });
        PyStream {
            plan: self.plan.clone(),
            node_id: id,
        }
    }

    /// Terminal: write elements to a sink.
    ///
    /// Accepts either a built-in sink (`PrintSink`) or a custom Python
    /// object with `write(item)` and `flush()` methods.
    fn sink(&self, _name: &str, sink: PyObject) {
        let mut plan = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = plan.len();
        plan.push(PlanNode {
            id,
            op: PlanOp::Sink(sink),
            parent: Some(self.node_id),
        });
    }
}
