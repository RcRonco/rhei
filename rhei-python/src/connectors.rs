//! Built-in source and sink connectors exposed to Python.

use pyo3::prelude::*;

use crate::source::py_to_json;

/// A source that emits items from a Python list.
///
/// Usage:
/// ```python
/// source = rhei.VecSource([1, 2, 3, 4, 5])
/// ```
#[pyclass(name = "VecSource")]
#[derive(Debug)]
pub struct PyVecSource {
    pub(crate) items: Vec<serde_json::Value>,
}

#[pymethods]
impl PyVecSource {
    /// Create a new `VecSource` from a Python list.
    #[new]
    fn new(items: Vec<PyObject>, py: Python<'_>) -> PyResult<Self> {
        let values: Vec<serde_json::Value> = items
            .iter()
            .map(|item| py_to_json(py, item))
            .collect::<PyResult<_>>()?;
        Ok(Self { items: values })
    }
}

/// A sink that prints each item to stdout.
///
/// Usage:
/// ```python
/// sink = rhei.PrintSink()
/// ```
#[pyclass(name = "PrintSink")]
#[derive(Debug)]
pub struct PyPrintSink;

#[pymethods]
impl PyPrintSink {
    /// Create a new `PrintSink`.
    #[new]
    fn new() -> Self {
        Self
    }
}
