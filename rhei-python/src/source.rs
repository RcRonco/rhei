//! Python source bridge: wraps a Python object implementing `next_batch()` as a
//! Rhei [`Source`](rhei_core::traits::Source).

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhei_core::traits::Source;

/// Wraps an arbitrary Python object that has a `next_batch()` method returning
/// a list (or `None` when exhausted).
pub(crate) struct PySource {
    obj: PyObject,
}

impl PySource {
    pub fn new(obj: PyObject) -> Self {
        Self { obj }
    }
}

impl std::fmt::Debug for PySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySource").finish_non_exhaustive()
    }
}

#[async_trait]
impl Source for PySource {
    type Output = serde_json::Value;

    async fn next_batch(&mut self) -> Option<Vec<serde_json::Value>> {
        Python::with_gil(|py| {
            let result = self.obj.call_method0(py, "next_batch").ok()?;
            if result.is_none(py) {
                return None;
            }
            let list: Vec<PyObject> = result.extract(py).ok()?;
            let values: Vec<serde_json::Value> = list
                .into_iter()
                .filter_map(|item| py_to_json(py, &item).ok())
                .collect();
            Some(values)
        })
    }
}

/// Convert a Python object to a `serde_json::Value`.
pub(crate) fn py_to_json(py: Python<'_>, obj: &PyObject) -> PyResult<serde_json::Value> {
    let bound = obj.bind(py);

    if bound.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(val) = bound.extract::<bool>() {
        return Ok(serde_json::Value::Bool(val));
    }
    if let Ok(val) = bound.extract::<i64>() {
        return Ok(serde_json::json!(val));
    }
    if let Ok(val) = bound.extract::<f64>() {
        return Ok(serde_json::json!(val));
    }
    if let Ok(val) = bound.extract::<String>() {
        return Ok(serde_json::Value::String(val));
    }
    if let Ok(list) = bound.extract::<Vec<PyObject>>() {
        let items: Vec<serde_json::Value> = list
            .iter()
            .filter_map(|item| py_to_json(py, item).ok())
            .collect();
        return Ok(serde_json::Value::Array(items));
    }
    // Fall back to string representation.
    let repr = bound.str()?.to_string();
    Ok(serde_json::Value::String(repr))
}

/// Convert a `serde_json::Value` to a Python object.
pub(crate) fn json_to_py(py: Python<'_>, val: &serde_json::Value) -> PyObject {
    match val {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => b
            .into_pyobject(py)
            .map_or_else(|_| py.None(), |o| o.to_owned().unbind().into_any()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py)
                    .map_or_else(|_| py.None(), |o| o.unbind().into_any())
            } else if let Some(f) = n.as_f64() {
                f.into_pyobject(py)
                    .map_or_else(|_| py.None(), |o| o.unbind().into_any())
            } else {
                py.None()
            }
        }
        serde_json::Value::String(s) => s
            .into_pyobject(py)
            .map_or_else(|_| py.None(), |o| o.unbind().into_any()),
        serde_json::Value::Array(arr) => {
            let items: Vec<PyObject> = arr.iter().map(|v| json_to_py(py, v)).collect();
            items
                .into_pyobject(py)
                .map_or_else(|_| py.None(), |o| o.unbind().into_any())
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                let _ = dict.set_item(k, json_to_py(py, v));
            }
            dict.unbind().into_any()
        }
    }
}
