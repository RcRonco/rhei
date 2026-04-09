//! Python sink bridge: wraps a Python object implementing `write(item)` and
//! `flush()` as a Rhei [`Sink`](rhei_core::traits::Sink).

use async_trait::async_trait;
use pyo3::prelude::*;
use rhei_core::traits::Sink;

use crate::source::json_to_py;

/// Wraps an arbitrary Python object that has `write(item)` and `flush()` methods.
pub(crate) struct PySink {
    obj: PyObject,
}

impl PySink {
    pub fn new(obj: PyObject) -> Self {
        Self { obj }
    }
}

impl std::fmt::Debug for PySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySink").finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for PySink {
    type Input = serde_json::Value;

    async fn write(&mut self, input: serde_json::Value) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let py_val = json_to_py(py, &input);
            self.obj
                .call_method1(py, "write", (py_val,))
                .map_err(|e| anyhow::anyhow!("PySink.write() failed: {e}"))?;
            Ok(())
        })
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            self.obj
                .call_method0(py, "flush")
                .map_err(|e| anyhow::anyhow!("PySink.flush() failed: {e}"))?;
            Ok(())
        })
    }
}
