//! Python-implemented sources and sinks (`PySource`, `PySink`).
//!
//! A Python user subclasses `rhei.Source` (implement `next_batch() -> Buffer |
//! None`) or `rhei.Sink` (implement `write_batch(buffer)`), and attaches it with
//! `df.from_source(MySource())` / `stream.sink(MySink())`. These wrap the
//! Python object as the public `ErasedSource`/`ErasedSink` traits.
//!
//! Methods are synchronous on the Python side; the runtime drives them from its
//! source/sink bridge tasks, acquiring the GIL per call. A source returns
//! `None` to signal end of stream. Errors are recorded in the shared error slot
//! so `run()` re-raises them.

use std::collections::HashMap;

use async_trait::async_trait;
use pyo3::prelude::*;
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::ErasedBuffer;

use crate::buffer::PyBuffer;
use crate::dataflow::{ErrorSlot, format_py_error, record_error};

/// Wraps a Python source object (with `next_batch() -> Buffer | None`) as an
/// [`ErasedSource`].
pub struct PySource {
    obj: Py<PyAny>,
    error_slot: ErrorSlot,
    /// Once the Python source returns `None` (or errors), stop calling it.
    exhausted: bool,
}

impl PySource {
    pub fn new(obj: Py<PyAny>, error_slot: ErrorSlot) -> Self {
        Self {
            obj,
            error_slot,
            exhausted: false,
        }
    }
}

#[async_trait]
impl ErasedSource for PySource {
    async fn next_batch(&mut self) -> Option<ErasedBuffer> {
        if self.exhausted {
            return None;
        }
        Python::with_gil(|py| {
            let ret = match self.obj.bind(py).call_method0("next_batch") {
                Ok(r) => r,
                Err(e) => {
                    record_error(
                        &self.error_slot,
                        format_py_error(py, "source.next_batch raised", &e),
                    );
                    self.exhausted = true;
                    return None;
                }
            };
            if ret.is_none() {
                self.exhausted = true;
                return None;
            }
            match ret.downcast::<PyBuffer>() {
                Ok(b) => Some(b.borrow().inner.clone()),
                Err(_) => {
                    record_error(
                        &self.error_slot,
                        "source.next_batch must return a rhei.Buffer or None".to_string(),
                    );
                    self.exhausted = true;
                    None
                }
            }
        })
    }
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn current_offsets(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    async fn restore_offsets(&mut self, _offsets: &HashMap<String, String>) -> anyhow::Result<()> {
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

/// Wraps a Python sink object (with `write_batch(buffer)`, optional `flush()`)
/// as an [`ErasedSink`].
pub struct PySink {
    obj: Py<PyAny>,
    error_slot: ErrorSlot,
}

impl PySink {
    pub fn new(obj: Py<PyAny>, error_slot: ErrorSlot) -> Self {
        Self { obj, error_slot }
    }
}

#[async_trait]
impl ErasedSink for PySink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let obj = match Py::new(py, PyBuffer::from_erased(buf)) {
                Ok(o) => o,
                Err(e) => {
                    record_error(
                        &self.error_slot,
                        format_py_error(py, "sink: wrapping batch", &e),
                    );
                    return;
                }
            };
            if let Err(e) = self.obj.bind(py).call_method1("write_batch", (obj,)) {
                record_error(
                    &self.error_slot,
                    format_py_error(py, "sink.write_batch raised", &e),
                );
            }
        });
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            // flush is optional on the Python side.
            if self.obj.bind(py).hasattr("flush").unwrap_or(false)
                && let Err(e) = self.obj.bind(py).call_method0("flush")
            {
                record_error(
                    &self.error_slot,
                    format_py_error(py, "sink.flush raised", &e),
                );
            }
        });
        Ok(())
    }
}
