//! Custom Python stateful operators (`PyOperator`) + state access
//! (`PyStateHandle`).
//!
//! A Python user subclasses `rhei.Operator` and implements `process`,
//! optionally `on_watermark`, `on_timer`, `open`, `close`. `PyOperator` wraps
//! that instance as an [`ErasedBatchOperator`] and attaches it via
//! `DataflowGraph::add_erased_operator`.
//!
//! ## State bridge (no `unsafe`)
//!
//! `ErasedBatchOperator::process` is `async` and receives `&mut OperatorContext`
//! on a single Timely worker thread (driven by `rt.block_on`). Python is
//! synchronous and a `#[pyclass]` cannot hold a borrowed `&mut StateContext`.
//!
//! The body of each lifecycle method here is fully synchronous (no `.await`),
//! so for the duration of one Python call we *move* the owned `StateContext`
//! out of the context into an `Arc<Mutex<Option<StateContext>>>` shared with a
//! short-lived `PyStateHandle`, leaving a throwaway placeholder in its place,
//! then move the real state back when the call returns. On return the cell is
//! emptied, so a handle a user wrongly stashes raises a clear error on later
//! use and is safe to drop on any thread. Async state reads (`get_raw`) are
//! driven with `futures::executor::block_on` (the default `LocalBackend` needs
//! no Tokio reactor, so this does not nest `block_on`).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rhei_core::arrow::OperatorContext;
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;
use rhei_runtime::erased_batch::ErasedBatchOperator;
use rhei_runtime::erased_buffer::ErasedBuffer;

use crate::buffer::PyBuffer;

/// Build a throwaway in-memory `StateContext` to occupy `ctx.state` while the
/// real one is lent to Python. Never flushed (it is moved out before the call
/// returns and replaced with the real state).
fn placeholder_state() -> StateContext {
    let path = std::env::temp_dir().join("rhei-python-operator-placeholder");
    let backend = LocalBackend::new(path, None)
        .unwrap_or_else(|_| unreachable!("in-memory LocalBackend::new cannot fail"));
    StateContext::new(Box::new(backend))
}

/// Shared cell holding the operator's `StateContext` for the duration of one
/// synchronous Python lifecycle call. `Arc<Mutex<..>>` (not `Rc<RefCell<..>>`)
/// so `PyStateHandle` stays `Send`: a Python user who stashes the handle past
/// its call (an anti-pattern) can have it garbage-collected on any thread
/// without tripping pyo3's unsendable-drop assertion. Validity is enforced by
/// the `Option`, which the operator clears when the call returns.
type StateCell = Arc<Mutex<Option<StateContext>>>;

/// A short-lived handle giving Python `get`/`put`/`delete`/`register_timer`
/// access to the operator's keyed state. Valid only during the call it was
/// passed to; using it afterward raises `RuntimeError`.
#[pyclass(name = "State")]
pub struct PyStateHandle {
    cell: StateCell,
}

impl PyStateHandle {
    fn with_state<R>(&self, f: impl FnOnce(&mut StateContext) -> PyResult<R>) -> PyResult<R> {
        let mut guard = self
            .cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = guard.as_mut().ok_or_else(|| {
            PyRuntimeError::new_err(
                "rhei.State is only valid during the operator call it was passed to",
            )
        })?;
        f(state)
    }
}

#[pymethods]
impl PyStateHandle {
    /// Get the raw bytes stored at `key`, or `None`.
    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        let bytes = self.with_state(|state| {
            futures::executor::block_on(state.get_raw(key))
                .map_err(|e| PyRuntimeError::new_err(format!("state.get failed: {e}")))
        })?;
        Ok(bytes.map(|b| PyBytes::new(py, &b).unbind()))
    }

    /// Store raw `value` bytes at `key`.
    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.with_state(|state| {
            state.put_raw(key, value);
            Ok(())
        })
    }

    /// Delete the value at `key`.
    fn delete(&self, key: &[u8]) -> PyResult<()> {
        self.with_state(|state| {
            state.delete(key);
            Ok(())
        })
    }

    /// Register an event-time timer that fires `on_timer(timestamp, key, state)`
    /// once the watermark passes `timestamp`.
    fn register_timer(&self, timestamp: u64, key: &str) -> PyResult<()> {
        self.with_state(|state| {
            state.timers().register(timestamp, key.to_string());
            Ok(())
        })
    }
}

/// Wraps a Python `rhei.Operator` instance as an [`ErasedBatchOperator`].
pub struct PyOperator {
    /// The user's Python operator instance.
    obj: Py<PyAny>,
    /// Records the first fatal Python error so `run()` can re-raise it.
    error_slot: crate::dataflow::ErrorSlot,
}

impl PyOperator {
    pub fn new(obj: Py<PyAny>, error_slot: crate::dataflow::ErrorSlot) -> Self {
        Self { obj, error_slot }
    }

    /// Invoke a Python lifecycle method that returns `list[Buffer]`, lending it
    /// the operator state for the call. `args_builder` produces the positional
    /// args *before* the trailing `State` handle.
    ///
    /// The real `StateContext` is moved into a shared cell for the call and
    /// moved back afterward; a placeholder occupies `ctx.state` meanwhile. Even
    /// if Python stashes the handle, the cell is emptied on return so later use
    /// fails cleanly and the handle is safe to drop on any thread.
    fn call_emitting(
        &mut self,
        method: &str,
        ctx: &mut OperatorContext,
        args_builder: impl for<'py> FnOnce(Python<'py>) -> PyResult<Vec<Py<PyAny>>>,
    ) -> Vec<ErasedBuffer> {
        let real = std::mem::replace(&mut ctx.state, placeholder_state());
        let cell: StateCell = Arc::new(Mutex::new(Some(real)));

        let result = Python::with_gil(|py| {
            let handle = match Py::new(py, PyStateHandle { cell: cell.clone() }) {
                Ok(h) => h,
                Err(e) => return Err(format!("{method}: building state handle: {e}")),
            };
            let mut args = match args_builder(py) {
                Ok(a) => a,
                Err(e) => return Err(format_py(py, method, "building args", &e)),
            };
            args.push(handle.into_any());
            let py_args = pyo3::types::PyTuple::new(py, args)
                .map_err(|e| format_py(py, method, "building args tuple", &e))?;
            let ret = match self.obj.bind(py).call_method1(method, py_args) {
                Ok(r) => r,
                Err(e) => return Err(format_py(py, method, "raised", &e)),
            };
            buffers_from_return(&ret).map_err(|e| format_py(py, method, "return value", &e))
        });

        // Restore the real state regardless of outcome; this also invalidates
        // any handle Python may have stashed.
        if let Some(state) = cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            ctx.state = state;
        }

        match result {
            Ok(buffers) => buffers,
            Err(msg) => {
                crate::dataflow::record_error(&self.error_slot, msg);
                vec![]
            }
        }
    }
}

/// Format a Python error with operator/method context.
fn format_py(py: Python<'_>, method: &str, what: &str, err: &PyErr) -> String {
    let msg = err.value(py).str().map_or_else(
        |_| "<unprintable Python error>".to_string(),
        |s| s.to_string_lossy().into_owned(),
    );
    format!("operator.{method}: {what}: {msg}")
}

/// Convert a Python `list[Buffer]` (or a single `Buffer`, or `None`) returned by
/// an operator method into `Vec<ErasedBuffer>`.
fn buffers_from_return(ret: &Bound<'_, PyAny>) -> PyResult<Vec<ErasedBuffer>> {
    if ret.is_none() {
        return Ok(vec![]);
    }
    // A single Buffer is accepted as a convenience.
    if let Ok(buf) = ret.downcast::<PyBuffer>() {
        return Ok(vec![buf.borrow().inner.clone()]);
    }
    let mut out = Vec::new();
    let iter = ret.try_iter().map_err(|_| {
        PyValueError::new_err("operator method must return a list of rhei.Buffer (or None)")
    })?;
    for item in iter {
        let item = item?;
        let buf = item
            .downcast::<PyBuffer>()
            .map_err(|_| PyValueError::new_err("operator method returned a non-Buffer item"))?;
        out.push(buf.borrow().inner.clone());
    }
    Ok(out)
}

#[async_trait]
impl ErasedBatchOperator for PyOperator {
    async fn process(
        &mut self,
        input: ErasedBuffer,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        Ok(self.call_emitting("process", ctx, move |py| {
            let buf = Py::new(py, PyBuffer::from_erased(input))?;
            Ok(vec![buf.into_any()])
        }))
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        // Skip the call entirely if the operator does not define on_watermark.
        if !self.defines("on_watermark") {
            return Ok(vec![]);
        }
        Ok(self.call_emitting("on_watermark", ctx, move |py| {
            Ok(vec![watermark.into_pyobject(py)?.into_any().unbind()])
        }))
    }

    async fn open(&mut self, ctx: &mut OperatorContext) -> anyhow::Result<()> {
        if !self.defines("open") {
            return Ok(());
        }
        // open returns nothing meaningful; ignore any emitted buffers.
        let _ = self.call_emitting("open", ctx, |_py| Ok(vec![]));
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if !self.defines("close") {
            return Ok(());
        }
        Python::with_gil(|py| {
            if let Err(e) = self.obj.bind(py).call_method0("close") {
                crate::dataflow::record_error(
                    &self.error_slot,
                    format_py(py, "close", "raised", &e),
                );
            }
        });
        Ok(())
    }

    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        if !self.defines("on_timer") {
            return Ok(vec![]);
        }
        let key = key.to_string();
        Ok(self.call_emitting("on_timer", ctx, move |py| {
            Ok(vec![
                timestamp.into_pyobject(py)?.into_any().unbind(),
                key.into_pyobject(py)?.into_any().unbind(),
            ])
        }))
    }

    fn clone_erased(&self) -> Box<dyn ErasedBatchOperator> {
        Box::new(PyOperator {
            obj: Python::with_gil(|py| self.obj.clone_ref(py)),
            error_slot: self.error_slot.clone(),
        })
    }
}

impl PyOperator {
    /// Whether the Python operator instance defines `method` (vs inheriting a
    /// no-op default it didn't override). Cheap `hasattr` check.
    fn defines(&self, method: &str) -> bool {
        Python::with_gil(|py| self.obj.bind(py).hasattr(method).unwrap_or(false))
    }
}
