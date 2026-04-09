//! Python bindings for the Rhei stream processing engine via `PyO3`.
//!
//! Provides a `Pipeline` class for building and running stream pipelines
//! from Python, with `VecSource` and `PrintSink` built-in connectors.

use pyo3::prelude::*;

mod connectors;
mod pipeline;
mod sink;
mod source;
mod stream;

/// The native extension module, imported as `rhei._native`.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<pipeline::Pipeline>()?;
    m.add_class::<stream::PyStream>()?;
    m.add_class::<connectors::PyVecSource>()?;
    m.add_class::<connectors::PyPrintSink>()?;
    Ok(())
}
