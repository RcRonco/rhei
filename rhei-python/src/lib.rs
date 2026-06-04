//! Python bindings for the rhei stream-processing framework (PyO3).
//!
//! Exposes a thin layer over rhei-runtime's type-erased dataflow API so that
//! Python users can build and run Arrow-columnar pipelines.

use pyo3::prelude::*;

mod buffer;
mod dataflow;

use buffer::PyBuffer;
use dataflow::{PyCollectHandle, PyDataflow, PyStream};

/// The compiled extension module. Named `_rhei` to match
/// `module-name = "rhei._rhei"` in `pyproject.toml`; the pure-Python
/// `rhei` package re-exports from it.
#[pymodule]
fn _rhei(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyBuffer>()?;
    m.add_class::<PyDataflow>()?;
    m.add_class::<PyStream>()?;
    m.add_class::<PyCollectHandle>()?;
    Ok(())
}
