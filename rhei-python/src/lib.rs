//! Python bindings for the rhei stream-processing framework (PyO3).
//!
//! Exposes a thin layer over rhei-runtime's type-erased dataflow API so that
//! Python users can build and run Arrow-columnar pipelines.

use pyo3::prelude::*;

/// The compiled extension module. Named `_rhei` to match
/// `module-name = "rhei._rhei"` in `pyproject.toml`; the pure-Python
/// `rhei` package re-exports from it.
#[pymodule]
fn _rhei(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
