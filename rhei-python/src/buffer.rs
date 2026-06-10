//! `PyBuffer`: the Python-facing handle around rhei's type-erased Arrow buffer.
//!
//! A `Buffer` wraps an [`ErasedBuffer`] (a `RecordBatch` + schema id). Conversion
//! to/from pyarrow is zero-copy via the Arrow C Data Interface
//! (`arrow::pyarrow`).

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use pyo3::prelude::*;

use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

/// An Arrow batch flowing through a rhei pipeline.
///
/// Construct from a pyarrow `RecordBatch` with [`PyBuffer::from_arrow`] and read
/// it back with [`PyBuffer::to_arrow`]. Both are zero-copy.
#[pyclass(name = "Buffer", frozen)]
pub struct PyBuffer {
    pub(crate) inner: ErasedBuffer,
}

impl PyBuffer {
    /// Wrap an existing `ErasedBuffer` (used internally when receiving batches
    /// from the runtime).
    pub(crate) fn from_erased(inner: ErasedBuffer) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyBuffer {
    /// Build a `Buffer` from a pyarrow `RecordBatch` (zero-copy).
    ///
    /// The schema id is derived from the batch's Arrow schema, so the buffer
    /// interoperates with any rhei operator expecting that schema.
    #[staticmethod]
    fn from_arrow(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let batch = RecordBatch::from_pyarrow_bound(obj)?;
        let schema_id = schema_hash_of(batch.schema_ref());
        Ok(Self {
            inner: ErasedBuffer::from_parts(batch, None, schema_id),
        })
    }

    /// Export this buffer as a pyarrow `RecordBatch` (zero-copy).
    fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.inner.as_record_batch().to_pyarrow(py)
    }

    /// Number of rows in the buffer.
    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    fn __len__(&self) -> usize {
        self.inner.num_rows()
    }

    fn __repr__(&self) -> String {
        format!("Buffer(rows={})", self.inner.num_rows())
    }
}
