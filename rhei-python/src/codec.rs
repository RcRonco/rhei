//! Arrow ↔ Python row codec.
//!
//! Converts between a single row of an Arrow `RecordBatch` and a Python `dict`,
//! and builds a `RecordBatch` from a list of dicts given a target schema —
//! generically over a *runtime* Arrow schema (no compile-time `RheiSchema`).
//!
//! This is the foundation of the row-oriented Python API (`map`/`filter`/
//! `flat_map`/`inspect`/`key_by`) and is reused wherever Python code needs to
//! see Arrow data as Python values.
//!
//! Supported column types (MVP): the signed/unsigned integers, `Float32/64`,
//! `Boolean`, `Utf8`/`LargeUtf8`, `Binary`/`LargeBinary`. Nulls map to/from
//! Python `None`. Other types raise `NotImplementedError` with the offending
//! `DataType`, so unsupported schemas fail loudly at the row rather than
//! silently corrupting.

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder,
    StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// Read one cell (`row` of `column`) as a Python object, honoring nulls.
fn read_scalar(py: Python<'_>, column: &dyn Array, row: usize) -> PyResult<PyObject> {
    if column.is_null(row) {
        return Ok(py.None());
    }
    let obj = match column.data_type() {
        DataType::Int8 => column
            .as_primitive::<Int8Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Int16 => column
            .as_primitive::<Int16Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Int32 => column
            .as_primitive::<Int32Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Int64 => column
            .as_primitive::<Int64Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::UInt8 => column
            .as_primitive::<UInt8Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::UInt16 => column
            .as_primitive::<UInt16Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::UInt32 => column
            .as_primitive::<UInt32Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::UInt64 => column
            .as_primitive::<UInt64Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Float32 => column
            .as_primitive::<Float32Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Float64 => column
            .as_primitive::<Float64Type>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Boolean => column
            .as_boolean()
            .value(row)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind(),
        DataType::Utf8 => column
            .as_string::<i32>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::LargeUtf8 => column
            .as_string::<i64>()
            .value(row)
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        DataType::Binary => PyBytes::new(py, column.as_binary::<i32>().value(row))
            .into_any()
            .unbind(),
        DataType::LargeBinary => PyBytes::new(py, column.as_binary::<i64>().value(row))
            .into_any()
            .unbind(),
        other => {
            return Err(PyNotImplementedError::new_err(format!(
                "rhei row API does not yet support column type {other:?}; use map_batches for full Arrow access"
            )));
        }
    };
    Ok(obj)
}

/// Convert one row of `batch` into a Python `dict` keyed by field name.
pub fn batch_row_to_dict<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
    row: usize,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let schema = batch.schema();
    for (i, field) in schema.fields().iter().enumerate() {
        let value = read_scalar(py, batch.column(i).as_ref(), row)?;
        dict.set_item(field.name(), value)?;
    }
    Ok(dict)
}

/// Append one Python value to a type-erased Arrow builder. `None` appends a null.
#[allow(clippy::too_many_lines)]
fn append_value(
    builder: &mut dyn ArrayBuilder,
    data_type: &DataType,
    value: &Bound<'_, PyAny>,
    field_name: &str,
) -> PyResult<()> {
    let is_none = value.is_none();
    macro_rules! downcast {
        ($ty:ty) => {
            builder
                .as_any_mut()
                .downcast_mut::<$ty>()
                .ok_or_else(|| PyNotImplementedError::new_err("internal: builder type mismatch"))?
        };
    }
    match data_type {
        DataType::Int8 => {
            let b = downcast!(Int8Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Int16 => {
            let b = downcast!(Int16Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Int32 => {
            let b = downcast!(Int32Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Int64 => {
            let b = downcast!(Int64Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::UInt8 => {
            let b = downcast!(UInt8Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::UInt16 => {
            let b = downcast!(UInt16Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::UInt32 => {
            let b = downcast!(UInt32Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::UInt64 => {
            let b = downcast!(UInt64Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Float32 => {
            let b = downcast!(Float32Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Float64 => {
            let b = downcast!(Float64Builder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Boolean => {
            let b = downcast!(BooleanBuilder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract()?);
            }
        }
        DataType::Utf8 => {
            let b = downcast!(StringBuilder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract::<String>()?);
            }
        }
        DataType::LargeUtf8 => {
            let b = downcast!(LargeStringBuilder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract::<String>()?);
            }
        }
        DataType::Binary => {
            let b = downcast!(BinaryBuilder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract::<Vec<u8>>()?);
            }
        }
        DataType::LargeBinary => {
            let b = downcast!(LargeBinaryBuilder);
            if is_none {
                b.append_null();
            } else {
                b.append_value(value.extract::<Vec<u8>>()?);
            }
        }
        other => {
            return Err(PyNotImplementedError::new_err(format!(
                "rhei row API does not yet support column type {other:?} (field '{field_name}'); \
                 use map_batches for full Arrow access"
            )));
        }
    }
    Ok(())
}

/// Construct a fresh, empty type-erased builder for `data_type`.
fn new_builder(data_type: &DataType, capacity: usize) -> PyResult<Box<dyn ArrayBuilder>> {
    let b: Box<dyn ArrayBuilder> = match data_type {
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 16)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, capacity * 16)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 16)),
        DataType::LargeBinary => {
            Box::new(LargeBinaryBuilder::with_capacity(capacity, capacity * 16))
        }
        other => {
            return Err(PyNotImplementedError::new_err(format!(
                "rhei row API does not yet support column type {other:?}; \
                 use map_batches for full Arrow access"
            )));
        }
    };
    Ok(b)
}

/// Build a `RecordBatch` from a slice of Python `dict` rows and a target schema.
///
/// Each dict must contain every field by name. Missing keys append a null.
pub fn dicts_to_batch(rows: &[Bound<'_, PyAny>], schema: &SchemaRef) -> PyResult<RecordBatch> {
    let fields = schema.fields();
    let mut builders: Vec<Box<dyn ArrayBuilder>> = fields
        .iter()
        .map(|f| new_builder(f.data_type(), rows.len()))
        .collect::<PyResult<_>>()?;

    for row in rows {
        let dict = row
            .downcast::<PyDict>()
            .map_err(|_| PyNotImplementedError::new_err("row transform must return dict rows"))?;
        for (i, field) in fields.iter().enumerate() {
            let value = match dict.get_item(field.name())? {
                Some(v) => v,
                None => row.py().None().into_bound(row.py()),
            };
            append_value(
                builders[i].as_mut(),
                field.data_type(),
                &value,
                field.name(),
            )?;
        }
    }

    let columns: Vec<ArrayRef> = builders.iter_mut().map(ArrayBuilder::finish).collect();
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("failed to build RecordBatch: {e}"))
    })
}
