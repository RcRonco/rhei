//! L3: tumbling-window aggregation (`TumblingWindowAgg` + `Agg` specs).
//!
//! A declarative, watermark-driven tumbling window. The user supplies a window
//! size, a Python `key_fn(row) -> str`, a Python `time_fn(row) -> int` (event
//! time in millis), and a list of [`Agg`] specs. Aggregation runs entirely in
//! Rust on the Arrow columns; Python is invoked only for key/time extraction.
//!
//! Output schema is fixed and derived from the specs:
//! `[key: Utf8, window_start: Int64, window_end: Int64, <agg columns…>]`.
//! Windows are emitted and evicted when the watermark passes `window_end`.
//!
//! The built-in typed window operators are monomorphized over `T: RheiSchema`
//! and so are unreachable from the erased Python layer; this is a fresh erased
//! operator built on `RecordBatch` + arrow, per the L2–L5 strategy.
//!
//! MVP scope: tumbling windows only; sliding/session/count windows and joins
//! are deferred. Accumulators are held in operator memory (bounded by the set
//! of open windows) and are NOT yet checkpointed — a restart loses in-flight
//! windows. `key_by` upstream so each key lands on one worker.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use pyo3::prelude::*;
use rhei_core::arrow::OperatorContext;
use rhei_runtime::erased_batch::ErasedBatchOperator;
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

use crate::codec::batch_row_to_dict;
use crate::dataflow::{ErrorSlot, format_py_error, record_error};

/// The kind of aggregation an [`Agg`] computes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AggKind {
    Count,
    Sum,
    Min,
    Max,
    Mean,
}

/// A declarative aggregation: a kind, an optional source column (none for
/// `count`), and an output column name.
#[pyclass(name = "Agg", frozen)]
#[derive(Clone)]
pub struct PyAgg {
    kind: AggKind,
    column: Option<String>,
    output_name: String,
}

#[pymethods]
impl PyAgg {
    /// `COUNT(*)` over the window. Output column defaults to `"count"`.
    #[staticmethod]
    #[pyo3(signature = (name = None))]
    fn count(name: Option<String>) -> Self {
        Self {
            kind: AggKind::Count,
            column: None,
            output_name: name.unwrap_or_else(|| "count".to_string()),
        }
    }

    /// `SUM(column)` (nulls ignored). Output defaults to `"sum_<column>"`.
    #[staticmethod]
    #[pyo3(signature = (column, name = None))]
    fn sum(column: String, name: Option<String>) -> Self {
        let output_name = name.unwrap_or_else(|| format!("sum_{column}"));
        Self {
            kind: AggKind::Sum,
            column: Some(column),
            output_name,
        }
    }

    /// `MIN(column)` (nulls ignored). Output defaults to `"min_<column>"`.
    #[staticmethod]
    #[pyo3(signature = (column, name = None))]
    fn min(column: String, name: Option<String>) -> Self {
        let output_name = name.unwrap_or_else(|| format!("min_{column}"));
        Self {
            kind: AggKind::Min,
            column: Some(column),
            output_name,
        }
    }

    /// `MAX(column)` (nulls ignored). Output defaults to `"max_<column>"`.
    #[staticmethod]
    #[pyo3(signature = (column, name = None))]
    fn max(column: String, name: Option<String>) -> Self {
        let output_name = name.unwrap_or_else(|| format!("max_{column}"));
        Self {
            kind: AggKind::Max,
            column: Some(column),
            output_name,
        }
    }

    /// `MEAN(column)` (nulls ignored). Output defaults to `"mean_<column>"`.
    #[staticmethod]
    #[pyo3(signature = (column, name = None))]
    fn mean(column: String, name: Option<String>) -> Self {
        let output_name = name.unwrap_or_else(|| format!("mean_{column}"));
        Self {
            kind: AggKind::Mean,
            column: Some(column),
            output_name,
        }
    }
}

impl PyAgg {
    /// The Arrow output type for this aggregation: `count` is `Int64`, the rest
    /// are `Float64`.
    fn output_type(&self) -> DataType {
        match self.kind {
            AggKind::Count => DataType::Int64,
            _ => DataType::Float64,
        }
    }
}

/// Running accumulator state for one aggregation within one window.
#[derive(Clone)]
enum Acc {
    Count(i64),
    Sum(Option<f64>),
    Min(Option<f64>),
    Max(Option<f64>),
    Mean { sum: f64, count: i64 },
}

impl Acc {
    fn new(kind: AggKind) -> Self {
        match kind {
            AggKind::Count => Acc::Count(0),
            AggKind::Sum => Acc::Sum(None),
            AggKind::Min => Acc::Min(None),
            AggKind::Max => Acc::Max(None),
            AggKind::Mean => Acc::Mean { sum: 0.0, count: 0 },
        }
    }

    /// Fold a row into the accumulator. `value` is the source column's value at
    /// this row (`None` for null or for `count`).
    fn update(&mut self, value: Option<f64>) {
        match self {
            Acc::Count(n) => *n += 1,
            Acc::Sum(s) => {
                if let Some(v) = value {
                    *s = Some(s.unwrap_or(0.0) + v);
                }
            }
            Acc::Min(m) => {
                if let Some(v) = value {
                    *m = Some(m.map_or(v, |cur| cur.min(v)));
                }
            }
            Acc::Max(m) => {
                if let Some(v) = value {
                    *m = Some(m.map_or(v, |cur| cur.max(v)));
                }
            }
            Acc::Mean { sum, count } => {
                if let Some(v) = value {
                    *sum += v;
                    *count += 1;
                }
            }
        }
    }
}

/// A tumbling-window aggregation operator over the erased layer.
pub struct TumblingWindowAgg {
    window_size_ms: i64,
    key_fn: Py<PyAny>,
    time_fn: Py<PyAny>,
    specs: Vec<PyAgg>,
    out_schema: SchemaRef,
    out_schema_id: u64,
    error_slot: ErrorSlot,
    /// Open windows: `(window_start, key) -> per-spec accumulators`.
    windows: HashMap<(i64, String), Vec<Acc>>,
}

impl TumblingWindowAgg {
    pub fn new(
        window_size_ms: i64,
        key_fn: Py<PyAny>,
        time_fn: Py<PyAny>,
        specs: Vec<PyAgg>,
        error_slot: ErrorSlot,
    ) -> Self {
        let out_schema = build_output_schema(&specs);
        let out_schema_id = schema_hash_of(&out_schema);
        Self {
            window_size_ms,
            key_fn,
            time_fn,
            specs,
            out_schema,
            out_schema_id,
            error_slot,
            windows: HashMap::new(),
        }
    }
}

/// Build the fixed output schema `[key, window_start, window_end, <aggs>]`.
fn build_output_schema(specs: &[PyAgg]) -> SchemaRef {
    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
    ];
    for spec in specs {
        // Aggregations over an empty/all-null window yield null.
        fields.push(Field::new(&spec.output_name, spec.output_type(), true));
    }
    Arc::new(Schema::new(fields))
}

/// Read a column cell as `f64`, widening any numeric type. Non-numeric or null
/// yields `None`.
fn read_numeric(column: &dyn Array, row: usize) -> Option<f64> {
    if column.is_null(row) {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let v = match column.data_type() {
        DataType::Int8 => column.as_primitive::<Int8Type>().value(row) as f64,
        DataType::Int16 => column.as_primitive::<Int16Type>().value(row) as f64,
        DataType::Int32 => column.as_primitive::<Int32Type>().value(row) as f64,
        DataType::Int64 => column.as_primitive::<Int64Type>().value(row) as f64,
        DataType::UInt8 => column.as_primitive::<UInt8Type>().value(row) as f64,
        DataType::UInt16 => column.as_primitive::<UInt16Type>().value(row) as f64,
        DataType::UInt32 => column.as_primitive::<UInt32Type>().value(row) as f64,
        DataType::UInt64 => column.as_primitive::<UInt64Type>().value(row) as f64,
        DataType::Float32 => f64::from(column.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => column.as_primitive::<Float64Type>().value(row),
        _ => return None,
    };
    Some(v)
}

impl TumblingWindowAgg {
    /// Build an output batch for the given emitted `(window_start, key, accs)`
    /// rows, ordered as the schema dictates.
    fn build_batch(&self, rows: Vec<(i64, String, Vec<Acc>)>) -> anyhow::Result<RecordBatch> {
        let mut key_b = StringBuilder::new();
        let mut start_b = Int64Builder::new();
        let mut end_b = Int64Builder::new();
        // One builder per agg spec, typed by the spec.
        let mut count_builders: Vec<Option<Int64Builder>> = self
            .specs
            .iter()
            .map(|s| (s.kind == AggKind::Count).then(Int64Builder::new))
            .collect();
        let mut float_builders: Vec<Option<Float64Builder>> = self
            .specs
            .iter()
            .map(|s| (s.kind != AggKind::Count).then(Float64Builder::new))
            .collect();

        for (start, key, accs) in rows {
            key_b.append_value(&key);
            start_b.append_value(start);
            end_b.append_value(start + self.window_size_ms);
            for (i, acc) in accs.into_iter().enumerate() {
                match acc {
                    Acc::Count(n) => count_builders[i]
                        .as_mut()
                        .expect("count builder")
                        .append_value(n),
                    Acc::Sum(v) | Acc::Min(v) | Acc::Max(v) => {
                        let b = float_builders[i].as_mut().expect("float builder");
                        match v {
                            Some(x) => b.append_value(x),
                            None => b.append_null(),
                        }
                    }
                    Acc::Mean { sum, count } => {
                        let b = float_builders[i].as_mut().expect("float builder");
                        if count > 0 {
                            #[allow(clippy::cast_precision_loss)]
                            b.append_value(sum / count as f64);
                        } else {
                            b.append_null();
                        }
                    }
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(key_b.finish()),
            Arc::new(start_b.finish()),
            Arc::new(end_b.finish()),
        ];
        for i in 0..self.specs.len() {
            if let Some(b) = count_builders[i].as_mut() {
                columns.push(Arc::new(b.finish()));
            } else if let Some(b) = float_builders[i].as_mut() {
                columns.push(Arc::new(b.finish()));
            }
        }
        Ok(RecordBatch::try_new(self.out_schema.clone(), columns)?)
    }
}

#[async_trait]
impl ErasedBatchOperator for TumblingWindowAgg {
    async fn process(
        &mut self,
        input: ErasedBuffer,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        let batch = match input.mask() {
            Some(mask) => arrow::compute::filter_record_batch(input.as_record_batch(), mask)?,
            None => input.as_record_batch().clone(),
        };

        // Pre-resolve each spec's source column index against this batch.
        let col_idx: Vec<Option<usize>> = self
            .specs
            .iter()
            .map(|s| {
                s.column
                    .as_ref()
                    .and_then(|c| batch.schema().index_of(c).ok())
            })
            .collect();

        Python::with_gil(|py| {
            for row in 0..batch.num_rows() {
                let dict = match batch_row_to_dict(py, &batch, row) {
                    Ok(d) => d,
                    Err(e) => {
                        record_error(
                            &self.error_slot,
                            format_py_error(py, "window: reading row", &e),
                        );
                        return;
                    }
                };
                let key = match self
                    .key_fn
                    .bind(py)
                    .call1((dict.clone(),))
                    .and_then(|r| r.extract::<String>())
                {
                    Ok(k) => k,
                    Err(e) => {
                        record_error(&self.error_slot, format_py_error(py, "window: key_fn", &e));
                        return;
                    }
                };
                let ts = match self
                    .time_fn
                    .bind(py)
                    .call1((dict,))
                    .and_then(|r| r.extract::<i64>())
                {
                    Ok(t) => t,
                    Err(e) => {
                        record_error(&self.error_slot, format_py_error(py, "window: time_fn", &e));
                        return;
                    }
                };
                let window_start = ts.div_euclid(self.window_size_ms) * self.window_size_ms;
                let accs = self
                    .windows
                    .entry((window_start, key))
                    .or_insert_with(|| self.specs.iter().map(|s| Acc::new(s.kind)).collect());
                for (i, acc) in accs.iter_mut().enumerate() {
                    let value = match col_idx[i] {
                        Some(ci) => read_numeric(batch.column(ci).as_ref(), row),
                        None => None,
                    };
                    acc.update(value);
                }
            }
        });
        Ok(vec![])
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        // Emit and evict windows whose end has passed the watermark.
        let wm = i64::try_from(watermark).unwrap_or(i64::MAX);
        let ready: Vec<(i64, String)> = self
            .windows
            .keys()
            .filter(|(start, _)| start.saturating_add(self.window_size_ms) <= wm)
            .cloned()
            .collect();
        if ready.is_empty() {
            return Ok(vec![]);
        }
        let mut rows = Vec::with_capacity(ready.len());
        for k in ready {
            if let Some(accs) = self.windows.remove(&k) {
                rows.push((k.0, k.1, accs));
            }
        }
        let batch = self.build_batch(rows)?;
        Ok(vec![ErasedBuffer::from_parts(
            batch,
            None,
            self.out_schema_id,
        )])
    }

    async fn open(&mut self, _ctx: &mut OperatorContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_timer(
        &mut self,
        _timestamp: u64,
        _key: &str,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        Ok(vec![])
    }

    fn clone_erased(&self) -> Box<dyn ErasedBatchOperator> {
        Box::new(TumblingWindowAgg {
            window_size_ms: self.window_size_ms,
            key_fn: Python::with_gil(|py| self.key_fn.clone_ref(py)),
            time_fn: Python::with_gil(|py| self.time_fn.clone_ref(py)),
            specs: self.specs.clone(),
            out_schema: self.out_schema.clone(),
            out_schema_id: self.out_schema_id,
            error_slot: self.error_slot.clone(),
            windows: HashMap::new(),
        })
    }
}
