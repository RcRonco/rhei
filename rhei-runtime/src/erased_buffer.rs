//! Type-erased Arrow buffer for Timely dataflow channels.
//!
//! [`ErasedBuffer`] wraps a `RecordBatch` + optional `BooleanArray` mask
//! without knowing the concrete `RheiSchema` type at compile time. This
//! enables flowing Arrow buffers through Timely's untyped `Exchange` channels.
//!
//! Serialization uses Arrow IPC (streaming format) so buffers can cross
//! process boundaries for distributed execution.

use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;

use rhei_core::arrow::{RheiBuffer, RheiSchema};

/// Type-erased key extraction function: given a `RecordBatch` and row index,
/// returns the key string for that row. Used by `key_by` exchange.
pub type KeyFn = Arc<dyn Fn(&RecordBatch, usize) -> String + Send + Sync>;

/// Type-erased Arrow buffer for flowing through Timely dataflow channels.
///
/// Contains a `RecordBatch` (the actual columnar data) and an optional
/// `BooleanArray` mask (selection vector). The `schema_id` field is a
/// stable hash of the schema for runtime type checking.
#[derive(Clone)]
pub struct ErasedBuffer {
    batch: RecordBatch,
    mask: Option<BooleanArray>,
    schema_id: u64,
    /// Transient routing hint set by `key_by`, read by Exchange pact.
    /// Not serialized — defaults to `None` after deserialization.
    exchange_target: Option<u64>,
}

impl std::fmt::Debug for ErasedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedBuffer")
            .field("rows", &self.batch.num_rows())
            .field("cols", &self.batch.num_columns())
            .field("schema_id", &self.schema_id)
            .field("has_mask", &self.mask.is_some())
            .finish_non_exhaustive()
    }
}

impl ErasedBuffer {
    /// Create an `ErasedBuffer` from a typed `RheiBuffer<T>`.
    pub fn from_typed<T: RheiSchema>(buffer: RheiBuffer<T>) -> Self {
        let schema_id = schema_hash::<T>();
        let mask = buffer.mask().cloned();
        let batch = buffer.as_record_batch().clone();
        Self {
            batch,
            mask,
            schema_id,
            exchange_target: None,
        }
    }

    /// Construct an `ErasedBuffer` directly from its parts, without a
    /// compile-time `RheiSchema` type.
    ///
    /// `schema_id` must be computed with [`schema_hash_of`] from an Arrow
    /// [`Schema`] equal to `batch.schema()` for the buffer to be
    /// [`downcast`](Self::downcast)-able to a typed `RheiBuffer<T>`. This is
    /// the runtime-typed entry point used by dynamic graph builders.
    pub fn from_parts(batch: RecordBatch, mask: Option<BooleanArray>, schema_id: u64) -> Self {
        Self {
            batch,
            mask,
            schema_id,
            exchange_target: None,
        }
    }

    /// Downcast back to a typed `RheiBuffer<T>`.
    ///
    /// Returns `Err` if the `schema_id` doesn't match (type mismatch).
    pub fn downcast<T: RheiSchema>(self) -> Result<RheiBuffer<T>, ErasedBufferError> {
        let expected = schema_hash::<T>();
        if self.schema_id != expected {
            return Err(ErasedBufferError::SchemaMismatch {
                expected,
                actual: self.schema_id,
            });
        }

        let mut buf = RheiBuffer::from_record_batch(self.batch);
        if let Some(mask) = self.mask {
            buf = buf.with_mask(mask);
        }
        Ok(buf)
    }

    /// Returns the number of physical rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the schema ID (stable hash of the Arrow schema).
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// Returns a reference to the underlying `RecordBatch`.
    pub fn as_record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Returns the optional selection mask. Rows where the mask is `false` are
    /// logically filtered out; consumers that read [`as_record_batch`] directly
    /// must apply the mask (e.g. via `arrow::compute::filter_record_batch`).
    ///
    /// [`as_record_batch`]: Self::as_record_batch
    pub fn mask(&self) -> Option<&BooleanArray> {
        self.mask.as_ref()
    }

    /// Set the exchange routing target (worker index).
    #[allow(dead_code)]
    pub(crate) fn with_exchange_target(mut self, target: u64) -> Self {
        self.exchange_target = Some(target);
        self
    }

    /// Get the exchange routing target, if set.
    pub(crate) fn exchange_target(&self) -> Option<u64> {
        self.exchange_target
    }

    /// Concatenate multiple buffers into one using Arrow's `concat_batches`.
    ///
    /// All buffers must have the same schema. Returns `None` if the input is empty.
    /// Masks are applied (compacted) before concatenation.
    pub(crate) fn concat(buffers: Vec<ErasedBuffer>) -> Option<ErasedBuffer> {
        if buffers.is_empty() {
            return None;
        }
        if buffers.len() == 1 {
            return Some(buffers.into_iter().next().unwrap_or_else(|| unreachable!()));
        }
        let schema = buffers[0].batch.schema();
        let schema_id = buffers[0].schema_id;
        let batches: Vec<RecordBatch> = buffers
            .into_iter()
            .map(|b| {
                if let Some(mask) = b.mask {
                    arrow::compute::filter_record_batch(&b.batch, &mask).unwrap_or(b.batch)
                } else {
                    b.batch
                }
            })
            .collect();
        let combined = arrow::compute::concat_batches(&schema, batches.iter())
            .unwrap_or_else(|_| RecordBatch::new_empty(schema));
        Some(ErasedBuffer {
            batch: combined,
            mask: None,
            schema_id,
            exchange_target: None,
        })
    }

    /// Partition this buffer into per-worker sub-buffers based on key hashes.
    ///
    /// For each row, extracts the key via `key_fn`, hashes it with seahash,
    /// and assigns it to `hash % num_workers`. Returns one `ErasedBuffer` per
    /// non-empty partition, tagged with `exchange_target`.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn partition_for_exchange(
        &self,
        key_fn: &KeyFn,
        num_workers: usize,
    ) -> Vec<ErasedBuffer> {
        if num_workers <= 1 {
            return vec![self.clone()];
        }

        let num_rows = self.batch.num_rows();
        if num_rows == 0 {
            return vec![];
        }

        // Assign each logical row to a worker.
        let mut worker_rows: Vec<Vec<usize>> = vec![vec![]; num_workers];
        for row_idx in 0..num_rows {
            if let Some(ref mask) = self.mask
                && !mask.value(row_idx)
            {
                continue;
            }
            let key = key_fn(&self.batch, row_idx);
            let target = (seahash::hash(key.as_bytes()) as usize) % num_workers;
            worker_rows[target].push(row_idx);
        }

        // Build per-worker sub-buffers using Arrow take kernel.
        let mut result = Vec::with_capacity(num_workers);
        for (worker_idx, rows) in worker_rows.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let indices =
                arrow_array::UInt32Array::from(rows.iter().map(|&r| r as u32).collect::<Vec<_>>());
            let columns: Vec<Arc<dyn arrow_array::Array>> = self
                .batch
                .columns()
                .iter()
                .filter_map(|col| arrow::compute::take(col.as_ref(), &indices, None).ok())
                .collect();
            if let Ok(sub_batch) = RecordBatch::try_new(self.batch.schema(), columns) {
                let buf = ErasedBuffer {
                    batch: sub_batch,
                    mask: None,
                    schema_id: self.schema_id,
                    exchange_target: Some(worker_idx as u64),
                };
                result.push(buf);
            }
        }
        result
    }
}

/// Errors when working with `ErasedBuffer`.
#[derive(Debug, Clone)]
pub enum ErasedBufferError {
    /// The schema ID of the buffer doesn't match the expected type.
    SchemaMismatch {
        /// Expected schema hash.
        expected: u64,
        /// Actual schema hash in the buffer.
        actual: u64,
    },
}

impl std::fmt::Display for ErasedBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaMismatch { expected, actual } => {
                write!(
                    f,
                    "ErasedBuffer schema mismatch: expected {expected:#x}, got {actual:#x}"
                )
            }
        }
    }
}

impl std::error::Error for ErasedBufferError {}

/// Compute the stable schema id for an Arrow [`Schema`].
///
/// This is the same hash [`ErasedBuffer::from_typed`] derives from a
/// `RheiSchema` type, exposed for callers that only have a runtime
/// [`Schema`] (e.g. dynamically-typed graph builders). A buffer built via
/// [`ErasedBuffer::from_parts`] with `schema_hash_of(&schema)` will
/// [`downcast`](ErasedBuffer::downcast) to any `T` whose
/// `arrow_schema()` equals `schema`.
///
/// The hash is derived from the schema's full `Debug` representation, so
/// **field nullability and schema/field metadata are part of the identity** —
/// two otherwise structurally-identical schemas that differ only in a field's
/// `nullable` flag or in attached metadata hash to *different* ids. Callers
/// bridging an external schema (e.g. a `pyarrow.Schema`, where fields default
/// to `nullable = true`) must reproduce the target `T`'s nullability and
/// metadata exactly, or `downcast` will fail and the buffer will be dropped.
pub fn schema_hash_of(schema: &Schema) -> u64 {
    let schema_str = format!("{schema:?}");
    seahash::hash(schema_str.as_bytes())
}

/// Compute a stable hash for a `RheiSchema` type based on its Arrow schema.
fn schema_hash<T: RheiSchema>() -> u64 {
    schema_hash_of(&T::arrow_schema())
}

// ── Serde for Timely Exchange ─────────────────────────────────────────

impl serde::Serialize for ErasedBuffer {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;

        let ipc_bytes = serialize_batch(&self.batch).map_err(serde::ser::Error::custom)?;
        let mask_bytes = self.mask.as_ref().map(serialize_mask);

        let mut tup = serializer.serialize_tuple(3)?;
        tup.serialize_element(&self.schema_id)?;
        tup.serialize_element(&ipc_bytes)?;
        tup.serialize_element(&mask_bytes)?;
        tup.end()
    }
}

impl<'de> serde::Deserialize<'de> for ErasedBuffer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (schema_id, ipc_bytes, mask_bytes): (u64, Vec<u8>, Option<Vec<u8>>) =
            serde::Deserialize::deserialize(deserializer)?;

        let batch = deserialize_batch(&ipc_bytes).map_err(serde::de::Error::custom)?;
        let mask = mask_bytes.map(|b| deserialize_mask(&b));

        Ok(Self {
            batch,
            mask,
            schema_id,
            exchange_target: None,
        })
    }
}

fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

fn deserialize_batch(bytes: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batches: Vec<RecordBatch> = reader.into_iter().collect::<Result<Vec<_>, _>>()?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| arrow::error::ArrowError::IpcError("empty IPC stream".to_string()))
}

#[allow(clippy::expect_used)]
fn serialize_mask(mask: &BooleanArray) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "_mask",
            arrow_schema::DataType::Boolean,
            false,
        )])),
        vec![Arc::new(mask.clone())],
    )
    .expect("mask batch construction cannot fail");
    serialize_batch(&batch).expect("mask serialization cannot fail")
}

#[allow(clippy::expect_used)]
fn deserialize_mask(bytes: &[u8]) -> BooleanArray {
    let batch = deserialize_batch(bytes).expect("mask deserialization cannot fail");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("mask column must be BooleanArray")
        .clone()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
mod tests {
    use arrow_array::builder::ArrayBuilder;

    use super::*;
    use rhei_core::arrow::{RheiBuilder, RheiSchema};

    struct TestRow {
        id: i64,
        name: String,
    }

    struct TestBuilder {
        id: arrow_array::builder::Int64Builder,
        name: arrow_array::builder::StringBuilder,
    }

    #[allow(dead_code)]
    struct TestView<'a> {
        id: i64,
        name: &'a str,
    }

    #[allow(dead_code)]
    struct TestColumns<'a> {
        id: &'a arrow_array::Int64Array,
        name: &'a arrow_array::StringArray,
    }

    impl RheiBuilder for TestBuilder {
        type Item = TestRow;

        fn append(&mut self, item: TestRow) {
            self.id.append_value(item.id);
            self.name.append_value(&item.name);
        }

        fn append_null(&mut self) {
            self.id.append_null();
            self.name.append_null();
        }

        fn len(&self) -> usize {
            self.id.len()
        }

        fn finish(mut self) -> RecordBatch {
            RecordBatch::try_new(
                TestRow::arrow_schema(),
                vec![Arc::new(self.id.finish()), Arc::new(self.name.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchema for TestRow {
        type Builder = TestBuilder;
        type View<'a> = TestView<'a>;
        type Columns<'a> = TestColumns<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            TestBuilder {
                id: arrow_array::builder::Int64Builder::with_capacity(capacity),
                name: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            TestView {
                id: batch.column(0).as_primitive::<Int64Type>().value(index),
                name: batch.column(1).as_string::<i32>().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            TestColumns {
                id: batch.column(0).as_primitive::<Int64Type>(),
                name: batch.column(1).as_string::<i32>(),
            }
        }
    }

    #[test]
    fn roundtrip_typed_to_erased_and_back() {
        let mut builder = TestRow::builder(2);
        builder.append(TestRow {
            id: 1,
            name: "a".into(),
        });
        builder.append(TestRow {
            id: 2,
            name: "b".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);

        let erased = ErasedBuffer::from_typed(buf);
        assert_eq!(erased.num_rows(), 2);

        let restored: RheiBuffer<TestRow> = erased.downcast().unwrap();
        assert_eq!(restored.len(), 2);

        let v = TestRow::view(restored.as_record_batch(), 0);
        assert_eq!(v.id, 1);
        assert_eq!(v.name, "a");
    }

    #[test]
    fn roundtrip_with_mask() {
        let mut builder = TestRow::builder(3);
        builder.append(TestRow {
            id: 1,
            name: "a".into(),
        });
        builder.append(TestRow {
            id: 2,
            name: "b".into(),
        });
        builder.append(TestRow {
            id: 3,
            name: "c".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let masked = buf.with_mask(BooleanArray::from(vec![true, false, true]));

        let erased = ErasedBuffer::from_typed(masked);
        let restored: RheiBuffer<TestRow> = erased.downcast().unwrap();
        assert_eq!(restored.len(), 2); // only unmasked rows
        assert_eq!(restored.physical_len(), 3);
    }

    #[test]
    fn schema_hash_of_matches_typed_hash() {
        // schema_hash_of(&T::arrow_schema()) must equal the id produced by from_typed::<T>.
        let mut builder = TestRow::builder(1);
        builder.append(TestRow {
            id: 7,
            name: "z".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let computed = super::schema_hash_of(&TestRow::arrow_schema());
        assert_eq!(erased.schema_id(), computed);
    }

    #[test]
    fn from_parts_roundtrips_via_downcast() {
        // A buffer constructed from raw parts (no compile-time T) must downcast
        // back to T when the schema_id is computed from T's schema.
        let schema = TestRow::arrow_schema();
        let id_array = arrow_array::Int64Array::from(vec![11_i64, 22]);
        let name_array = arrow_array::StringArray::from(vec!["p", "q"]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let schema_id = super::schema_hash_of(&schema);
        let erased = ErasedBuffer::from_parts(batch, None, schema_id);
        assert_eq!(erased.num_rows(), 2);

        let typed: RheiBuffer<TestRow> = erased.downcast().unwrap();
        assert_eq!(typed.len(), 2);
        let v = TestRow::view(typed.as_record_batch(), 1);
        assert_eq!(v.id, 22);
        assert_eq!(v.name, "q");
    }

    #[test]
    fn schema_mismatch_returns_error() {
        let mut builder = TestRow::builder(1);
        builder.append(TestRow {
            id: 1,
            name: "a".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);

        let mut erased = ErasedBuffer::from_typed(buf);
        erased.schema_id = 999; // corrupt the schema ID

        let result = erased.downcast::<TestRow>();
        assert!(result.is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let mut builder = TestRow::builder(2);
        builder.append(TestRow {
            id: 10,
            name: "x".into(),
        });
        builder.append(TestRow {
            id: 20,
            name: "y".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);

        let erased = ErasedBuffer::from_typed(buf);
        let bytes = bincode::serialize(&erased).unwrap();
        let restored: ErasedBuffer = bincode::deserialize(&bytes).unwrap();

        assert_eq!(restored.num_rows(), 2);
        let typed: RheiBuffer<TestRow> = restored.downcast().unwrap();
        let v = TestRow::view(typed.as_record_batch(), 1);
        assert_eq!(v.id, 20);
        assert_eq!(v.name, "y");
    }

    #[test]
    fn serde_roundtrip_with_mask() {
        let mut builder = TestRow::builder(3);
        builder.append(TestRow {
            id: 1,
            name: "a".into(),
        });
        builder.append(TestRow {
            id: 2,
            name: "b".into(),
        });
        builder.append(TestRow {
            id: 3,
            name: "c".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let masked = buf.with_mask(BooleanArray::from(vec![true, false, true]));

        let erased = ErasedBuffer::from_typed(masked);
        let bytes = bincode::serialize(&erased).unwrap();
        let restored: ErasedBuffer = bincode::deserialize(&bytes).unwrap();

        let typed: RheiBuffer<TestRow> = restored.downcast().unwrap();
        assert_eq!(typed.len(), 2);
        assert_eq!(typed.physical_len(), 3);
    }

    #[test]
    fn partition_for_exchange_no_loss() {
        let mut builder = TestRow::builder(6);
        for (i, name) in ["alice", "bob", "alice", "charlie", "bob", "alice"]
            .iter()
            .enumerate()
        {
            builder.append(TestRow {
                id: i as i64,
                name: name.to_string(),
            });
        }
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let key_fn: KeyFn = Arc::new(|batch: &RecordBatch, row_idx: usize| {
            use arrow_array::cast::AsArray;
            batch
                .column(1)
                .as_string::<i32>()
                .value(row_idx)
                .to_string()
        });

        let partitions = erased.partition_for_exchange(&key_fn, 3);
        let total_rows: usize = partitions.iter().map(ErasedBuffer::num_rows).sum();
        assert_eq!(total_rows, 6, "no rows should be lost during partitioning");
    }

    #[test]
    fn partition_for_exchange_deterministic() {
        let mut builder = TestRow::builder(4);
        for name in ["alpha", "beta", "gamma", "delta"] {
            builder.append(TestRow {
                id: 0,
                name: name.to_string(),
            });
        }
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let key_fn: KeyFn = Arc::new(|batch: &RecordBatch, row_idx: usize| {
            use arrow_array::cast::AsArray;
            batch
                .column(1)
                .as_string::<i32>()
                .value(row_idx)
                .to_string()
        });

        let p1 = erased.partition_for_exchange(&key_fn, 4);
        let p2 = erased.partition_for_exchange(&key_fn, 4);
        assert_eq!(
            p1.len(),
            p2.len(),
            "partition count should be deterministic"
        );
        for (a, b) in p1.iter().zip(p2.iter()) {
            assert_eq!(a.exchange_target(), b.exchange_target());
            assert_eq!(a.num_rows(), b.num_rows());
        }
    }

    #[test]
    fn partition_for_exchange_matches_partition_key() {
        use crate::executor::partition_key;

        let mut builder = TestRow::builder(4);
        for name in ["alpha", "beta", "gamma", "delta"] {
            builder.append(TestRow {
                id: 0,
                name: name.to_string(),
            });
        }
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let key_fn: KeyFn = Arc::new(|batch: &RecordBatch, row_idx: usize| {
            use arrow_array::cast::AsArray;
            batch
                .column(1)
                .as_string::<i32>()
                .value(row_idx)
                .to_string()
        });

        let partitions = erased.partition_for_exchange(&key_fn, 4);
        for part in &partitions {
            let target = part.exchange_target().unwrap() as usize;
            // Verify every row in this partition hashes to the same target.
            for row in 0..part.num_rows() {
                let key = key_fn(part.as_record_batch(), row);
                let expected = partition_key(&key, 4);
                assert_eq!(target, expected, "key '{key}' routed to wrong worker");
            }
        }
    }

    #[test]
    fn partition_for_exchange_single_worker_passthrough() {
        let mut builder = TestRow::builder(2);
        builder.append(TestRow {
            id: 1,
            name: "a".into(),
        });
        builder.append(TestRow {
            id: 2,
            name: "b".into(),
        });
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        let erased = ErasedBuffer::from_typed(buf);

        let key_fn: KeyFn = Arc::new(|_batch: &RecordBatch, _row_idx: usize| "any".to_string());

        let partitions = erased.partition_for_exchange(&key_fn, 1);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].num_rows(), 2);
    }

    #[test]
    fn partition_for_exchange_respects_mask() {
        let mut builder = TestRow::builder(4);
        for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
            builder.append(TestRow {
                id: i as i64,
                name: name.to_string(),
            });
        }
        let buf: RheiBuffer<TestRow> = RheiBuffer::from_builder(builder);
        // Mask out rows 1 ("b") and 3 ("d").
        let masked = buf.with_mask(BooleanArray::from(vec![true, false, true, false]));
        let erased = ErasedBuffer::from_typed(masked);

        let key_fn: KeyFn = Arc::new(|batch: &RecordBatch, row_idx: usize| {
            use arrow_array::cast::AsArray;
            batch
                .column(1)
                .as_string::<i32>()
                .value(row_idx)
                .to_string()
        });

        let partitions = erased.partition_for_exchange(&key_fn, 4);
        let total_rows: usize = partitions.iter().map(ErasedBuffer::num_rows).sum();
        // Only 2 rows should be routed (masked rows excluded).
        assert_eq!(total_rows, 2);
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
mod prop_tests {
    use super::*;
    use crate::executor::partition_key;
    use arrow_array::Int64Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field};
    use proptest::prelude::*;

    fn n_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn make_erased(values: &[i64]) -> ErasedBuffer {
        let schema = n_schema();
        let col = Int64Array::from(values.to_vec());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
        ErasedBuffer::from_parts(batch, None, schema_hash_of(&schema))
    }

    fn value_key_fn() -> KeyFn {
        Arc::new(|batch: &RecordBatch, idx: usize| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .value(idx)
                .to_string()
        })
    }

    fn erased_values(buf: &ErasedBuffer) -> Vec<i64> {
        let batch = buf.as_record_batch();
        let col = batch.column(0).as_primitive::<Int64Type>();
        (0..col.len()).map(|i| col.value(i)).collect()
    }

    proptest! {
        /// `partition_key` is total, in-range, and deterministic.
        #[test]
        fn partition_key_is_bounded_and_deterministic(
            key in ".{0,32}",
            n_workers in 1usize..16,
        ) {
            let p = partition_key(&key, n_workers);
            prop_assert!(p < n_workers);
            prop_assert_eq!(p, partition_key(&key, n_workers));
        }

        /// Exchange partitioning routes every logical row to exactly one worker:
        /// the multiset of values across partitions equals the input.
        #[test]
        fn partition_for_exchange_conserves_rows(
            values in prop::collection::vec(any::<i64>(), 0..64),
            n_workers in 1usize..8,
        ) {
            let buf = make_erased(&values);
            let parts = buf.partition_for_exchange(&value_key_fn(), n_workers);

            let mut seen: Vec<i64> = parts.iter().flat_map(erased_values).collect();
            let mut expected = values.clone();
            seen.sort_unstable();
            expected.sort_unstable();
            prop_assert_eq!(seen, expected);
        }

        /// Each routed row lands on the worker its key hashes to, and every
        /// sub-buffer's `exchange_target` is consistent with its rows.
        #[test]
        fn partition_for_exchange_routes_by_partition_key(
            values in prop::collection::vec(any::<i64>(), 1..64),
            n_workers in 2usize..8,
        ) {
            let buf = make_erased(&values);
            let parts = buf.partition_for_exchange(&value_key_fn(), n_workers);

            for part in &parts {
                let target = part.exchange_target.unwrap() as usize;
                prop_assert!(target < n_workers);
                for v in erased_values(part) {
                    prop_assert_eq!(partition_key(&v.to_string(), n_workers), target);
                }
            }
        }

        /// Arrow-IPC serde round-trip preserves rows and the schema id (the path
        /// every batch takes across a Timely Exchange).
        #[test]
        fn serde_round_trip_preserves_rows_and_schema_id(
            values in prop::collection::vec(any::<i64>(), 0..64),
        ) {
            let original = make_erased(&values);
            let json = serde_json::to_string(&original).unwrap();
            let restored: ErasedBuffer = serde_json::from_str(&json).unwrap();

            prop_assert_eq!(restored.num_rows(), original.num_rows());
            prop_assert_eq!(restored.schema_id, original.schema_id);
            prop_assert_eq!(erased_values(&restored), values);
        }
    }
}
