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

/// Compute a stable hash for a `RheiSchema` type based on its Arrow schema.
fn schema_hash<T: RheiSchema>() -> u64 {
    let schema = T::arrow_schema();
    let schema_str = format!("{schema:?}");
    seahash::hash(schema_str.as_bytes())
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
}
