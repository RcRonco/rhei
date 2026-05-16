use std::hash::Hash;
use std::marker::PhantomData;

use arrow_array::{BooleanArray, RecordBatch};

use super::iter::RheiIter;
use super::schema::RheiSchema;

/// Core buffer abstraction wrapping an Arrow `RecordBatch` with an optional
/// selection vector (mask).
///
/// The mask enables zero-copy filtering: rather than copying data to remove
/// filtered rows, a `BooleanArray` marks which rows are logically valid.
/// Downstream operators skip masked rows during iteration.
pub struct RheiBuffer<T: RheiSchema> {
    batch: RecordBatch,
    mask: Option<BooleanArray>,
    _marker: PhantomData<T>,
}

impl<T: RheiSchema> RheiBuffer<T> {
    /// Create a buffer from a completed builder.
    pub fn from_builder(builder: T::Builder) -> Self {
        use super::RheiBuilder;
        Self {
            batch: builder.finish(),
            mask: None,
            _marker: PhantomData,
        }
    }

    /// Create a buffer from a raw `RecordBatch` (no mask).
    pub fn from_record_batch(batch: RecordBatch) -> Self {
        Self {
            batch,
            mask: None,
            _marker: PhantomData,
        }
    }

    /// Number of logically valid rows (respecting the mask).
    pub fn len(&self) -> usize {
        match &self.mask {
            None => self.batch.num_rows(),
            Some(mask) => mask.true_count(),
        }
    }

    /// Number of physical rows in the underlying `RecordBatch` (ignores mask).
    pub fn physical_len(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns true if the buffer has no logically valid rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Replace the current mask with a new one.
    pub fn with_mask(mut self, mask: BooleanArray) -> Self {
        self.mask = Some(mask);
        self
    }

    /// AND a new mask with the existing mask (composable filters).
    /// If no mask exists, the new mask becomes the mask.
    #[allow(clippy::expect_used)]
    pub fn and_mask(self, new_mask: BooleanArray) -> Self {
        match self.mask {
            None => Self {
                batch: self.batch,
                mask: Some(new_mask),
                _marker: PhantomData,
            },
            Some(existing) => {
                let combined = arrow::compute::kernels::boolean::and(&existing, &new_mask)
                    .expect("mask lengths must match");
                Self {
                    batch: self.batch,
                    mask: Some(combined),
                    _marker: PhantomData,
                }
            }
        }
    }

    /// Returns a reference to the current mask, if any.
    pub fn mask(&self) -> Option<&BooleanArray> {
        self.mask.as_ref()
    }

    /// Materialize: physically remove masked rows, producing a compact buffer.
    ///
    /// Used before network transfer or when the mask selectivity is low
    /// (many rows filtered out).
    #[allow(clippy::expect_used)]
    pub fn compact(&self) -> Self {
        match &self.mask {
            None => Self {
                batch: self.batch.clone(),
                mask: None,
                _marker: PhantomData,
            },
            Some(mask) => {
                let filtered = arrow::compute::filter_record_batch(&self.batch, mask)
                    .expect("mask length must match batch row count");
                Self {
                    batch: filtered,
                    mask: None,
                    _marker: PhantomData,
                }
            }
        }
    }

    /// Returns true if the buffer should be compacted (high mask ratio).
    /// Threshold: fewer than half of physical rows are valid.
    pub fn should_compact(&self) -> bool {
        match &self.mask {
            None => false,
            Some(mask) => mask.true_count() < self.physical_len() / 2,
        }
    }

    /// Iterate over logically valid rows as zero-copy views.
    /// Masked rows are skipped.
    pub fn iter(&self) -> RheiIter<'_, T> {
        RheiIter::new(&self.batch, self.mask.as_ref())
    }

    /// Access the underlying [`RecordBatch`] directly (for `DataFusion` or custom kernels).
    pub fn as_record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Access typed column references for vectorized processing.
    pub fn columns(&self) -> T::Columns<'_> {
        T::columns(&self.batch)
    }

    /// Partition the buffer by key into `num_partitions` sub-buffers.
    ///
    /// Each row is assigned to a partition based on the hash of the key
    /// extracted by `key_fn`. Empty partitions produce empty buffers.
    /// The buffer is compacted before partitioning if heavily masked.
    #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
    pub fn partition_by_key<K: Hash>(
        &self,
        key_fn: impl Fn(&T::View<'_>) -> K,
        num_partitions: usize,
    ) -> Vec<RheiBuffer<T>> {
        use std::hash::Hasher;

        let source = if self.should_compact() {
            self.compact()
        } else {
            Self {
                batch: self.batch.clone(),
                mask: self.mask.clone(),
                _marker: PhantomData,
            }
        };

        let mut partition_masks: Vec<Vec<bool>> = (0..num_partitions)
            .map(|_| vec![false; source.physical_len()])
            .collect();

        for (row_idx, view) in source.iter().enumerate_physical() {
            let key = key_fn(&view);
            let mut hasher = std::hash::DefaultHasher::new();
            key.hash(&mut hasher);
            let partition = (hasher.finish() as usize) % num_partitions;
            partition_masks[partition][row_idx] = true;
        }

        partition_masks
            .into_iter()
            .map(|mask_vec| {
                let mask = BooleanArray::from(mask_vec);
                let filtered = arrow::compute::filter_record_batch(source.as_record_batch(), &mask)
                    .expect("partition mask length must match");
                RheiBuffer::from_record_batch(filtered)
            })
            .collect()
    }
}

impl<'a, T: RheiSchema> IntoIterator for &'a RheiBuffer<T> {
    type Item = T::View<'a>;
    type IntoIter = RheiIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: RheiSchema> Clone for RheiBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            mask: self.mask.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: RheiSchema> std::fmt::Debug for RheiBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RheiBuffer")
            .field("physical_rows", &self.physical_len())
            .field("logical_rows", &self.len())
            .field("has_mask", &self.mask.is_some())
            .finish_non_exhaustive()
    }
}

/// Output from a batch operator: zero, one, or multiple buffers.
///
/// Avoids `Vec` allocation in the common single-buffer case while still
/// supporting window operators that emit multiple buffers per invocation.
#[derive(Debug, Clone)]
pub enum BufferOutput<T: RheiSchema> {
    /// No output (e.g. filter removed all rows, or operator is buffering).
    None,
    /// Single output buffer (the common case for map, filter, flat-map).
    Single(RheiBuffer<T>),
    /// Multiple output buffers (e.g. window operator closing several windows).
    Multi(Vec<RheiBuffer<T>>),
}

impl<T: RheiSchema> BufferOutput<T> {
    /// Convert into a `Vec<RheiBuffer<T>>` for compatibility.
    pub fn into_vec(self) -> Vec<RheiBuffer<T>> {
        match self {
            Self::None => vec![],
            Self::Single(buf) => vec![buf],
            Self::Multi(bufs) => bufs,
        }
    }

    /// Returns true if the output contains no data.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::Single(_) => false,
            Self::Multi(bufs) => bufs.is_empty(),
        }
    }
}

impl<T: RheiSchema> From<RheiBuffer<T>> for BufferOutput<T> {
    fn from(buf: RheiBuffer<T>) -> Self {
        Self::Single(buf)
    }
}

impl<T: RheiSchema> From<Vec<RheiBuffer<T>>> for BufferOutput<T> {
    fn from(mut bufs: Vec<RheiBuffer<T>>) -> Self {
        match bufs.len() {
            0 => Self::None,
            1 => Self::Single(bufs.swap_remove(0)),
            _ => Self::Multi(bufs),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::ArrayBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::arrow::{RheiBuilder, RheiSchema};

    // Manual implementation of RheiSchema for a simple 2-field struct
    struct TestEvent {
        id: i64,
        name: String,
    }

    struct TestEventBuilder {
        id: arrow_array::builder::Int64Builder,
        name: arrow_array::builder::StringBuilder,
    }

    #[derive(Debug, Clone)]
    struct TestEventView<'a> {
        id: i64,
        name: &'a str,
    }

    struct TestEventColumns<'a> {
        #[allow(dead_code)]
        id: &'a Int64Array,
        #[allow(dead_code)]
        name: &'a StringArray,
    }

    impl RheiBuilder for TestEventBuilder {
        type Item = TestEvent;

        fn append(&mut self, item: TestEvent) {
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
                TestEvent::arrow_schema(),
                vec![Arc::new(self.id.finish()), Arc::new(self.name.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchema for TestEvent {
        type Builder = TestEventBuilder;
        type View<'a> = TestEventView<'a>;
        type Columns<'a> = TestEventColumns<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            TestEventBuilder {
                id: arrow_array::builder::Int64Builder::with_capacity(capacity),
                name: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            TestEventView {
                id: batch.column(0).as_primitive::<Int64Type>().value(index),
                name: batch.column(1).as_string::<i32>().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            TestEventColumns {
                id: batch.column(0).as_primitive::<Int64Type>(),
                name: batch.column(1).as_string::<i32>(),
            }
        }
    }

    #[test]
    fn buffer_from_builder() {
        let mut builder = TestEvent::builder(4);
        builder.append(TestEvent {
            id: 1,
            name: "alice".into(),
        });
        builder.append(TestEvent {
            id: 2,
            name: "bob".into(),
        });
        builder.append(TestEvent {
            id: 3,
            name: "carol".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.physical_len(), 3);
        assert!(!buf.is_empty());
        assert!(buf.mask().is_none());
    }

    #[test]
    fn buffer_iter_no_mask() {
        let mut builder = TestEvent::builder(3);
        builder.append(TestEvent {
            id: 10,
            name: "x".into(),
        });
        builder.append(TestEvent {
            id: 20,
            name: "y".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        let views: Vec<_> = buf.iter().collect();
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].id, 10);
        assert_eq!(views[0].name, "x");
        assert_eq!(views[1].id, 20);
        assert_eq!(views[1].name, "y");
    }

    #[test]
    fn buffer_mask_skips_rows() {
        let mut builder = TestEvent::builder(4);
        builder.append(TestEvent {
            id: 1,
            name: "a".into(),
        });
        builder.append(TestEvent {
            id: 2,
            name: "b".into(),
        });
        builder.append(TestEvent {
            id: 3,
            name: "c".into(),
        });
        builder.append(TestEvent {
            id: 4,
            name: "d".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        // Mask: keep rows 0, 2 (ids 1, 3)
        let mask = BooleanArray::from(vec![true, false, true, false]);
        let masked = buf.with_mask(mask);

        assert_eq!(masked.len(), 2);
        assert_eq!(masked.physical_len(), 4);

        let views: Vec<_> = masked.iter().collect();
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].id, 1);
        assert_eq!(views[1].id, 3);
    }

    #[test]
    fn buffer_and_mask_composes() {
        let mut builder = TestEvent::builder(4);
        for i in 0..4 {
            builder.append(TestEvent {
                id: i,
                name: format!("n{i}"),
            });
        }

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        let mask1 = BooleanArray::from(vec![true, true, false, false]);
        let mask2 = BooleanArray::from(vec![true, false, true, false]);

        // AND of mask1 and mask2 = [true, false, false, false]
        let masked = buf.with_mask(mask1).and_mask(mask2);
        assert_eq!(masked.len(), 1);

        let views: Vec<_> = masked.iter().collect();
        assert_eq!(views[0].id, 0);
    }

    #[test]
    fn buffer_compact_removes_masked() {
        let mut builder = TestEvent::builder(4);
        builder.append(TestEvent {
            id: 1,
            name: "keep".into(),
        });
        builder.append(TestEvent {
            id: 2,
            name: "drop".into(),
        });
        builder.append(TestEvent {
            id: 3,
            name: "keep".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        let mask = BooleanArray::from(vec![true, false, true]);
        let masked = buf.with_mask(mask);

        let compacted = masked.compact();
        assert_eq!(compacted.physical_len(), 2);
        assert_eq!(compacted.len(), 2);
        assert!(compacted.mask().is_none());

        let views: Vec<_> = compacted.iter().collect();
        assert_eq!(views[0].id, 1);
        assert_eq!(views[0].name, "keep");
        assert_eq!(views[1].id, 3);
    }

    #[test]
    fn buffer_partition_by_key() {
        let mut builder = TestEvent::builder(4);
        builder.append(TestEvent {
            id: 0,
            name: "a".into(),
        });
        builder.append(TestEvent {
            id: 1,
            name: "b".into(),
        });
        builder.append(TestEvent {
            id: 2,
            name: "c".into(),
        });
        builder.append(TestEvent {
            id: 3,
            name: "d".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        let partitions = buf.partition_by_key(|v| v.id, 2);

        assert_eq!(partitions.len(), 2);
        let total_rows: usize = partitions.iter().map(RheiBuffer::len).sum();
        assert_eq!(total_rows, 4);

        // Every row should appear in exactly one partition
        let mut all_ids: Vec<i64> = partitions
            .iter()
            .flat_map(|p| p.iter().map(|v| v.id))
            .collect();
        all_ids.sort_unstable();
        assert_eq!(all_ids, vec![0, 1, 2, 3]);
    }

    #[test]
    fn buffer_should_compact_heuristic() {
        let mut builder = TestEvent::builder(4);
        for i in 0..10 {
            builder.append(TestEvent {
                id: i,
                name: format!("n{i}"),
            });
        }
        let buf = RheiBuffer::<TestEvent>::from_builder(builder);

        // No mask → don't compact
        assert!(!buf.should_compact());

        // Mask keeping 8/10 → don't compact (80% valid)
        let mask_high = BooleanArray::from(vec![
            true, true, true, true, true, true, true, true, false, false,
        ]);
        let masked_high = buf.clone().with_mask(mask_high);
        assert!(!masked_high.should_compact());

        // Mask keeping 2/10 → should compact (20% valid)
        let mask_low = BooleanArray::from(vec![
            true, false, false, false, false, false, false, false, false, true,
        ]);
        let masked_low = buf.with_mask(mask_low);
        assert!(masked_low.should_compact());
    }

    #[test]
    fn buffer_columns_access() {
        let mut builder = TestEvent::builder(2);
        builder.append(TestEvent {
            id: 42,
            name: "test".into(),
        });
        builder.append(TestEvent {
            id: 99,
            name: "foo".into(),
        });

        let buf = RheiBuffer::<TestEvent>::from_builder(builder);
        let cols = buf.columns();
        assert_eq!(cols.id.value(0), 42);
        assert_eq!(cols.id.value(1), 99);
        assert_eq!(cols.name.value(0), "test");
        assert_eq!(cols.name.value(1), "foo");
    }
}
