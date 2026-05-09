//! A batch source that distributes items across partitions for parallel consumption.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::arrow::{BatchSource, RheiBuffer, RheiSchema};

use super::vec_source::BatchVecSource;

/// A partitioned batch source that distributes items round-robin across partitions.
///
/// Items are assigned to partitions by index: item `i` belongs to partition
/// `i % num_partitions`. The executor calls `create_partition_source()` to
/// create per-worker readers that only emit their assigned subset.
#[derive(Debug)]
pub struct BatchPartitionedVecSource<T: RheiSchema> {
    items: Vec<T>,
    num_partitions: usize,
    batch_size: usize,
    _marker: PhantomData<T>,
}

impl<T: RheiSchema> BatchPartitionedVecSource<T> {
    /// Creates a new partitioned source distributing items across `num_partitions`.
    pub fn new(items: Vec<T>, num_partitions: usize) -> Self {
        assert!(num_partitions >= 1, "partition count must be at least 1");
        Self {
            items,
            num_partitions,
            batch_size: 1024,
            _marker: PhantomData,
        }
    }

    /// Sets the batch size for partition sources.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

#[async_trait]
impl<T: RheiSchema + Clone + Sync> BatchSource for BatchPartitionedVecSource<T> {
    type Output = T;

    async fn next_batch(&mut self) -> Option<RheiBuffer<T>> {
        None
    }

    fn partition_count(&self) -> Option<usize> {
        Some(self.num_partitions)
    }

    fn create_partition_source(
        &self,
        assigned: &[usize],
    ) -> Option<Box<dyn BatchSource<Output = T>>> {
        let mut items = Vec::new();
        for (i, item) in self.items.iter().enumerate() {
            if assigned.contains(&(i % self.num_partitions)) {
                items.push(item.clone());
            }
        }
        Some(Box::new(
            BatchVecSource::new(items).with_batch_size(self.batch_size),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow_array::builder::ArrayBuilder;

    struct SimpleItem {
        val: i64,
    }

    struct SimpleBuilder {
        col: arrow_array::builder::Int64Builder,
    }

    #[allow(dead_code)]
    struct SimpleView {
        val: i64,
    }

    struct SimpleCols<'a> {
        #[allow(dead_code)]
        val: &'a arrow_array::Int64Array,
    }

    impl crate::arrow::RheiBuilder for SimpleBuilder {
        type Item = SimpleItem;

        fn append(&mut self, item: SimpleItem) {
            self.col.append_value(item.val);
        }

        fn append_null(&mut self) {
            self.col.append_null();
        }

        fn len(&self) -> usize {
            self.col.len()
        }

        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                SimpleItem::arrow_schema(),
                vec![Arc::new(self.col.finish())],
            )
            .unwrap()
        }
    }

    impl Clone for SimpleItem {
        fn clone(&self) -> Self {
            Self { val: self.val }
        }
    }

    impl RheiSchema for SimpleItem {
        type Builder = SimpleBuilder;
        type View<'a> = SimpleView;
        type Columns<'a> = SimpleCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "val",
                arrow_schema::DataType::Int64,
                false,
            )]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            SimpleBuilder {
                col: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }

        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            SimpleView {
                val: batch.column(0).as_primitive::<Int64Type>().value(index),
            }
        }

        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            SimpleCols {
                val: batch.column(0).as_primitive::<Int64Type>(),
            }
        }
    }

    #[tokio::test]
    async fn factory_returns_none() {
        let mut src =
            BatchPartitionedVecSource::new(vec![SimpleItem { val: 1 }, SimpleItem { val: 2 }], 2);
        assert!(src.next_batch().await.is_none());
    }

    #[tokio::test]
    async fn partition_count_correct() {
        let src =
            BatchPartitionedVecSource::new((0..6).map(|i| SimpleItem { val: i }).collect(), 3);
        assert_eq!(src.partition_count(), Some(3));
    }

    #[tokio::test]
    async fn partition_source_gets_correct_items() {
        let src =
            BatchPartitionedVecSource::new((0..6).map(|i| SimpleItem { val: i }).collect(), 3)
                .with_batch_size(10);

        let mut p0 = src.create_partition_source(&[0]).unwrap();
        let mut all = Vec::new();
        while let Some(buf) = p0.next_batch().await {
            for view in &buf {
                all.push(view.val);
            }
        }
        assert_eq!(all, vec![0, 3]);

        let mut p1 = src.create_partition_source(&[1]).unwrap();
        let mut all = Vec::new();
        while let Some(buf) = p1.next_batch().await {
            for view in &buf {
                all.push(view.val);
            }
        }
        assert_eq!(all, vec![1, 4]);
    }

    #[tokio::test]
    async fn multiple_partitions_assigned() {
        let src =
            BatchPartitionedVecSource::new((0..6).map(|i| SimpleItem { val: i }).collect(), 3)
                .with_batch_size(10);

        let mut p = src.create_partition_source(&[0, 2]).unwrap();
        let mut all = Vec::new();
        while let Some(buf) = p.next_batch().await {
            for view in &buf {
                all.push(view.val);
            }
        }
        assert_eq!(all, vec![0, 2, 3, 5]);
    }
}
