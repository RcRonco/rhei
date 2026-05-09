//! A batch source that produces `RheiBuffer` from an in-memory `Vec<T>`.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::arrow::{BatchSource, RheiBuffer, RheiBuilder, RheiSchema};

/// A source that builds Arrow buffers from an in-memory `Vec<T>`.
///
/// Items are consumed in batches of `batch_size`. Each `next_batch()` call
/// returns a single `RheiBuffer` containing up to `batch_size` rows.
#[derive(Debug)]
pub struct BatchVecSource<T: RheiSchema> {
    items: Vec<T>,
    batch_size: usize,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
    _marker: PhantomData<T>,
}

impl<T: RheiSchema> BatchVecSource<T> {
    /// Creates a new `BatchVecSource` from the given items.
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items,
            batch_size: 1024,
            records_since_watermark: 0,
            watermark_interval: 10,
            watermark_pending: false,
            _marker: PhantomData,
        }
    }

    /// Sets the number of rows emitted per buffer (default: 1024).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets the number of records between watermark emissions (default: 10).
    pub fn with_watermark_interval(mut self, interval: usize) -> Self {
        self.watermark_interval = interval;
        self
    }
}

#[async_trait]
impl<T: RheiSchema + Sync> BatchSource for BatchVecSource<T> {
    type Output = T;

    async fn next_batch(&mut self) -> Option<RheiBuffer<T>> {
        if self.items.is_empty() {
            return None;
        }

        let count = self.batch_size.min(self.items.len());
        let mut builder = T::builder(count);
        for item in self.items.drain(..count) {
            builder.append(item);
        }

        self.records_since_watermark += count;
        if self.records_since_watermark >= self.watermark_interval {
            self.watermark_pending = true;
            self.records_since_watermark = 0;
        }

        Some(RheiBuffer::from_builder(builder))
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
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
    async fn emits_all_items_in_batches() {
        let items: Vec<_> = (0..10).map(|i| SimpleItem { val: i }).collect();
        let mut src = BatchVecSource::new(items).with_batch_size(3);

        let mut total_rows = 0;
        while let Some(buf) = src.next_batch().await {
            total_rows += buf.len();
        }
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn empty_source_returns_none() {
        let mut src = BatchVecSource::<SimpleItem>::new(vec![]);
        assert!(src.next_batch().await.is_none());
    }
}
