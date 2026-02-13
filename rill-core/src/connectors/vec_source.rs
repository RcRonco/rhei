use async_trait::async_trait;

use crate::traits::Source;

/// A source that produces elements from an in-memory `Vec<T>`.
///
/// Elements are emitted in batches. A watermark should be emitted every
/// `watermark_interval` records.
pub struct VecSource<T: Send> {
    items: Vec<T>,
    pos: usize,
    batch_size: usize,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
}

impl<T: Send> VecSource<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            items,
            pos: 0,
            batch_size: 1,
            records_since_watermark: 0,
            watermark_interval: 10,
            watermark_pending: false,
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_watermark_interval(mut self, interval: usize) -> Self {
        self.watermark_interval = interval;
        self
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> Source for VecSource<T> {
    type Output = T;

    async fn next_batch(&mut self) -> Option<Vec<T>> {
        if self.pos >= self.items.len() {
            return None;
        }

        let end = (self.pos + self.batch_size).min(self.items.len());
        let batch: Vec<T> = self.items[self.pos..end].to_vec();
        let count = batch.len();
        self.pos = end;
        self.records_since_watermark += count;

        if self.records_since_watermark >= self.watermark_interval {
            self.watermark_pending = true;
            self.records_since_watermark = 0;
        }

        Some(batch)
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emits_all_items() {
        let mut src = VecSource::new(vec![1, 2, 3, 4, 5]).with_batch_size(2);

        let mut all = Vec::new();
        while let Some(batch) = src.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn watermark_emitted_at_interval() {
        let mut src = VecSource::new(vec![0; 25])
            .with_batch_size(5)
            .with_watermark_interval(10);

        // Batch 1: 5 records, no watermark
        src.next_batch().await;
        assert!(!src.should_emit_watermark());

        // Batch 2: 10 records total, watermark
        src.next_batch().await;
        assert!(src.should_emit_watermark());
    }

    #[tokio::test]
    async fn empty_source() {
        let mut src = VecSource::<i32>::new(vec![]);
        assert!(src.next_batch().await.is_none());
    }
}
