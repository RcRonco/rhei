use async_trait::async_trait;

use super::vec_source::VecSource;
use crate::traits::Source;

/// A source that distributes items across partitions for testing parallel consumption.
///
/// Items are assigned to partitions round-robin by index: item `i` belongs to
/// partition `i % num_partitions`. The executor calls `create_partition_source()`
/// to create per-worker readers that only emit their assigned subset.
#[derive(Debug)]
pub struct PartitionedVecSource<T: Send> {
    items: Vec<T>,
    num_partitions: usize,
}

impl<T: Send> PartitionedVecSource<T> {
    /// Creates a new `PartitionedVecSource` with the given items and partition count.
    pub fn new(items: Vec<T>, num_partitions: usize) -> Self {
        assert!(num_partitions >= 1, "partition count must be at least 1");
        Self {
            items,
            num_partitions,
        }
    }
}

#[async_trait]
impl<T> Source for PartitionedVecSource<T>
where
    T: Clone
        + Send
        + Sync
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
{
    type Output = T;

    async fn next_batch(&mut self) -> Option<Vec<T>> {
        // Factory only — never called directly.
        None
    }

    fn partition_count(&self) -> Option<usize> {
        Some(self.num_partitions)
    }

    fn create_partition_source(&self, assigned: &[usize]) -> Box<dyn Source<Output = T>> {
        let mut items = Vec::new();
        for (i, item) in self.items.iter().enumerate() {
            if assigned.contains(&(i % self.num_partitions)) {
                items.push(item.clone());
            }
        }
        Box::new(VecSource::new(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn factory_returns_none() {
        let mut src = PartitionedVecSource::new(vec![1, 2, 3], 2);
        assert!(src.next_batch().await.is_none());
    }

    #[tokio::test]
    async fn partition_count_correct() {
        let src = PartitionedVecSource::new(vec![1, 2, 3, 4], 3);
        assert_eq!(src.partition_count(), Some(3));
    }

    #[tokio::test]
    async fn partition_source_gets_correct_items() {
        let src = PartitionedVecSource::new(vec![0, 1, 2, 3, 4, 5], 3);

        // Partition 0 gets items at indices 0, 3
        let mut p0 = src.create_partition_source(&[0]);
        let mut all = Vec::new();
        while let Some(batch) = p0.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![0, 3]);

        // Partition 1 gets items at indices 1, 4
        let mut p1 = src.create_partition_source(&[1]);
        let mut all = Vec::new();
        while let Some(batch) = p1.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![1, 4]);

        // Partition 2 gets items at indices 2, 5
        let mut p2 = src.create_partition_source(&[2]);
        let mut all = Vec::new();
        while let Some(batch) = p2.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![2, 5]);
    }

    #[tokio::test]
    async fn multiple_partitions_assigned() {
        let src = PartitionedVecSource::new(vec![0, 1, 2, 3, 4, 5], 3);

        // Worker gets partitions 0 and 2
        let mut p = src.create_partition_source(&[0, 2]);
        let mut all = Vec::new();
        while let Some(batch) = p.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![0, 2, 3, 5]);
    }
}
