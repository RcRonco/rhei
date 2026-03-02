use async_trait::async_trait;

use crate::state::context::StateContext;

/// Core trait for stateful stream processing operators.
///
/// Implementations receive an input element and a mutable reference to
/// `StateContext` for reading/writing operator state. Returns zero or more
/// output elements wrapped in a `Result`.
#[async_trait]
pub trait StreamFunction: Send + Sync {
    /// The element type consumed by this operator.
    type Input: Clone + Send + std::fmt::Debug;
    /// The element type produced by this operator.
    type Output: Clone + Send + std::fmt::Debug;

    /// Processes a single input element and returns zero or more output elements.
    async fn process(
        &mut self,
        input: Self::Input,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<Self::Output>>;

    /// Process a batch of inputs. Default: process each element individually.
    /// Operators that benefit from batch processing (e.g. batch state lookups)
    /// can override this.
    async fn process_batch(
        &mut self,
        inputs: Vec<Self::Input>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<Self::Output>> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.process(input, ctx).await?);
        }
        Ok(outputs)
    }

    /// Called when the global watermark advances. Window operators use this
    /// to close eligible windows. Default: no output.
    async fn on_watermark(
        &mut self,
        _watermark: u64,
        _ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<Self::Output>> {
        Ok(vec![])
    }
}

/// A source that produces elements into a stream.
#[async_trait]
pub trait Source: Send + Sync {
    /// The element type produced by this source.
    type Output: Send;

    /// Pull the next batch of elements. Returns `None` when exhausted.
    async fn next_batch(&mut self) -> Option<Vec<Self::Output>>;

    /// Returns true when a watermark should be emitted (e.g. every N records).
    fn should_emit_watermark(&self) -> bool {
        false
    }

    /// Returns the current event-time watermark (millis). Downstream window
    /// operators use this to close windows. Default: None.
    fn current_watermark(&self) -> Option<u64> {
        None
    }

    /// Called after a successful checkpoint. Sources that track offsets
    /// (e.g. Kafka) use this to commit offsets to the external system.
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns a snapshot of current source offsets for checkpoint persistence.
    ///
    /// Key format is source-specific (e.g. `"topic/partition"` for Kafka).
    /// The default implementation returns an empty map.
    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    /// Restore source offsets from a checkpoint manifest.
    ///
    /// Called before any `next_batch()` on restart so the source can seek
    /// to the correct position. The default implementation is a no-op.
    async fn restore_offsets(
        &mut self,
        _offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns the number of partitions if this source supports parallel consumption.
    ///
    /// When `Some(n)`, the executor calls `create_partition_source()` to create
    /// per-worker readers. Default: `None` (single-worker only).
    fn partition_count(&self) -> Option<usize> {
        None
    }

    /// Create a Source that reads only the given partition indices.
    ///
    /// Called by the executor when `partition_count()` returns `Some`.
    /// Takes `&self` because multiple partition readers are created from one factory.
    fn create_partition_source(&self, _assigned: &[usize]) -> Box<dyn Source<Output = Self::Output>>
    where
        Self: Sized,
    {
        unimplemented!("sources returning Some from partition_count() must implement this")
    }
}

/// A sink that consumes elements from a stream.
#[async_trait]
pub trait Sink: Send + Sync {
    /// The element type consumed by this sink.
    type Input: Send;

    /// Writes a single element to the sink.
    async fn write(&mut self, input: Self::Input) -> anyhow::Result<()>;

    /// Flushes any buffered data. Default implementation is a no-op.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal source that uses all trait defaults.
    struct MinimalSource;

    #[async_trait]
    impl Source for MinimalSource {
        type Output = i32;
        async fn next_batch(&mut self) -> Option<Vec<i32>> {
            None
        }
    }

    #[tokio::test]
    async fn source_defaults_partition_count_is_none() {
        let src = MinimalSource;
        assert_eq!(src.partition_count(), None);
    }

    #[tokio::test]
    async fn source_defaults_current_offsets_is_empty() {
        let src = MinimalSource;
        assert!(src.current_offsets().is_empty());
    }

    #[tokio::test]
    async fn source_defaults_restore_offsets_is_noop() {
        let mut src = MinimalSource;
        let offsets = std::collections::HashMap::from([("key".to_string(), "42".to_string())]);
        src.restore_offsets(&offsets).await.unwrap();
    }

    #[tokio::test]
    async fn source_defaults_on_checkpoint_complete_is_noop() {
        let mut src = MinimalSource;
        src.on_checkpoint_complete().await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "sources returning Some from partition_count()")]
    async fn source_defaults_create_partition_source_panics() {
        let src = MinimalSource;
        let _ = src.create_partition_source(&[0]);
    }

    #[tokio::test]
    async fn source_defaults_should_emit_watermark_is_false() {
        let src = MinimalSource;
        assert!(!src.should_emit_watermark());
    }
}
