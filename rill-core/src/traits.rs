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
