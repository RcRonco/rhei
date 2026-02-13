use async_trait::async_trait;

use crate::state::context::StateContext;

/// Core trait for stateful stream processing operators.
///
/// Implementations receive an input element and a mutable reference to
/// `StateContext` for reading/writing operator state. Returns zero or more
/// output elements.
#[async_trait]
pub trait StreamFunction: Send + Sync {
    type Input: Send;
    type Output: Send;

    async fn process(&mut self, input: Self::Input, ctx: &mut StateContext) -> Vec<Self::Output>;
}

/// A source that produces elements into a stream.
#[async_trait]
pub trait Source: Send + Sync {
    type Output: Send;

    /// Pull the next batch of elements. Returns `None` when exhausted.
    async fn next_batch(&mut self) -> Option<Vec<Self::Output>>;

    /// Returns true when a watermark should be emitted (e.g. every N records).
    fn should_emit_watermark(&self) -> bool {
        false
    }
}

/// A sink that consumes elements from a stream.
#[async_trait]
pub trait Sink: Send + Sync {
    type Input: Send;

    async fn write(&mut self, input: Self::Input) -> anyhow::Result<()>;

    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
