use async_trait::async_trait;

use super::buffer::{BufferOutput, RheiBuffer};
use super::context::OperatorContext;
use super::schema::RheiSchema;

/// Batch-oriented stream processing operator.
///
/// Unlike the row-based `StreamFunction`, this trait operates on Arrow
/// `RheiBuffer` batches. Operators receive a full buffer of rows and produce
/// zero or more output buffers.
///
/// Three processing modes are supported depending on operator needs:
/// - **Row (View):** iterate `input.iter()` for per-row logic with state access
/// - **Batch:** access `input.as_record_batch()` for DataFusion/kernel operations
/// - **Column:** access `input.columns()` for vectorized column-level processing
#[async_trait]
pub trait StreamFunction: Send + Sync {
    /// The schema type for input buffers.
    type Input: RheiSchema;
    /// The schema type for output buffers.
    type Output: RheiSchema;

    /// Process an input buffer and produce output.
    async fn process(
        &mut self,
        input: RheiBuffer<Self::Input>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Self::Output>>;

    /// Called when the global watermark advances.
    ///
    /// Window operators use this to close eligible windows and emit results.
    async fn on_watermark(
        &mut self,
        _watermark: u64,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Self::Output>> {
        Ok(BufferOutput::None)
    }

    /// Called once when the operator is first initialized.
    async fn open(&mut self, _ctx: &mut OperatorContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called once when the operator is shutting down.
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called when a registered timer fires (watermark >= timer timestamp).
    async fn on_timer(
        &mut self,
        _timestamp: u64,
        _key: &str,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Self::Output>> {
        Ok(BufferOutput::None)
    }

    /// Called when `process` returns an error.
    ///
    /// Return `Err(e)` to propagate, `Ok(Single(...))` for recovery output,
    /// or `Ok(None)` to skip silently.
    async fn on_error(
        &mut self,
        error: anyhow::Error,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Self::Output>> {
        Err(error)
    }
}

/// Batch-oriented source that produces Arrow buffers.
#[async_trait]
pub trait Source: Send + Sync {
    /// The schema type for output buffers.
    type Output: RheiSchema;

    /// Pull the next batch of data. Returns `None` when exhausted.
    async fn next_batch(&mut self) -> Option<RheiBuffer<Self::Output>>;

    /// Returns true when a watermark should be emitted.
    fn should_emit_watermark(&self) -> bool {
        false
    }

    /// Returns the current event-time watermark (millis).
    fn current_watermark(&self) -> Option<u64> {
        None
    }

    /// Called after a successful checkpoint.
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns current source offsets for checkpoint persistence.
    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    /// Restore source offsets from a checkpoint manifest.
    async fn restore_offsets(
        &mut self,
        _offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Number of partitions for parallel consumption.
    fn partition_count(&self) -> Option<usize> {
        None
    }

    /// Create a source for the given partition indices.
    fn create_partition_source(
        &self,
        _assigned: &[usize],
    ) -> Option<Box<dyn Source<Output = Self::Output>>>
    where
        Self: Sized,
    {
        unimplemented!("sources returning Some from partition_count() must implement this")
    }
}

/// Batch-oriented sink that consumes Arrow buffers.
#[async_trait]
pub trait Sink: Send + Sync {
    /// The schema type for input buffers.
    type Input: RheiSchema;

    /// Write a buffer of rows to the sink.
    async fn write_batch(&mut self, input: RheiBuffer<Self::Input>) -> anyhow::Result<()>;

    /// Flush any buffered data.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
