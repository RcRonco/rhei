//! Type-erased batch operator/source/sink wrappers.
//!
//! These bridge typed `StreamFunction`/`Source`/`Sink`
//! implementations to the [`ErasedBuffer`] streams used in the Timely executor.

#![allow(dead_code)]

use async_trait::async_trait;
use rhei_core::arrow::{BufferOutput, OperatorContext, RheiSchema, Sink, Source, StreamFunction};

use crate::erased_buffer::ErasedBuffer;

// ── Type-erased batch source ──────────────────────────────────────────

/// Type-erased source that produces [`ErasedBuffer`] batches.
#[async_trait]
pub(crate) trait ErasedSource: Send {
    /// Pull the next batch. Returns `None` when exhausted.
    async fn next_batch(&mut self) -> Option<ErasedBuffer>;
    /// Called after a successful checkpoint.
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()>;
    /// Returns current source offsets for checkpoint persistence.
    fn current_offsets(&self) -> std::collections::HashMap<String, String>;
    /// Restore source offsets from a checkpoint manifest.
    async fn restore_offsets(
        &mut self,
        offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()>;
    /// Number of partitions for parallel consumption.
    fn partition_count(&self) -> Option<usize>;
    /// Create a source for the given partition indices.
    fn create_partition_source(&self, assigned: &[usize]) -> Option<Box<dyn ErasedSource>>;
    /// Returns the current event-time watermark (millis).
    fn current_watermark(&self) -> Option<u64>;
}

/// Wraps a typed [`Source`] into an [`ErasedSource`].
pub(crate) struct SourceWrapper<S: Source>(pub(crate) S);

#[async_trait]
impl<S> ErasedSource for SourceWrapper<S>
where
    S: Source + 'static,
    S::Output: Sync,
{
    async fn next_batch(&mut self) -> Option<ErasedBuffer> {
        let buf = self.0.next_batch().await?;
        Some(ErasedBuffer::from_typed(buf))
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        self.0.on_checkpoint_complete().await
    }

    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        self.0.current_offsets()
    }

    async fn restore_offsets(
        &mut self,
        offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.restore_offsets(offsets).await
    }

    fn partition_count(&self) -> Option<usize> {
        self.0.partition_count()
    }

    fn create_partition_source(&self, assigned: &[usize]) -> Option<Box<dyn ErasedSource>> {
        self.0.partition_count()?;
        let partition_source = self.0.create_partition_source(assigned)?;
        Some(Box::new(DynSourceWrapper(partition_source)))
    }

    fn current_watermark(&self) -> Option<u64> {
        self.0.current_watermark()
    }
}

/// Wraps a `Box<dyn Source>` returned by `create_partition_source`.
struct DynSourceWrapper<T: RheiSchema>(Box<dyn Source<Output = T>>);

#[async_trait]
impl<T: RheiSchema + Sync> ErasedSource for DynSourceWrapper<T> {
    async fn next_batch(&mut self) -> Option<ErasedBuffer> {
        let buf = self.0.next_batch().await?;
        Some(ErasedBuffer::from_typed(buf))
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        self.0.on_checkpoint_complete().await
    }

    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        self.0.current_offsets()
    }

    async fn restore_offsets(
        &mut self,
        offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.restore_offsets(offsets).await
    }

    fn partition_count(&self) -> Option<usize> {
        None
    }

    fn create_partition_source(&self, _assigned: &[usize]) -> Option<Box<dyn ErasedSource>> {
        None
    }

    fn current_watermark(&self) -> Option<u64> {
        self.0.current_watermark()
    }
}

// ── Type-erased batch sink ────────────────────────────────────────────

/// Type-erased sink that consumes [`ErasedBuffer`] batches.
#[async_trait]
pub(crate) trait ErasedSink: Send {
    /// Write an erased buffer.
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()>;
    /// Flush any buffered data.
    async fn flush(&mut self) -> anyhow::Result<()>;
}

/// Wraps a typed [`Sink`] into an [`ErasedSink`].
pub(crate) struct SinkWrapper<K: Sink>(pub(crate) K);

#[async_trait]
impl<K> ErasedSink for SinkWrapper<K>
where
    K: Sink + 'static,
{
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        let typed = buf.downcast().map_err(|e| anyhow::anyhow!("{e}"))?;
        self.0.write_batch(typed).await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

// ── Type-erased batch operator ────────────────────────────────────────

/// Type-erased batch operator that processes [`ErasedBuffer`] → [`ErasedBuffer`].
#[async_trait]
pub(crate) trait ErasedBatchOperator: Send {
    /// Process an input buffer and produce output buffers.
    async fn process(
        &mut self,
        input: ErasedBuffer,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>>;
    /// Called when the global watermark advances.
    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>>;
    /// Called once at operator init.
    async fn open(&mut self, ctx: &mut OperatorContext) -> anyhow::Result<()>;
    /// Called once at operator shutdown.
    async fn close(&mut self) -> anyhow::Result<()>;
    /// Called when a timer fires.
    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>>;
    /// Clone for multi-worker execution.
    fn clone_erased(&self) -> Box<dyn ErasedBatchOperator>;
}

/// Wraps a typed [`StreamFunction`] into an [`ErasedBatchOperator`].
pub(crate) struct BatchOperatorWrapper<F: StreamFunction>(pub(crate) F);

#[async_trait]
impl<F> ErasedBatchOperator for BatchOperatorWrapper<F>
where
    F: StreamFunction + Clone + 'static,
{
    async fn process(
        &mut self,
        input: ErasedBuffer,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        let typed = input.downcast().map_err(|e| anyhow::anyhow!("{e}"))?;
        let output = match self.0.process(typed, ctx).await {
            Ok(out) => out,
            Err(e) => self.0.on_error(e, ctx).await?,
        };
        Ok(buffer_output_to_erased(output))
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        let output = self.0.on_watermark(watermark, ctx).await?;
        Ok(buffer_output_to_erased(output))
    }

    async fn open(&mut self, ctx: &mut OperatorContext) -> anyhow::Result<()> {
        self.0.open(ctx).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.0.close().await
    }

    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        let output = self.0.on_timer(timestamp, key, ctx).await?;
        Ok(buffer_output_to_erased(output))
    }

    fn clone_erased(&self) -> Box<dyn ErasedBatchOperator> {
        Box::new(BatchOperatorWrapper(self.0.clone()))
    }
}

fn buffer_output_to_erased<T: RheiSchema>(output: BufferOutput<T>) -> Vec<ErasedBuffer> {
    match output {
        BufferOutput::None => vec![],
        BufferOutput::Single(buf) => vec![ErasedBuffer::from_typed(buf)],
        BufferOutput::Multi(bufs) => bufs.into_iter().map(ErasedBuffer::from_typed).collect(),
    }
}
