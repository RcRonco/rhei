//! Type-erased traits and wrappers for the Timely execution layer.
//!
//! These types bridge the typed graph-level API ([`DataflowGraph`](crate::dataflow::DataflowGraph))
//! to the monomorphic `AnyItem` streams required by Timely Dataflow. They are
//! the compile targets for the graph-node traits in [`dataflow`](crate::dataflow).

use std::sync::Arc;

use async_trait::async_trait;
use rhei_core::state::context::StateContext;
use rhei_core::traits::{Sink, Source, StreamFunction};

use crate::any_item::{AnyItem, register_type};

// ── Type-erased source ──────────────────────────────────────────────

/// Type-erased source: produces batches of [`AnyItem`].
#[async_trait]
#[allow(dead_code)]
pub(crate) trait ErasedSource: Send {
    async fn next_batch(&mut self) -> Option<Vec<AnyItem>>;
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()>;
    fn current_offsets(&self) -> std::collections::HashMap<String, String>;
    async fn restore_offsets(
        &mut self,
        offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()>;
    fn partition_count(&self) -> Option<usize>;
    fn create_partition_source(&self, assigned: &[usize]) -> Option<Box<dyn ErasedSource>>;
    /// Returns the current event-time watermark (millis), if available.
    fn current_watermark(&self) -> Option<u64>;
    /// Register the output type in the global `AnyItem` type registry.
    ///
    /// Must be called before Timely starts exchanging data across processes,
    /// so that cross-process deserialization can find the correct type.
    fn register_output_type(&self);
}

/// Wraps a typed [`Source`] into an [`ErasedSource`].
pub(crate) struct SourceWrapper<S: Source>(pub(crate) S);

#[async_trait]
impl<S> ErasedSource for SourceWrapper<S>
where
    S: Source + 'static,
    S::Output:
        Clone + Sync + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    async fn next_batch(&mut self) -> Option<Vec<AnyItem>> {
        let batch = self.0.next_batch().await?;
        Some(batch.into_iter().map(AnyItem::new).collect())
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

    fn register_output_type(&self) {
        register_type::<S::Output>();
    }
}

/// Wraps a `Box<dyn Source<Output = T>>` into an [`ErasedSource`].
///
/// Used for partition sources returned by `create_partition_source()`,
/// which are already type-erased at the `Source` level but not at the
/// `ErasedSource` level.
struct DynSourceWrapper<T>(Box<dyn Source<Output = T>>);

#[async_trait]
impl<T> ErasedSource for DynSourceWrapper<T>
where
    T: Clone
        + Send
        + Sync
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
{
    async fn next_batch(&mut self) -> Option<Vec<AnyItem>> {
        let batch = self.0.next_batch().await?;
        Some(batch.into_iter().map(AnyItem::new).collect())
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

    fn register_output_type(&self) {
        register_type::<T>();
    }
}

// ── Type-erased sink ────────────────────────────────────────────────

/// Type-erased sink: consumes [`AnyItem`].
#[async_trait]
pub(crate) trait ErasedSink: Send {
    async fn write(&mut self, item: AnyItem) -> anyhow::Result<()>;
    async fn flush(&mut self) -> anyhow::Result<()>;
}

/// Wraps a typed [`Sink`] into an [`ErasedSink`].
pub(crate) struct SinkWrapper<K: Sink>(pub(crate) K);

#[async_trait]
impl<K> ErasedSink for SinkWrapper<K>
where
    K: Sink + 'static,
    K::Input: 'static,
{
    async fn write(&mut self, item: AnyItem) -> anyhow::Result<()> {
        let typed: K::Input = item.try_downcast().map_err(|e| anyhow::anyhow!("{e}"))?;
        self.0.write(typed).await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

// ── Type-erased operator ────────────────────────────────────────────

/// Type-erased stateful operator. Must be cloneable for multi-worker.
#[async_trait]
pub(crate) trait ErasedOperator: Send {
    #[allow(dead_code)]
    async fn process(
        &mut self,
        input: AnyItem,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>>;
    /// Process a batch of inputs in a single async call.
    async fn process_batch(
        &mut self,
        inputs: Vec<AnyItem>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>>;
    /// Called when the global watermark advances.
    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>>;
    /// Called once at operator init, before any `process`.
    async fn open(&mut self, ctx: &mut StateContext) -> anyhow::Result<()>;
    /// Called once at operator shutdown.
    async fn close(&mut self) -> anyhow::Result<()>;
    /// Called when a timer fires.
    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>>;
    fn clone_erased(&self) -> Box<dyn ErasedOperator>;
}

/// Wraps a typed [`StreamFunction`] into an [`ErasedOperator`].
pub(crate) struct OperatorWrapper<F: StreamFunction>(pub(crate) F);

#[async_trait]
impl<F> ErasedOperator for OperatorWrapper<F>
where
    F: StreamFunction + Clone + 'static,
    F::Input: serde::Serialize + serde::de::DeserializeOwned + 'static,
    F::Output: serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    async fn process(
        &mut self,
        input: AnyItem,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        let typed: F::Input = input.try_downcast().map_err(|e| anyhow::anyhow!("{e}"))?;
        let results = self.0.process(typed, ctx).await?;
        Ok(results.into_iter().map(AnyItem::new).collect())
    }

    async fn process_batch(
        &mut self,
        inputs: Vec<AnyItem>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        let typed: Vec<F::Input> = inputs
            .into_iter()
            .map(|item| item.try_downcast().map_err(|e| anyhow::anyhow!("{e}")))
            .collect::<Result<Vec<_>, _>>()?;
        let results = self.0.process_batch(typed, ctx).await?;
        Ok(results.into_iter().map(AnyItem::new).collect())
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        let results = self.0.on_watermark(watermark, ctx).await?;
        Ok(results.into_iter().map(AnyItem::new).collect())
    }

    async fn open(&mut self, ctx: &mut StateContext) -> anyhow::Result<()> {
        self.0.open(ctx).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.0.close().await
    }

    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        let results = self.0.on_timer(timestamp, key, ctx).await?;
        Ok(results.into_iter().map(AnyItem::new).collect())
    }

    fn clone_erased(&self) -> Box<dyn ErasedOperator> {
        Box::new(OperatorWrapper(self.0.clone()))
    }
}

// ── DLQ wrapper ─────────────────────────────────────────────────────

/// Tag for distinguishing main outputs from error outputs.
///
/// Generic over `T` so it can be used at both the typed and erased level.
/// At the Timely boundary the concrete type is `DlqTag<AnyItem>`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum DlqTag<T> {
    /// A successful output item.
    Main(T),
    /// An error message from a failed `process` / `process_batch` call.
    Error(String),
}

impl<T: std::fmt::Debug> std::fmt::Debug for DlqTag<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DlqTag::Main(item) => f.debug_tuple("Main").field(item).finish(),
            DlqTag::Error(msg) => f.debug_tuple("Error").field(msg).finish(),
        }
    }
}

/// Wraps a `Box<dyn ErasedOperator>`, converting `Err` results into
/// `DlqTag::Error` items instead of propagating them as pipeline failures.
pub(crate) struct DlqErasedOperator {
    pub(crate) inner: Box<dyn ErasedOperator>,
}

#[async_trait]
impl ErasedOperator for DlqErasedOperator {
    async fn process(
        &mut self,
        input: AnyItem,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        match self.inner.process(input, ctx).await {
            Ok(items) => Ok(items
                .into_iter()
                .map(|i| AnyItem::new(DlqTag::<AnyItem>::Main(i)))
                .collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::<AnyItem>::Error(e.to_string()))]),
        }
    }

    async fn process_batch(
        &mut self,
        inputs: Vec<AnyItem>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        match self.inner.process_batch(inputs, ctx).await {
            Ok(items) => Ok(items
                .into_iter()
                .map(|i| AnyItem::new(DlqTag::<AnyItem>::Main(i)))
                .collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::<AnyItem>::Error(e.to_string()))]),
        }
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        // Watermark errors are not DLQ-able — propagate them.
        self.inner.on_watermark(watermark, ctx).await
    }

    async fn open(&mut self, ctx: &mut StateContext) -> anyhow::Result<()> {
        self.inner.open(ctx).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner.close().await
    }

    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        match self.inner.on_timer(timestamp, key, ctx).await {
            Ok(items) => Ok(items
                .into_iter()
                .map(|i| AnyItem::new(DlqTag::<AnyItem>::Main(i)))
                .collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::<AnyItem>::Error(e.to_string()))]),
        }
    }

    fn clone_erased(&self) -> Box<dyn ErasedOperator> {
        Box::new(DlqErasedOperator {
            inner: self.inner.clone_erased(),
        })
    }
}

// ── Transform context & function types ──────────────────────────────

/// Runtime context available to stateless transforms.
///
/// Passed to the `_ctx` variants of `.map()`, `.filter()`, and `.flat_map()`
/// so user closures can observe worker-level metadata without requiring a
/// full stateful operator.
#[derive(Debug, Clone)]
pub struct TransformContext {
    /// Zero-based index of the current worker thread.
    pub worker_index: usize,
    /// Total number of worker threads in this execution.
    pub num_workers: usize,
}

/// Stateless transform: one [`AnyItem`] in, zero or more out.
/// `Arc` for sharing across workers without cloning the closure.
pub(crate) type TransformFn = Arc<dyn Fn(AnyItem, &TransformContext) -> Vec<AnyItem> + Send + Sync>;

/// Key extraction function.
pub(crate) type KeyFn = Arc<dyn Fn(&AnyItem) -> String + Send + Sync>;
