//! Dataflow graph API for building stream processing pipelines.
//!
//! Provides [`DataflowGraph`] as a standalone builder and [`Stream<T>`] /
//! [`KeyedStream<T>`] as lightweight, copyable handles into it. Operations
//! like `.map()`, `.filter()`, `.key_by()`, and `.operator()` add nodes to
//! the graph. Pass the finished graph to
//! [`Executor::run()`](crate::executor::Executor::run) for execution.
//!
//! ```ignore
//! let graph = DataflowGraph::new();
//! let orders = graph.source(kafka_source);
//! orders
//!     .map(parse)
//!     .key_by(|o| o.customer_id.clone())
//!     .operator("enrich", EnrichOp)
//!     .map(format_output)
//!     .sink(kafka_sink);
//!
//! let executor = Executor::builder()
//!     .checkpoint_dir("./checkpoints")
//!     .workers(4)
//!     .build();
//!
//! executor.run(graph).await?;
//! ```

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, RwLock};

use async_trait::async_trait;
use rhei_core::state::context::StateContext;
use rhei_core::traits::{Sink, Source, StreamFunction};

// ── Cloneable type-erased wrapper ────────────────────────────────────

/// Object-safe trait for cloneable, type-erased values.
///
/// Implemented automatically for all `T: Clone + Any + Send + Debug`. This enables
/// `AnyItem` to genuinely clone its contents, which is required for Timely's
/// `Exchange` pact (used in distributed execution).
pub(crate) trait CloneAnySend: Any + Send {
    /// Clone the value into a new boxed trait object.
    fn clone_box(&self) -> Box<dyn CloneAnySend>;
    /// Convert to `Box<dyn Any + Send>` for downcasting.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
    /// Borrow as `&dyn Any` for type checking and ref downcasting.
    fn as_any_ref(&self) -> &dyn Any;
    /// Best-effort debug representation for DLQ diagnostics.
    fn debug_repr(&self) -> String;
    /// Stable hash of the concrete type name, used as a serialization tag.
    fn stable_type_id(&self) -> u64;
    /// Serialize the value to bytes via bincode.
    fn serialize_bytes(&self) -> Vec<u8>;
}

impl<T: Clone + Any + Send + std::fmt::Debug + serde::Serialize> CloneAnySend for T {
    fn clone_box(&self) -> Box<dyn CloneAnySend> {
        Box::new(self.clone())
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }

    fn debug_repr(&self) -> String {
        format!("{self:?}")
    }

    fn stable_type_id(&self) -> u64 {
        seahash::hash(std::any::type_name::<T>().as_bytes())
    }

    fn serialize_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("AnyItem serialization failed")
    }
}

// ── Global type registry for AnyItem deserialization ────────────────

type DeserFn = Box<dyn Fn(&[u8]) -> AnyItem + Send + Sync>;

static TYPE_REGISTRY: LazyLock<RwLock<std::collections::HashMap<u64, DeserFn>>> =
    LazyLock::new(|| RwLock::new(std::collections::HashMap::new()));

fn register_type<T>()
where
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    let type_hash = seahash::hash(std::any::type_name::<T>().as_bytes());
    let needs_insert = {
        let reg = TYPE_REGISTRY.read().unwrap();
        !reg.contains_key(&type_hash)
    };
    if needs_insert {
        let mut reg = TYPE_REGISTRY.write().unwrap();
        reg.entry(type_hash).or_insert_with(|| {
            Box::new(|bytes: &[u8]| {
                let value: T = bincode::deserialize(bytes).expect("AnyItem deserialization failed");
                AnyItem(Box::new(value))
            })
        });
    }
}

/// Cloneable, type-erased wrapper for pipeline elements.
///
/// All values flowing through the Timely dataflow are wrapped in `AnyItem`.
/// With `Clone` bounds on `StreamFunction::Input`/`Output`, the clone is
/// genuine (not a panic stub), enabling future use of Timely's `Exchange` pact.
pub(crate) struct AnyItem(Box<dyn CloneAnySend>);

impl Clone for AnyItem {
    fn clone(&self) -> Self {
        AnyItem(self.0.clone_box())
    }
}

impl AnyItem {
    /// Wrap a concrete typed value.
    pub(crate) fn new<T>(value: T) -> Self
    where
        T: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        register_type::<T>();
        AnyItem(Box::new(value))
    }

    /// Consume and downcast to concrete type `T`. Panics on type mismatch.
    pub(crate) fn downcast<T: 'static>(self) -> T {
        *self
            .0
            .into_any()
            .downcast::<T>()
            .unwrap_or_else(|_| panic!("AnyItem: expected {}", std::any::type_name::<T>()))
    }

    /// Borrow and downcast to `&T`. Panics on type mismatch.
    pub(crate) fn downcast_ref<T: 'static>(&self) -> &T {
        self.0
            .as_any_ref()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("AnyItem: expected &{}", std::any::type_name::<T>()))
    }

    /// Best-effort debug representation for DLQ diagnostics.
    #[allow(dead_code)]
    pub(crate) fn debug_repr(&self) -> String {
        self.0.debug_repr()
    }
}

impl serde::Serialize for AnyItem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&self.0.stable_type_id())?;
        tup.serialize_element(&self.0.serialize_bytes())?;
        tup.end()
    }
}

impl<'de> serde::Deserialize<'de> for AnyItem {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (type_hash, bytes): (u64, Vec<u8>) = serde::Deserialize::deserialize(deserializer)?;
        let reg = TYPE_REGISTRY.read().unwrap();
        let deser_fn = reg.get(&type_hash).ok_or_else(|| {
            serde::de::Error::custom(format!("unknown AnyItem type hash: {type_hash}"))
        })?;
        Ok(deser_fn(&bytes))
    }
}

// ── Node identity ────────────────────────────────────────────────────

/// Opaque identifier for a node in the dataflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(pub(crate) usize);

// ── Type-erased traits ───────────────────────────────────────────────

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
struct SourceWrapper<S: Source>(S);

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
        let partition_source = self.0.create_partition_source(assigned);
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

/// Type-erased sink: consumes [`AnyItem`].
#[async_trait]
pub(crate) trait ErasedSink: Send {
    async fn write(&mut self, item: AnyItem) -> anyhow::Result<()>;
    async fn flush(&mut self) -> anyhow::Result<()>;
}

/// Wraps a typed [`Sink`] into an [`ErasedSink`].
struct SinkWrapper<K: Sink>(K);

#[async_trait]
impl<K> ErasedSink for SinkWrapper<K>
where
    K: Sink + 'static,
    K::Input: 'static,
{
    async fn write(&mut self, item: AnyItem) -> anyhow::Result<()> {
        let typed: K::Input = item.downcast();
        self.0.write(typed).await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

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
struct OperatorWrapper<F: StreamFunction>(F);

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
        let typed: F::Input = input.downcast();
        let results = self.0.process(typed, ctx).await?;
        Ok(results.into_iter().map(AnyItem::new).collect())
    }

    async fn process_batch(
        &mut self,
        inputs: Vec<AnyItem>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        let typed: Vec<F::Input> = inputs.into_iter().map(AnyItem::downcast).collect();
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

/// Internal tag used by [`DlqErasedOperator`] to distinguish main outputs
/// from error outputs at the `AnyItem` level.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum DlqTag {
    /// A successful output item (the inner `AnyItem` has the operator's output type).
    Main(AnyItem),
    /// An error message from a failed `process` / `process_batch` call.
    Error(String),
}

impl std::fmt::Debug for DlqTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DlqTag::Main(item) => f.debug_tuple("Main").field(&item.debug_repr()).finish(),
            DlqTag::Error(msg) => f.debug_tuple("Error").field(msg).finish(),
        }
    }
}

/// Wraps a `Box<dyn ErasedOperator>`, converting `Err` results into
/// `DlqTag::Error` items instead of propagating them as pipeline failures.
///
/// Used by [`KeyedStream::with_dlq`] via graph rewriting.
struct DlqErasedOperator {
    inner: Box<dyn ErasedOperator>,
}

#[async_trait]
impl ErasedOperator for DlqErasedOperator {
    async fn process(
        &mut self,
        input: AnyItem,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        match self.inner.process(input, ctx).await {
            Ok(items) => Ok(items.into_iter().map(|i| AnyItem::new(DlqTag::Main(i))).collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::Error(e.to_string()))]),
        }
    }

    async fn process_batch(
        &mut self,
        inputs: Vec<AnyItem>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<AnyItem>> {
        match self.inner.process_batch(inputs, ctx).await {
            Ok(items) => Ok(items.into_iter().map(|i| AnyItem::new(DlqTag::Main(i))).collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::Error(e.to_string()))]),
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
            Ok(items) => Ok(items.into_iter().map(|i| AnyItem::new(DlqTag::Main(i))).collect()),
            Err(e) => Ok(vec![AnyItem::new(DlqTag::Error(e.to_string()))]),
        }
    }

    fn clone_erased(&self) -> Box<dyn ErasedOperator> {
        Box::new(DlqErasedOperator {
            inner: self.inner.clone_erased(),
        })
    }
}

// ── Transform context ────────────────────────────────────────────────

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

// ── Type-erased function types ───────────────────────────────────────

/// Stateless transform: one [`AnyItem`] in, zero or more out.
/// `Arc` for sharing across workers without cloning the closure.
pub(crate) type TransformFn = Arc<dyn Fn(AnyItem, &TransformContext) -> Vec<AnyItem> + Send + Sync>;

/// Key extraction function.
pub(crate) type KeyFn = Arc<dyn Fn(&AnyItem) -> String + Send + Sync>;

// ── Graph nodes ──────────────────────────────────────────────────────

/// The kind of processing a graph node performs.
pub(crate) enum NodeKind {
    /// A data source.
    Source(Box<dyn ErasedSource>),
    /// A stateless transform (map/filter/`flat_map`).
    Transform(TransformFn),
    /// A key-based exchange point.
    KeyBy(KeyFn),
    /// A stateful operator.
    Operator {
        /// Human-readable operator name (used for `StateContext` namespacing).
        name: String,
        /// The type-erased operator instance.
        op: Box<dyn ErasedOperator>,
    },
    /// Merges two input streams (placeholder for future use).
    Merge,
    /// A data sink (terminal node).
    Sink(Box<dyn ErasedSink>),
}

/// A node in the dataflow graph.
pub(crate) struct GraphNode {
    pub id: NodeId,
    pub kind: NodeKind,
    /// Input node IDs (0 for Source, 1 for Transform/KeyBy/Operator/Sink, 2 for Merge).
    pub inputs: Vec<NodeId>,
}

/// The dataflow graph: a collection of nodes connected by edges.
///
/// Build a graph by calling [`source()`](Self::source) to create entry points,
/// then chaining transforms and sinks on the returned [`Stream`] handles.
/// Pass the finished graph to [`Executor::run()`](crate::executor::Executor::run)
/// for execution.
///
/// Uses interior mutability (`RefCell`) so stream handles can add nodes
/// via shared `&DataflowGraph` references. Graph construction is
/// single-threaded — no `Mutex` needed.
pub struct DataflowGraph {
    // Debug: just show node count to avoid requiring Debug on NodeKind.
    nodes: RefCell<Vec<GraphNode>>,
}

impl std::fmt::Debug for DataflowGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataflowGraph")
            .field("node_count", &self.nodes.borrow().len())
            .finish()
    }
}

impl DataflowGraph {
    /// Create a new empty dataflow graph.
    pub fn new() -> Self {
        Self {
            nodes: RefCell::new(Vec::new()),
        }
    }

    /// Add a data source to the dataflow. Returns a [`Stream`] handle.
    pub fn source<S>(&self, source: S) -> Stream<'_, S::Output>
    where
        S: Source + 'static,
        S::Output: Clone
            + Send
            + Sync
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let id = self.add_node(NodeKind::Source(Box::new(SourceWrapper(source))), vec![]);
        Stream::new(self, id)
    }

    /// Add a node and return its ID.
    pub(crate) fn add_node(&self, kind: NodeKind, inputs: Vec<NodeId>) -> NodeId {
        let mut nodes = self.nodes.borrow_mut();
        let id = NodeId(nodes.len());
        nodes.push(GraphNode { id, kind, inputs });
        id
    }

    /// Consume the graph and return the raw node list for compilation.
    pub(crate) fn into_nodes(self) -> Vec<GraphNode> {
        self.nodes.into_inner()
    }
}

impl Default for DataflowGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ── Stream handle ────────────────────────────────────────────────────

/// A lightweight, copyable handle representing a point in the dataflow graph.
///
/// `T` is the element type flowing through this point. Operations add nodes
/// to the graph and return new handles.
///
/// Stateless transforms (`.map()`, `.filter()`, `.flat_map()`) are available
/// on `Stream`. For stateful operators, first call `.key_by()` to get a
/// [`KeyedStream`].
pub struct Stream<'a, T> {
    graph: &'a DataflowGraph,
    node_id: NodeId,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for Stream<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl<T> Clone for Stream<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Stream<'_, T> {}

impl<
    'a,
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> Stream<'a, T>
{
    pub(crate) fn new(graph: &'a DataflowGraph, node_id: NodeId) -> Self {
        Self {
            graph,
            node_id,
            _phantom: PhantomData,
        }
    }

    /// Transform each element.
    pub fn map<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T) -> O + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            vec![AnyItem::new(f(typed))]
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Transform each element with access to [`TransformContext`].
    pub fn map_ctx<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T, &TransformContext) -> O + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            vec![AnyItem::new(f(typed, ctx))]
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate.
    pub fn filter<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            if f(&typed) {
                vec![AnyItem::new(typed)]
            } else {
                vec![]
            }
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate, with access to
    /// [`TransformContext`].
    pub fn filter_ctx<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T, &TransformContext) -> bool + Send + Sync + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            if f(&typed, ctx) {
                vec![AnyItem::new(typed)]
            } else {
                vec![]
            }
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// One-to-many transform.
    pub fn flat_map<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T) -> Vec<O> + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            f(typed).into_iter().map(AnyItem::new).collect()
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// One-to-many transform with access to [`TransformContext`].
    pub fn flat_map_ctx<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T, &TransformContext) -> Vec<O> + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            f(typed, ctx).into_iter().map(AnyItem::new).collect()
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Partition elements by key. Returns a [`KeyedStream`] that supports
    /// stateful operators.
    ///
    /// The key function determines worker assignment via `hash(key) % workers`.
    /// All elements with the same key are guaranteed to land on the same worker.
    pub fn key_by<KF>(self, key_fn: KF) -> KeyedStream<'a, T>
    where
        KF: Fn(&T) -> String + Send + Sync + 'static,
    {
        let erased_key_fn: KeyFn = Arc::new(move |item: &AnyItem| {
            let typed = item.downcast_ref::<T>();
            key_fn(typed)
        });
        let node_id = self
            .graph
            .add_node(NodeKind::KeyBy(erased_key_fn), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Combine two streams of the same type. The result is an unkeyed `Stream`.
    pub fn merge(self, other: Stream<'a, T>) -> Stream<'a, T> {
        let node_id = self
            .graph
            .add_node(NodeKind::Merge, vec![self.node_id, other.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Terminal: write elements to a sink.
    pub fn sink<K>(self, sink: K)
    where
        K: Sink<Input = T> + 'static,
    {
        self.graph.add_node(
            NodeKind::Sink(Box::new(SinkWrapper(sink))),
            vec![self.node_id],
        );
    }
}

// ── KafkaMessage convenience methods ─────────────────────────────────

#[cfg(feature = "kafka")]
impl<'a> Stream<'a, rhei_core::connectors::kafka::types::KafkaMessage> {
    /// Deserialize Kafka message payloads as JSON, dropping messages that
    /// fail to parse (with a warning log).
    ///
    /// ```ignore
    /// let events: Stream<MyEvent> = graph
    ///     .source(kafka_source)
    ///     .parse_json::<MyEvent>();
    /// ```
    pub fn parse_json<O>(self) -> Stream<'a, O>
    where
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        self.flat_map(|msg| {
            let payload = msg.payload.as_deref().unwrap_or_default();
            match serde_json::from_slice::<O>(payload) {
                Ok(val) => vec![val],
                Err(e) => {
                    tracing::warn!(
                        topic = %msg.topic,
                        partition = msg.partition,
                        offset = msg.offset,
                        error = %e,
                        "Failed to deserialize JSON payload"
                    );
                    vec![]
                }
            }
        })
    }
}

// ── Kafka JSON serialization helpers ─────────────────────────────────

#[cfg(feature = "kafka")]
impl<
    'a,
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> Stream<'a, T>
{
    /// Serialize each element as JSON into a [`KafkaRecord`] with no key.
    ///
    /// ```ignore
    /// stream.to_json().sink(kafka_sink);
    /// ```
    pub fn to_json(self) -> Stream<'a, rhei_core::connectors::kafka::types::KafkaRecord> {
        self.map(|item| {
            let payload = serde_json::to_vec(&item).expect("JSON serialization failed");
            rhei_core::connectors::kafka::types::KafkaRecord::new(payload)
        })
    }

    /// Serialize each element as JSON into a [`KafkaRecord`] with a key
    /// extracted by the given closure.
    ///
    /// ```ignore
    /// stream
    ///     .to_json_keyed(|event| event.id.clone().into_bytes())
    ///     .sink(kafka_sink);
    /// ```
    pub fn to_json_keyed<F>(
        self,
        key_fn: F,
    ) -> Stream<'a, rhei_core::connectors::kafka::types::KafkaRecord>
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        self.map(move |item| {
            let key = key_fn(&item);
            let payload = serde_json::to_vec(&item).expect("JSON serialization failed");
            rhei_core::connectors::kafka::types::KafkaRecord::with_key(key, payload)
        })
    }
}

// ── KeyedStream handle ───────────────────────────────────────────────

/// A stream partitioned by key. Only `KeyedStream` supports stateful operators.
///
/// Returned by [`Stream::key_by()`]. Stateless transforms (`.map()`,
/// `.filter()`, `.flat_map()`) preserve the partitioning. Calling
/// `.key_by()` again re-partitions (triggers a new exchange).
pub struct KeyedStream<'a, T> {
    graph: &'a DataflowGraph,
    node_id: NodeId,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for KeyedStream<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyedStream")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl<T> Clone for KeyedStream<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for KeyedStream<'_, T> {}

impl<
    'a,
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> KeyedStream<'a, T>
{
    pub(crate) fn new(graph: &'a DataflowGraph, node_id: NodeId) -> Self {
        Self {
            graph,
            node_id,
            _phantom: PhantomData,
        }
    }

    /// Transform each element (preserves partitioning).
    pub fn map<F, O>(self, f: F) -> KeyedStream<'a, O>
    where
        F: Fn(T) -> O + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            vec![AnyItem::new(f(typed))]
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Transform each element with access to [`TransformContext`] (preserves
    /// partitioning).
    pub fn map_ctx<F, O>(self, f: F) -> KeyedStream<'a, O>
    where
        F: Fn(T, &TransformContext) -> O + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            vec![AnyItem::new(f(typed, ctx))]
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate (preserves partitioning).
    pub fn filter<F>(self, f: F) -> KeyedStream<'a, T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            if f(&typed) {
                vec![AnyItem::new(typed)]
            } else {
                vec![]
            }
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate, with access to
    /// [`TransformContext`] (preserves partitioning).
    pub fn filter_ctx<F>(self, f: F) -> KeyedStream<'a, T>
    where
        F: Fn(&T, &TransformContext) -> bool + Send + Sync + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            if f(&typed, ctx) {
                vec![AnyItem::new(typed)]
            } else {
                vec![]
            }
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// One-to-many transform (preserves partitioning).
    pub fn flat_map<F, O>(self, f: F) -> KeyedStream<'a, O>
    where
        F: Fn(T) -> Vec<O> + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, _ctx| {
            let typed: T = item.downcast();
            f(typed).into_iter().map(AnyItem::new).collect()
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// One-to-many transform with access to [`TransformContext`] (preserves
    /// partitioning).
    pub fn flat_map_ctx<F, O>(self, f: F) -> KeyedStream<'a, O>
    where
        F: Fn(T, &TransformContext) -> Vec<O> + Send + Sync + 'static,
        O: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: AnyItem, ctx| {
            let typed: T = item.downcast();
            f(typed, ctx).into_iter().map(AnyItem::new).collect()
        });
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(transform), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Add a stateful operator. The operator is cloned per worker, and each
    /// worker gets its own [`StateContext`] created automatically.
    ///
    /// Only available on `KeyedStream` — this is enforced at compile time.
    pub fn operator<Func>(self, name: &str, func: Func) -> KeyedStream<'a, Func::Output>
    where
        Func: StreamFunction<Input = T> + Clone + 'static,
        Func::Output: serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        let node_id = self.graph.add_node(
            NodeKind::Operator {
                name: name.to_string(),
                op: Box::new(OperatorWrapper(func)),
            },
            vec![self.node_id],
        );
        KeyedStream::new(self.graph, node_id)
    }

    /// Attach a dead-letter queue to the preceding operator.
    ///
    /// Rewrites the operator so that `Err(e)` results are routed to an error
    /// stream instead of crashing the pipeline. The closure receives the error
    /// stream (`Stream<String>`) for wiring (e.g. to a Kafka DLQ sink).
    /// Returns the main `KeyedStream<T>` for continued chaining.
    ///
    /// ```ignore
    /// stream
    ///     .key_by(|x| x.key.clone())
    ///     .operator("process", MyProcessor)
    ///     .with_dlq(|errors| errors.to_json().sink(dlq_sink))
    ///     .to_json_keyed(|e| e.key())
    ///     .sink(output_sink);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the preceding node is not an `Operator`.
    pub fn with_dlq<F>(self, f: F) -> KeyedStream<'a, T>
    where
        F: FnOnce(Stream<'a, String>),
    {
        // Rewrite the operator node to wrap it in DlqErasedOperator.
        {
            let mut nodes = self.graph.nodes.borrow_mut();
            let node = &mut nodes[self.node_id.0];
            match &mut node.kind {
                NodeKind::Operator { op, .. } => {
                    let temp = op.clone_erased();
                    let inner = std::mem::replace(op, temp);
                    *op = Box::new(DlqErasedOperator { inner });
                }
                other => panic!(
                    "with_dlq called on non-operator node: {:?}",
                    std::mem::discriminant(other)
                ),
            }
        }

        // The operator now emits DlqTag items. Split into main/error streams.
        let main_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let tag: DlqTag = item.downcast();
            match tag {
                DlqTag::Main(inner) => vec![inner],
                DlqTag::Error(_) => vec![],
            }
        });
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(main_transform), vec![self.node_id]);

        let side_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let tag: DlqTag = item.downcast();
            match tag {
                DlqTag::Main(_) => vec![],
                DlqTag::Error(msg) => vec![AnyItem::new(msg)],
            }
        });
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(side_transform), vec![self.node_id]);

        // Wire the error stream via the user's closure.
        f(Stream::new(self.graph, side_id));

        KeyedStream::new(self.graph, main_id)
    }

    /// Re-partition by a new key (triggers a new exchange).
    pub fn key_by<KF>(self, key_fn: KF) -> KeyedStream<'a, T>
    where
        KF: Fn(&T) -> String + Send + Sync + 'static,
    {
        let erased_key_fn: KeyFn = Arc::new(move |item: &AnyItem| {
            let typed = item.downcast_ref::<T>();
            key_fn(typed)
        });
        let node_id = self
            .graph
            .add_node(NodeKind::KeyBy(erased_key_fn), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Per-key rolling reduce. Emits the updated value on every input.
    pub fn reduce<F, KF>(self, name: &str, key_fn: KF, reduce_fn: F) -> KeyedStream<'a, T>
    where
        T: Sync,
        F: Fn(T, T) -> T + Send + Sync + Clone + 'static,
        KF: Fn(&T) -> String + Send + Sync + Clone + 'static,
    {
        self.operator(
            name,
            rhei_core::operators::reduce::ReduceOp::new(key_fn, reduce_fn),
        )
    }

    /// Per-key rolling aggregation. Emits the updated aggregate on every input.
    pub fn aggregate<A, KF>(self, name: &str, key_fn: KF, agg: A) -> KeyedStream<'a, A::Output>
    where
        T: Sync,
        A: rhei_core::operators::aggregator::Aggregator<Input = T> + Send + Sync + Clone + 'static,
        A::Accumulator: serde::Serialize + serde::de::DeserializeOwned,
        A::Output: Clone
            + Send
            + Sync
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
        KF: Fn(&T) -> String + Send + Sync + Clone + 'static,
    {
        self.operator(
            name,
            rhei_core::operators::rolling_aggregate::RollingAggregateOp::new(key_fn, agg),
        )
    }

    /// Async enrichment with bounded concurrency.
    ///
    /// Wraps each element through the async function `f` with up to
    /// `concurrency` parallel lookups per batch. Only on `KeyedStream` for
    /// worker affinity.
    pub fn enrich<F, O, Fut>(
        self,
        name: &str,
        concurrency: usize,
        timeout: std::time::Duration,
        f: F,
    ) -> KeyedStream<'a, O>
    where
        T: Sync + 'static,
        O: Clone
            + Send
            + Sync
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
        Fut: std::future::Future<Output = anyhow::Result<O>> + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    {
        self.operator(
            name,
            rhei_core::operators::enrich::EnrichOp::new(concurrency, timeout, f),
        )
    }

    /// Terminal: write elements to a sink.
    pub fn sink<K>(self, sink: K)
    where
        K: Sink<Input = T> + 'static,
    {
        self.graph.add_node(
            NodeKind::Sink(Box::new(SinkWrapper(sink))),
            vec![self.node_id],
        );
    }
}

// ── Kafka JSON serialization helpers (KeyedStream) ───────────────────

#[cfg(feature = "kafka")]
impl<
    'a,
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> KeyedStream<'a, T>
{
    /// Serialize each element as JSON into a [`KafkaRecord`] with no key.
    pub fn to_json(self) -> KeyedStream<'a, rhei_core::connectors::kafka::types::KafkaRecord> {
        self.map(|item| {
            let payload = serde_json::to_vec(&item).expect("JSON serialization failed");
            rhei_core::connectors::kafka::types::KafkaRecord::new(payload)
        })
    }

    /// Serialize each element as JSON into a [`KafkaRecord`] with a key
    /// extracted by the given closure.
    pub fn to_json_keyed<F>(
        self,
        key_fn: F,
    ) -> KeyedStream<'a, rhei_core::connectors::kafka::types::KafkaRecord>
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        self.map(move |item| {
            let key = key_fn(&item);
            let payload = serde_json::to_vec(&item).expect("JSON serialization failed");
            rhei_core::connectors::kafka::types::KafkaRecord::with_key(key, payload)
        })
    }
}

// ── Side output split ────────────────────────────────────────────────

use rhei_core::operators::with_side::WithSide;

impl<
    'a,
    M: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
    S: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> Stream<'a, WithSide<M, S>>
{
    /// Split a `WithSide<M, S>` stream into its main and side components.
    ///
    /// Adds two Transform nodes that filter and unwrap from the same upstream.
    pub fn split_side(self) -> (Stream<'a, M>, Stream<'a, S>) {
        let main_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let ws: WithSide<M, S> = item.downcast();
            match ws {
                WithSide::Main(m) => vec![AnyItem::new(m)],
                WithSide::Side(_) => vec![],
            }
        });
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(main_transform), vec![self.node_id]);

        let side_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let ws: WithSide<M, S> = item.downcast();
            match ws {
                WithSide::Main(_) => vec![],
                WithSide::Side(s) => vec![AnyItem::new(s)],
            }
        });
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(side_transform), vec![self.node_id]);

        (
            Stream::new(self.graph, main_id),
            Stream::new(self.graph, side_id),
        )
    }
}

impl<
    'a,
    M: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
    S: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
> KeyedStream<'a, WithSide<M, S>>
{
    /// Split a `WithSide<M, S>` keyed stream into main (keyed) and side (unkeyed).
    ///
    /// The main stream preserves key partitioning; the side stream is unkeyed.
    pub fn split_side(self) -> (KeyedStream<'a, M>, Stream<'a, S>) {
        let main_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let ws: WithSide<M, S> = item.downcast();
            match ws {
                WithSide::Main(m) => vec![AnyItem::new(m)],
                WithSide::Side(_) => vec![],
            }
        });
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(main_transform), vec![self.node_id]);

        let side_transform: TransformFn = Arc::new(|item: AnyItem, _ctx| {
            let ws: WithSide<M, S> = item.downcast();
            match ws {
                WithSide::Main(_) => vec![],
                WithSide::Side(s) => vec![AnyItem::new(s)],
            }
        });
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(side_transform), vec![self.node_id]);

        (
            KeyedStream::new(self.graph, main_id),
            Stream::new(self.graph, side_id),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_output_type_makes_anyitem_deserializable() {
        // Register String in the type registry.
        register_type::<String>();

        // Serialize an AnyItem<String>.
        let item = AnyItem::new("hello".to_string());
        let bytes = bincode::serialize(&item).unwrap();

        // Deserialize — should succeed because String is registered.
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<String>(), "hello");
    }

    #[test]
    fn source_wrapper_registers_output_type() {
        // Create a VecSource<u16> and wrap it.
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1u16, 2, 3]);
        let wrapper = SourceWrapper(source);

        // Calling register_output_type should register u16.
        wrapper.register_output_type();

        // Verify by serializing + deserializing an AnyItem<u16>.
        let item = AnyItem::new(42u16);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<u16>(), 42);
    }
}
