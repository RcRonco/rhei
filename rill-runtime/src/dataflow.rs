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
use std::sync::Arc;

use async_trait::async_trait;
use rill_core::state::context::StateContext;
use rill_core::traits::{Sink, Source, StreamFunction};

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
}

impl<T: Clone + Any + Send + std::fmt::Debug> CloneAnySend for T {
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
    pub(crate) fn new<T: Clone + Send + std::fmt::Debug + 'static>(value: T) -> Self {
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
    pub(crate) fn debug_repr(&self) -> String {
        self.0.debug_repr()
    }
}

// ── Node identity ────────────────────────────────────────────────────

/// Opaque identifier for a node in the dataflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(pub(crate) usize);

// ── Type-erased traits ───────────────────────────────────────────────

/// Type-erased source: produces batches of [`AnyItem`].
#[async_trait]
pub(crate) trait ErasedSource: Send {
    async fn next_batch(&mut self) -> Option<Vec<AnyItem>>;
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()>;
    fn current_offsets(&self) -> std::collections::HashMap<String, String>;
}

/// Wraps a typed [`Source`] into an [`ErasedSource`].
struct SourceWrapper<S: Source>(S);

#[async_trait]
impl<S> ErasedSource for SourceWrapper<S>
where
    S: Source + 'static,
    S::Output: Clone + std::fmt::Debug + 'static,
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
    async fn process(
        &mut self,
        input: AnyItem,
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
    F::Input: 'static,
    F::Output: 'static,
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

    fn clone_erased(&self) -> Box<dyn ErasedOperator> {
        Box::new(OperatorWrapper(self.0.clone()))
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
        S::Output: Clone + Send + std::fmt::Debug + 'static,
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

impl<'a, T: Clone + Send + std::fmt::Debug + 'static> Stream<'a, T> {
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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

impl<'a, T: Clone + Send + std::fmt::Debug + 'static> KeyedStream<'a, T> {
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        O: Clone + Send + std::fmt::Debug + 'static,
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
        Func::Output: 'static,
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
