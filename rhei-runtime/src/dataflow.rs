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

use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::Arc;

use rhei_core::traits::{Sink, Source, StreamFunction};

use crate::any_item::AnyItem;
use crate::erased::{
    DlqErasedOperator, DlqTag, ErasedOperator, ErasedSink, ErasedSource, KeyFn, OperatorWrapper,
    SinkWrapper, SourceWrapper, TransformFn,
};

// Re-export TransformContext for public API compatibility.
pub use crate::erased::TransformContext;

// ── Node identity ────────────────────────────────────────────────────

/// Opaque identifier for a node in the dataflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(pub(crate) usize);

// ── Graph-node compile traits ────────────────────────────────────────
//
// These traits represent dataflow nodes at the *graph construction* level.
// Each trait's `compile()` method bridges to the AnyItem-based execution
// layer used by the Timely executor. This keeps AnyItem out of the
// user-facing graph API — conversion is deferred to compilation time.

/// A source node that produces data batches. Compiles to [`ErasedSource`].
pub(crate) trait SourceNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSource>;
}

/// A stateless transform node (`map`/`filter`/`flat_map`). Compiles to [`TransformFn`].
pub(crate) trait TransformNode: Send {
    fn compile(self: Box<Self>) -> TransformFn;
}

/// A key-extraction node for partitioning. Compiles to [`KeyFn`].
pub(crate) trait KeyByNode: Send {
    fn compile(self: Box<Self>) -> KeyFn;
}

/// A stateful operator node. Compiles to [`ErasedOperator`].
pub(crate) trait OperatorNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedOperator>;
}

/// A terminal sink node. Compiles to [`ErasedSink`].
pub(crate) trait SinkNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSink>;
}

// ── Typed node wrappers ─────────────────────────────────────────────

/// Wraps a typed [`Source`] for deferred compilation.
pub(crate) struct TypedSourceNode<S: Source>(pub(crate) S);

impl<S> SourceNode for TypedSourceNode<S>
where
    S: Source + Send + 'static,
    S::Output: Clone
        + Send
        + Sync
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedSource> {
        Box::new(SourceWrapper(self.0))
    }
}

/// Deferred transform: stores a factory that produces the AnyItem-based closure
/// at compile time rather than at graph construction time.
pub(crate) struct LazyTransformNode(pub(crate) Box<dyn FnOnce() -> TransformFn + Send>);

impl TransformNode for LazyTransformNode {
    fn compile(self: Box<Self>) -> TransformFn {
        (self.0)()
    }
}

/// Deferred key function: stores a factory that produces the AnyItem-based key fn.
pub(crate) struct LazyKeyByNode(pub(crate) Box<dyn FnOnce() -> KeyFn + Send>);

impl KeyByNode for LazyKeyByNode {
    fn compile(self: Box<Self>) -> KeyFn {
        (self.0)()
    }
}

/// Wraps a typed [`StreamFunction`] for deferred compilation.
pub(crate) struct TypedOperatorNode<F: StreamFunction>(pub(crate) F);

impl<F> OperatorNode for TypedOperatorNode<F>
where
    F: StreamFunction + Clone + Send + 'static,
    F::Input: serde::Serialize + serde::de::DeserializeOwned + 'static,
    F::Output: serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedOperator> {
        Box::new(OperatorWrapper(self.0))
    }
}

/// Wraps an [`OperatorNode`] with dead-letter-queue error routing.
///
/// Compiles the inner operator first, then wraps it in [`DlqErasedOperator`].
pub(crate) struct DlqOperatorNode(pub(crate) Box<dyn OperatorNode>);

impl OperatorNode for DlqOperatorNode {
    fn compile(self: Box<Self>) -> Box<dyn ErasedOperator> {
        let inner = self.0.compile();
        Box::new(DlqErasedOperator { inner })
    }
}

/// Wraps a typed [`Sink`] for deferred compilation.
pub(crate) struct TypedSinkNode<K: Sink>(pub(crate) K);

impl<K> SinkNode for TypedSinkNode<K>
where
    K: Sink + Send + 'static,
    K::Input: 'static,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedSink> {
        Box::new(SinkWrapper(self.0))
    }
}

// ── Graph nodes ──────────────────────────────────────────────────────

/// The kind of processing a graph node performs.
///
/// Each variant stores a graph-level compile trait rather than an
/// AnyItem-based erased form. Call the corresponding `compile()` method
/// to produce the execution-layer form needed by the Timely executor.
pub(crate) enum NodeKind {
    /// A data source.
    Source(Box<dyn SourceNode>),
    /// A stateless transform (map/filter/`flat_map`).
    Transform(Box<dyn TransformNode>),
    /// A key-based exchange point.
    KeyBy(Box<dyn KeyByNode>),
    /// A stateful operator.
    Operator {
        /// Human-readable operator name (used for `StateContext` namespacing).
        name: String,
        /// The typed operator node.
        op: Box<dyn OperatorNode>,
    },
    /// Merges two input streams (placeholder for future use).
    Merge,
    /// A data sink (terminal node).
    Sink(Box<dyn SinkNode>),
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
        S: Source + Send + 'static,
        S::Output: Clone
            + Send
            + Sync
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        let id = self.add_node(NodeKind::Source(Box::new(TypedSourceNode(source))), vec![]);
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

// ── Graph validation ────────────────────────────────────────────────

/// Errors produced by [`DataflowGraph::validate()`].
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// The graph has no nodes at all.
    EmptyGraph,
    /// The graph has no source nodes.
    NoSources,
    /// The graph has no sink nodes.
    NoSinks,
    /// One or more streams do not terminate at a sink.
    ///
    /// Each entry is `(node_index, node_kind_label)` for the dangling
    /// leaf node.
    DanglingStreams(Vec<(usize, String)>),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyGraph => write!(f, "dataflow graph is empty — add at least one source"),
            Self::NoSources => write!(
                f,
                "dataflow graph has no sources — call graph.source(...) to add one"
            ),
            Self::NoSinks => write!(
                f,
                "dataflow graph has no sinks — every stream must end with .sink(...)"
            ),
            Self::DanglingStreams(nodes) => {
                writeln!(
                    f,
                    "the following streams do not reach a sink (call .sink(...) on each):"
                )?;
                for (idx, kind) in nodes {
                    writeln!(f, "  - node {idx} ({kind})")?;
                }
                write!(
                    f,
                    "hint: every path from a source must terminate at a .sink()"
                )
            }
        }
    }
}

impl std::error::Error for ValidationError {}

impl DataflowGraph {
    /// Validate the graph structure before execution.
    ///
    /// Checks:
    /// - The graph is not empty
    /// - At least one source exists
    /// - At least one sink exists
    /// - Every non-sink leaf node can reach a sink (no dangling streams)
    ///
    /// Call this before passing the graph to
    /// [`Executor::run()`](crate::executor::Executor::run) to get clear,
    /// actionable error messages instead of runtime failures.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = DataflowGraph::new();
    /// graph.source(my_source)
    ///     .map(|x| x * 2);
    ///     // Oops — forgot .sink(...)
    ///
    /// if let Err(e) = graph.validate() {
    ///     eprintln!("Graph error: {e}");
    ///     // "the following streams do not reach a sink..."
    /// }
    /// ```
    pub fn validate(&self) -> Result<(), ValidationError> {
        let nodes = self.nodes.borrow();

        if nodes.is_empty() {
            return Err(ValidationError::EmptyGraph);
        }

        let has_sources = nodes.iter().any(|n| matches!(n.kind, NodeKind::Source(_)));
        if !has_sources {
            return Err(ValidationError::NoSources);
        }

        let has_sinks = nodes.iter().any(|n| matches!(n.kind, NodeKind::Sink(_)));
        if !has_sinks {
            return Err(ValidationError::NoSinks);
        }

        // Build successor adjacency list to find dangling leaves.
        let n = nodes.len();
        let mut successors: Vec<Vec<usize>> = vec![vec![]; n];
        for node in nodes.iter() {
            for &input_id in &node.inputs {
                successors[input_id.0].push(node.id.0);
            }
        }

        // Find leaf nodes (no successors) that are NOT sinks.
        let dangling: Vec<(usize, String)> = nodes
            .iter()
            .filter(|node| {
                successors[node.id.0].is_empty() && !matches!(node.kind, NodeKind::Sink(_))
            })
            .map(|node| {
                let kind_label = match &node.kind {
                    NodeKind::Source(_) => "source".to_string(),
                    NodeKind::Transform(_) => "transform".to_string(),
                    NodeKind::KeyBy(_) => "key_by".to_string(),
                    NodeKind::Operator { name, .. } => {
                        format!("operator \"{name}\"")
                    }
                    NodeKind::Merge => "merge".to_string(),
                    NodeKind::Sink(_) => unreachable!(),
                };
                (node.id.0, kind_label)
            })
            .collect();

        if !dangling.is_empty() {
            return Err(ValidationError::DanglingStreams(dangling));
        }

        Ok(())
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                vec![AnyItem::new(f(typed))]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                vec![AnyItem::new(f(typed, ctx))]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate.
    pub fn filter<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                if f(&typed) {
                    vec![AnyItem::new(typed)]
                } else {
                    vec![]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate, with access to
    /// [`TransformContext`].
    pub fn filter_ctx<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T, &TransformContext) -> bool + Send + Sync + 'static,
    {
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                if f(&typed, ctx) {
                    vec![AnyItem::new(typed)]
                } else {
                    vec![]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                f(typed).into_iter().map(AnyItem::new).collect()
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                f(typed, ctx).into_iter().map(AnyItem::new).collect()
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyKeyByNode(Box::new(move || {
            Arc::new(move |item: &AnyItem| {
                let typed = item.downcast_ref::<T>();
                key_fn(typed)
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::KeyBy(Box::new(node)), vec![self.node_id]);
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
        K: Sink<Input = T> + Send + 'static,
    {
        self.graph.add_node(
            NodeKind::Sink(Box::new(TypedSinkNode(sink))),
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
            #[allow(clippy::expect_used)] // invariant: T: Serialize, so JSON encoding cannot fail
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
            #[allow(clippy::expect_used)] // invariant: T: Serialize, so JSON encoding cannot fail
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                vec![AnyItem::new(f(typed))]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                vec![AnyItem::new(f(typed, ctx))]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate (preserves partitioning).
    pub fn filter<F>(self, f: F) -> KeyedStream<'a, T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                if f(&typed) {
                    vec![AnyItem::new(typed)]
                } else {
                    vec![]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Drop elements that don't match the predicate, with access to
    /// [`TransformContext`] (preserves partitioning).
    pub fn filter_ctx<F>(self, f: F) -> KeyedStream<'a, T>
    where
        F: Fn(&T, &TransformContext) -> bool + Send + Sync + 'static,
    {
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                if f(&typed, ctx) {
                    vec![AnyItem::new(typed)]
                } else {
                    vec![]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, _ctx: &TransformContext| {
                let typed: T = item.downcast();
                f(typed).into_iter().map(AnyItem::new).collect()
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
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
        let node = LazyTransformNode(Box::new(move || {
            Arc::new(move |item: AnyItem, ctx: &TransformContext| {
                let typed: T = item.downcast();
                f(typed, ctx).into_iter().map(AnyItem::new).collect()
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(node)), vec![self.node_id]);
        KeyedStream::new(self.graph, node_id)
    }

    /// Add a stateful operator. The operator is cloned per worker, and each
    /// worker gets its own [`StateContext`] created automatically.
    ///
    /// Only available on `KeyedStream` — this is enforced at compile time.
    pub fn operator<Func>(self, name: &str, func: Func) -> KeyedStream<'a, Func::Output>
    where
        Func: StreamFunction<Input = T> + Clone + Send + 'static,
        Func::Output: serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        let node_id = self.graph.add_node(
            NodeKind::Operator {
                name: name.to_string(),
                op: Box::new(TypedOperatorNode(func)),
            },
            vec![self.node_id],
        );
        KeyedStream::new(self.graph, node_id)
    }

    /// Route operator errors to a dead-letter queue sink.
    ///
    /// The simplest way to add DLQ handling: pass any `Sink<Input = String>`
    /// and errors are automatically routed there. The main stream continues
    /// with successful results only.
    ///
    /// ```ignore
    /// stream
    ///     .key_by(|x| x.key.clone())
    ///     .operator("process", MyProcessor)
    ///     .dlq(PrintSink::new().with_prefix("DLQ"))
    ///     .sink(output_sink);
    /// ```
    ///
    /// For more control over the error stream (e.g. transforms before
    /// sinking), use [`with_dlq()`](Self::with_dlq) instead.
    ///
    /// # Panics
    ///
    /// Panics if the preceding node is not an `Operator`.
    pub fn dlq<K>(self, sink: K) -> KeyedStream<'a, T>
    where
        K: Sink<Input = String> + Send + 'static,
    {
        self.with_dlq(|errors| errors.sink(sink))
    }

    /// Attach a dead-letter queue to the preceding operator (advanced).
    ///
    /// Rewrites the operator so that `Err(e)` results are routed to an error
    /// stream instead of crashing the pipeline. The closure receives the error
    /// stream (`Stream<String>`) for wiring (e.g. to transform before sinking).
    /// Returns the main `KeyedStream<T>` for continued chaining.
    ///
    /// For the simple case, prefer [`dlq(sink)`](Self::dlq) which takes a
    /// sink directly.
    ///
    /// ```ignore
    /// stream
    ///     .key_by(|x| x.key.clone())
    ///     .operator("process", MyProcessor)
    ///     .with_dlq(|errors| errors.map(enrich_error).sink(dlq_sink))
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
        // Rewrite the operator node to wrap it in DlqOperatorNode.
        {
            let mut nodes = self.graph.nodes.borrow_mut();
            let node = &mut nodes[self.node_id.0];
            // Swap NodeKind out temporarily, wrap operator, swap back.
            let old_kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
            match old_kind {
                NodeKind::Operator { name, op } => {
                    node.kind = NodeKind::Operator {
                        name,
                        op: Box::new(DlqOperatorNode(op)),
                    };
                }
                _ => panic!("with_dlq called on non-operator node"),
            }
        }

        // The operator now emits DlqTag items. Split into main/error streams.
        let main_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let tag: DlqTag<AnyItem> = item.downcast();
                match tag {
                    DlqTag::Main(inner) => vec![inner],
                    DlqTag::Error(_) => vec![],
                }
            })
        }));
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(main_node)), vec![self.node_id]);

        let side_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let tag: DlqTag<AnyItem> = item.downcast();
                match tag {
                    DlqTag::Main(_) => vec![],
                    DlqTag::Error(msg) => vec![AnyItem::new(msg)],
                }
            })
        }));
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(side_node)), vec![self.node_id]);

        // Wire the error stream via the user's closure.
        f(Stream::new(self.graph, side_id));

        KeyedStream::new(self.graph, main_id)
    }

    /// Re-partition by a new key (triggers a new exchange).
    pub fn key_by<KF>(self, key_fn: KF) -> KeyedStream<'a, T>
    where
        KF: Fn(&T) -> String + Send + Sync + 'static,
    {
        let node = LazyKeyByNode(Box::new(move || {
            Arc::new(move |item: &AnyItem| {
                let typed = item.downcast_ref::<T>();
                key_fn(typed)
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::KeyBy(Box::new(node)), vec![self.node_id]);
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
        K: Sink<Input = T> + Send + 'static,
    {
        self.graph.add_node(
            NodeKind::Sink(Box::new(TypedSinkNode(sink))),
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
            #[allow(clippy::expect_used)] // invariant: T: Serialize, so JSON encoding cannot fail
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
            #[allow(clippy::expect_used)] // invariant: T: Serialize, so JSON encoding cannot fail
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
        let main_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let ws: WithSide<M, S> = item.downcast();
                match ws {
                    WithSide::Main(m) => vec![AnyItem::new(m)],
                    WithSide::Side(_) => vec![],
                }
            })
        }));
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(main_node)), vec![self.node_id]);

        let side_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let ws: WithSide<M, S> = item.downcast();
                match ws {
                    WithSide::Main(_) => vec![],
                    WithSide::Side(s) => vec![AnyItem::new(s)],
                }
            })
        }));
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(side_node)), vec![self.node_id]);

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
        let main_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let ws: WithSide<M, S> = item.downcast();
                match ws {
                    WithSide::Main(m) => vec![AnyItem::new(m)],
                    WithSide::Side(_) => vec![],
                }
            })
        }));
        let main_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(main_node)), vec![self.node_id]);

        let side_node = LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let ws: WithSide<M, S> = item.downcast();
                match ws {
                    WithSide::Main(_) => vec![],
                    WithSide::Side(s) => vec![AnyItem::new(s)],
                }
            })
        }));
        let side_id = self
            .graph
            .add_node(NodeKind::Transform(Box::new(side_node)), vec![self.node_id]);

        (
            KeyedStream::new(self.graph, main_id),
            Stream::new(self.graph, side_id),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::erased::SourceWrapper;
    use async_trait::async_trait;
    use rhei_core::state::context::StateContext;

    // ── Validation tests ────────────────────────────────────────────

    #[test]
    fn validate_empty_graph() {
        let graph = DataflowGraph::new();
        let err = graph.validate().unwrap_err();
        assert!(matches!(err, ValidationError::EmptyGraph));
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn validate_no_sinks() {
        let graph = DataflowGraph::new();
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1i32, 2, 3]);
        graph.source(source).map(|x: i32| x * 2);
        // No .sink() call -- the NoSinks check fires before DanglingStreams
        let err = graph.validate().unwrap_err();
        assert!(matches!(err, ValidationError::NoSinks));
        assert!(
            err.to_string().contains("no sinks"),
            "error should mention no sinks: {err}"
        );
    }

    #[test]
    fn validate_valid_graph() {
        let graph = DataflowGraph::new();
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1i32, 2, 3]);
        graph
            .source(source)
            .map(|x: i32| x * 2)
            .sink(rhei_core::connectors::print_sink::PrintSink::<i32>::new());
        assert!(graph.validate().is_ok());
    }

    #[test]
    fn validate_fan_out_one_dangling() {
        let graph = DataflowGraph::new();
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1i32, 2, 3]);
        let stream = graph.source(source);
        let mapped = stream.map(|x: i32| x * 2);
        // One branch has a sink, the other dangles
        mapped.sink(rhei_core::connectors::print_sink::PrintSink::<i32>::new());
        stream.map(|x: i32| x + 1); // dangling!

        let err = graph.validate().unwrap_err();
        match err {
            ValidationError::DanglingStreams(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].1, "transform");
            }
            other => panic!("expected DanglingStreams, got: {other}"),
        }
    }

    #[test]
    fn validate_fan_out_both_sunk() {
        let graph = DataflowGraph::new();
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1i32, 2, 3]);
        let stream = graph.source(source);
        stream
            .map(|x: i32| x * 2)
            .sink(rhei_core::connectors::print_sink::PrintSink::<i32>::new());
        stream
            .map(|x: i32| x + 1)
            .sink(rhei_core::connectors::print_sink::PrintSink::<i32>::new());
        assert!(graph.validate().is_ok());
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

    #[test]
    fn typed_source_node_compiles_to_erased_source() {
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![1u16, 2, 3]);
        let node: Box<dyn SourceNode> = Box::new(TypedSourceNode(source));
        let erased = node.compile();
        // register_output_type should work on the compiled source.
        erased.register_output_type();
        let item = AnyItem::new(99u16);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<u16>(), 99);
    }

    #[test]
    fn lazy_transform_node_compiles_and_executes() {
        let node: Box<dyn TransformNode> = Box::new(LazyTransformNode(Box::new(|| {
            Arc::new(|item: AnyItem, _ctx: &TransformContext| {
                let val: i32 = item.downcast();
                vec![AnyItem::new(val * 2)]
            })
        })));
        let transform = node.compile();
        let ctx = TransformContext {
            worker_index: 0,
            num_workers: 1,
        };
        let result = transform(AnyItem::new(5i32), &ctx);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].clone().downcast::<i32>(), 10);
    }

    #[test]
    fn lazy_key_by_node_compiles_and_extracts() {
        let node: Box<dyn KeyByNode> = Box::new(LazyKeyByNode(Box::new(|| {
            Arc::new(|item: &AnyItem| {
                let val = item.downcast_ref::<i32>();
                format!("key-{val}")
            })
        })));
        let key_fn = node.compile();
        let item = AnyItem::new(42i32);
        assert_eq!(key_fn(&item), "key-42");
    }

    #[test]
    fn dlq_operator_node_wraps_inner() {
        // Verify DlqOperatorNode compiles by wrapping a typed operator.
        // Use a simple identity-like StreamFunction.
        #[derive(Clone)]
        struct IdentityOp;

        #[async_trait]
        impl StreamFunction for IdentityOp {
            type Input = i32;
            type Output = i32;

            async fn process(
                &mut self,
                input: i32,
                _ctx: &mut StateContext,
            ) -> anyhow::Result<Vec<i32>> {
                Ok(vec![input])
            }
        }

        let inner: Box<dyn OperatorNode> = Box::new(TypedOperatorNode(IdentityOp));
        let dlq_node: Box<dyn OperatorNode> = Box::new(DlqOperatorNode(inner));
        // Compile should succeed — produces a DlqErasedOperator wrapping OperatorWrapper.
        let _erased = dlq_node.compile();
    }
}
