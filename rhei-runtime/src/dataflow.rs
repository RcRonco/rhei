//! Dataflow graph API for building batch (Arrow columnar) stream processing pipelines.
//!
//! Provides [`DataflowGraph`] as a standalone builder and [`Stream<T>`]
//! as a lightweight, copyable handle into it. Operations like `.map()`,
//! `.filter()`, `.flat_map()`, and `.operator()` add nodes to the graph.
//! Pass the finished graph to
//! [`Executor::run()`](crate::executor::Executor::run) for execution.
//!
//! ```ignore
//! let graph = DataflowGraph::new();
//! let orders = graph.source(kafka_source);
//! orders
//!     .map(parse)
//!     .filter_fn(|o| o.is_valid())
//!     .operator("enrich", EnrichOp)
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

use rhei_core::arrow::{RheiSchema, Sink, Source, StreamFunction};

use crate::erased_batch::{
    BatchOperatorWrapper, ErasedBatchOperator, ErasedSink, ErasedSource, SinkWrapper, SourceWrapper,
};
use crate::erased_buffer::{BatchKeyFn, ErasedBuffer};

// ── Node identity ────────────────────────────────────────────────────

/// Opaque identifier for a node in the dataflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(pub(crate) usize);

// ── Batch (Arrow) compile traits ────────────────────────────────────

/// A batch source node that produces `ErasedBuffer` batches.
pub(crate) trait SourceNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSource>;
}

/// A batch operator node. Compiles to [`ErasedBatchOperator`].
pub(crate) trait BatchOperatorNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedBatchOperator>;
}

/// A batch sink node. Compiles to [`ErasedSink`].
pub(crate) trait SinkNode: Send {
    fn compile(self: Box<Self>) -> Box<dyn ErasedSink>;
}

/// A batch-level transform: `ErasedBuffer` → `Vec<ErasedBuffer>`.
pub(crate) type BatchTransformFn = Arc<dyn Fn(ErasedBuffer) -> Vec<ErasedBuffer> + Send + Sync>;

// ── Typed batch node wrappers ───────────────────────────────────────

/// Wraps a typed [`Source`] for deferred compilation.
pub(crate) struct TypedSourceNode<S: Source>(pub(crate) S);

impl<S> SourceNode for TypedSourceNode<S>
where
    S: Source + 'static,
    S::Output: Sync,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedSource> {
        Box::new(SourceWrapper(self.0))
    }
}

/// Wraps a typed [`StreamFunction`] for deferred compilation.
pub(crate) struct TypedBatchOperatorNode<F: StreamFunction>(pub(crate) F);

impl<F> BatchOperatorNode for TypedBatchOperatorNode<F>
where
    F: StreamFunction + Clone + 'static,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedBatchOperator> {
        Box::new(BatchOperatorWrapper(self.0))
    }
}

/// Wraps a typed [`Sink`] for deferred compilation.
pub(crate) struct TypedSinkNode<K: Sink>(pub(crate) K);

impl<K> SinkNode for TypedSinkNode<K>
where
    K: Sink + 'static,
{
    fn compile(self: Box<Self>) -> Box<dyn ErasedSink> {
        Box::new(SinkWrapper(self.0))
    }
}

/// Deferred batch transform: stores a factory that produces the erased closure.
pub(crate) struct LazyBatchTransformNode(pub(crate) Box<dyn FnOnce() -> BatchTransformFn + Send>);

impl LazyBatchTransformNode {
    pub fn compile(self) -> BatchTransformFn {
        (self.0)()
    }
}

/// Deferred batch `key_by` node: stores a factory that produces the erased key function.
pub(crate) struct LazyBatchKeyByNode(pub(crate) Box<dyn FnOnce() -> BatchKeyFn + Send>);

impl LazyBatchKeyByNode {
    pub fn compile(self) -> BatchKeyFn {
        (self.0)()
    }
}

// ── Graph nodes ──────────────────────────────────────────────────────

/// The kind of processing a graph node performs.
///
/// Each variant stores a graph-level compile trait rather than an
/// erased execution form. Call the corresponding `compile()` method
/// to produce the execution-layer form needed by the Timely executor.
#[allow(clippy::enum_variant_names)] // all variants are batch-only after row API removal
pub(crate) enum NodeKind {
    /// A batch data source producing `ErasedBuffer` batches.
    Source(Box<dyn SourceNode>),
    /// A batch stateless transform (`ErasedBuffer` → `Vec<ErasedBuffer>`).
    BatchTransform(LazyBatchTransformNode),
    /// A batch stateful operator.
    BatchOperator {
        /// Human-readable operator name (used for `OperatorContext` namespacing).
        name: String,
        /// The typed batch operator node.
        op: Box<dyn BatchOperatorNode>,
    },
    /// A batch data sink consuming `ErasedBuffer` batches.
    Sink(Box<dyn SinkNode>),
    /// Key-based exchange: partitions rows by key hash and routes to workers.
    BatchKeyBy(LazyBatchKeyByNode),
    /// Merges multiple streams into one (Timely Concatenate).
    BatchMerge,
}

/// A node in the dataflow graph.
pub(crate) struct GraphNode {
    pub id: NodeId,
    pub kind: NodeKind,
    /// Input node IDs (0 for Source, 1 for Transform/Operator/Sink).
    pub inputs: Vec<NodeId>,
    /// Optional human-readable label for debugging and observability.
    pub label: Option<String>,
}

/// The dataflow graph: a collection of nodes connected by edges.
///
/// Build a graph by calling [`source()`](Self::source) to create
/// entry points, then chaining transforms and sinks on the returned
/// [`Stream`] handles. Pass the finished graph to
/// [`Executor::run()`](crate::executor::Executor::run) for execution.
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

    /// Add a batch (Arrow columnar) data source. Returns a [`Stream`] handle.
    pub fn source<S>(&self, source: S) -> Stream<'_, S::Output>
    where
        S: Source + 'static,
        S::Output: Sync,
    {
        let id = self.add_node(NodeKind::Source(Box::new(TypedSourceNode(source))), vec![]);
        Stream::new(self, id)
    }

    /// Add a node and return its ID.
    pub(crate) fn add_node(&self, kind: NodeKind, inputs: Vec<NodeId>) -> NodeId {
        let mut nodes = self.nodes.borrow_mut();
        let id = NodeId(nodes.len());
        nodes.push(GraphNode {
            id,
            kind,
            inputs,
            label: None,
        });
        id
    }

    /// Set a human-readable label on the most recently added node.
    pub(crate) fn set_label(&self, node_id: NodeId, label: String) {
        let mut nodes = self.nodes.borrow_mut();
        nodes[node_id.0].label = Some(label);
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
                    NodeKind::BatchTransform(_) => "transform".to_string(),
                    NodeKind::BatchOperator { name, .. } => {
                        format!("operator \"{name}\"")
                    }
                    NodeKind::BatchKeyBy(_) => "key_by".to_string(),
                    NodeKind::BatchMerge => "merge".to_string(),
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

// ── Stream handle ──────────────────────────────────────────────

/// A lightweight handle representing a point in a batch (Arrow columnar) dataflow.
///
/// `T` is the `RheiSchema` type flowing through this point. Operations add
/// batch nodes to the graph and return new handles.
pub struct Stream<'a, T: RheiSchema> {
    graph: &'a DataflowGraph,
    node_id: NodeId,
    _phantom: PhantomData<T>,
}

impl<T: RheiSchema> std::fmt::Debug for Stream<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl<T: RheiSchema> Clone for Stream<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: RheiSchema> Copy for Stream<'_, T> {}

impl<'a, T: RheiSchema + 'static> Stream<'a, T> {
    pub(crate) fn new(graph: &'a DataflowGraph, node_id: NodeId) -> Self {
        Self {
            graph,
            node_id,
            _phantom: PhantomData,
        }
    }

    /// Per-row map transform. Iterates views, builds output buffer.
    pub fn map<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T::View<'_>) -> O + Send + Sync + 'static,
        O: RheiSchema + 'static,
    {
        use rhei_core::arrow::RheiBuilder;

        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch map downcast failed: {e}");
                        return vec![];
                    }
                };
                if typed.is_empty() {
                    return vec![];
                }
                let mut builder = O::builder(typed.len());
                for view in &typed {
                    builder.append(f(view));
                }
                let buf: rhei_core::arrow::RheiBuffer<O> =
                    rhei_core::arrow::RheiBuffer::from_builder(builder);
                vec![ErasedBuffer::from_typed(buf)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Closure-based zero-copy filter. Builds a selection mask from the predicate.
    pub fn filter_fn<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T::View<'_>) -> bool + Send + Sync + 'static,
    {
        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch filter_fn downcast failed: {e}");
                        return vec![];
                    }
                };
                if typed.is_empty() {
                    return vec![];
                }
                let mut mask_vec = vec![false; typed.physical_len()];
                let mut any_true = false;
                for (phys_idx, view) in typed.iter().enumerate_physical() {
                    if f(&view) {
                        mask_vec[phys_idx] = true;
                        any_true = true;
                    }
                }
                if !any_true {
                    return vec![];
                }
                let mask = arrow_array::BooleanArray::from(mask_vec);
                let filtered = typed.and_mask(mask);
                vec![ErasedBuffer::from_typed(filtered)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Expression-based zero-copy filter using Arrow compute kernels.
    pub fn filter(self, expr: rhei_core::operators::Expr) -> Stream<'a, T> {
        let node = LazyBatchTransformNode(Box::new(move || {
            let expr = expr.clone();
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch filter downcast failed: {e}");
                        return vec![];
                    }
                };
                if typed.is_empty() {
                    return vec![];
                }
                let batch = typed.as_record_batch();
                let mask = match rhei_core::operators::filter_expr::eval_predicate(&expr, batch) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!("batch filter eval failed: {e}");
                        return vec![];
                    }
                };
                let filtered = typed.and_mask(mask);
                if filtered.is_empty() {
                    return vec![];
                }
                vec![ErasedBuffer::from_typed(filtered)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Per-row flat-map transform. Each view produces zero or more output rows.
    pub fn flat_map<F, O>(self, f: F) -> Stream<'a, O>
    where
        F: Fn(T::View<'_>) -> Vec<O> + Send + Sync + 'static,
        O: RheiSchema + 'static,
    {
        use rhei_core::arrow::RheiBuilder;

        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch flat_map downcast failed: {e}");
                        return vec![];
                    }
                };
                if typed.is_empty() {
                    return vec![];
                }
                let mut builder = O::builder(typed.len());
                for view in &typed {
                    for item in f(view) {
                        builder.append(item);
                    }
                }
                if builder.len() == 0 {
                    return vec![];
                }
                let buf: rhei_core::arrow::RheiBuffer<O> =
                    rhei_core::arrow::RheiBuffer::from_builder(builder);
                vec![ErasedBuffer::from_typed(buf)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Add a stateful batch operator.
    pub fn operator<F>(self, name: &str, func: F) -> Stream<'a, F::Output>
    where
        F: StreamFunction<Input = T> + Clone + Send + 'static,
    {
        let node_id = self.graph.add_node(
            NodeKind::BatchOperator {
                name: name.to_string(),
                op: Box::new(TypedBatchOperatorNode(func)),
            },
            vec![self.node_id],
        );
        Stream::new(self.graph, node_id)
    }

    /// Key-based exchange: partitions rows by key hash and routes them
    /// so that all rows with the same key land on the same worker.
    pub fn key_by<F>(self, key_fn: F) -> Stream<'a, T>
    where
        F: for<'v> Fn(&T::View<'v>) -> String + Send + Sync + 'static,
    {
        let node = LazyBatchKeyByNode(Box::new(move || {
            Arc::new(move |batch: &arrow_array::RecordBatch, row_idx: usize| {
                let view = T::view(batch, row_idx);
                key_fn(&view)
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchKeyBy(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Merge two streams of the same type into one.
    pub fn merge(self, other: Stream<'a, T>) -> Stream<'a, T> {
        let node_id = self
            .graph
            .add_node(NodeKind::BatchMerge, vec![self.node_id, other.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Assign a human-readable name to this stream point for debugging.
    pub fn name(self, label: &str) -> Stream<'a, T> {
        self.graph.set_label(self.node_id, label.to_string());
        self
    }

    /// Side-effect inspection: calls the closure for each row without modifying the stream.
    pub fn inspect<F>(self, f: F) -> Stream<'a, T>
    where
        F: Fn(&T::View<'_>) + Send + Sync + 'static,
    {
        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch inspect downcast failed: {e}");
                        return vec![];
                    }
                };
                for view in &typed {
                    f(&view);
                }
                vec![ErasedBuffer::from_typed(typed)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Limit the stream to at most `max` rows total, then stop producing output.
    pub fn limit(self, max: usize) -> Stream<'a, T> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let remaining = Arc::new(AtomicUsize::new(max));

        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let left = remaining.load(Ordering::Relaxed);
                if left == 0 {
                    return vec![];
                }
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch limit downcast failed: {e}");
                        return vec![];
                    }
                };
                let len = typed.len();
                if len <= left {
                    remaining.fetch_sub(len, Ordering::Relaxed);
                    vec![ErasedBuffer::from_typed(typed)]
                } else {
                    remaining.store(0, Ordering::Relaxed);
                    let mut mask_vec = vec![false; typed.physical_len()];
                    for (i, (phys_idx, _view)) in typed.iter().enumerate_physical().enumerate() {
                        if i >= left {
                            break;
                        }
                        mask_vec[phys_idx] = true;
                    }
                    let mask = arrow_array::BooleanArray::from(mask_vec);
                    let filtered = typed.and_mask(mask);
                    vec![ErasedBuffer::from_typed(filtered)]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Re-batch the stream: accumulate rows and emit in chunks of `size`.
    pub fn batch(self, size: usize) -> Stream<'a, T> {
        use std::sync::Mutex;

        let pending: Arc<Mutex<Vec<ErasedBuffer>>> = Arc::new(Mutex::new(Vec::new()));
        let pending_rows: Arc<std::sync::atomic::AtomicUsize> =
            Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let rows = buf.num_rows();
                let mut guard = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.push(buf);
                let total =
                    pending_rows.fetch_add(rows, std::sync::atomic::Ordering::Relaxed) + rows;
                if total >= size {
                    let batches: Vec<ErasedBuffer> = guard.drain(..).collect();
                    pending_rows.store(0, std::sync::atomic::Ordering::Relaxed);
                    let merged = ErasedBuffer::concat(batches);
                    match merged {
                        Some(m) => vec![m],
                        None => vec![],
                    }
                } else {
                    vec![]
                }
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Deduplicate consecutive rows with the same key within each batch.
    pub fn distinct_by<F>(self, key_fn: F) -> Stream<'a, T>
    where
        F: Fn(&T::View<'_>) -> String + Send + Sync + 'static,
    {
        use std::collections::HashSet;
        use std::sync::Mutex;

        let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let node = LazyBatchTransformNode(Box::new(move || {
            Arc::new(move |buf: ErasedBuffer| {
                let typed = match buf.downcast::<T>() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("batch distinct_by downcast failed: {e}");
                        return vec![];
                    }
                };
                if typed.is_empty() {
                    return vec![];
                }
                let mut guard = seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut mask_vec = vec![false; typed.physical_len()];
                let mut any_true = false;
                for (phys_idx, view) in typed.iter().enumerate_physical() {
                    let k = key_fn(&view);
                    if guard.insert(k) {
                        mask_vec[phys_idx] = true;
                        any_true = true;
                    }
                }
                if !any_true {
                    return vec![];
                }
                let mask = arrow_array::BooleanArray::from(mask_vec);
                let filtered = typed.and_mask(mask);
                vec![ErasedBuffer::from_typed(filtered)]
            })
        }));
        let node_id = self
            .graph
            .add_node(NodeKind::BatchTransform(node), vec![self.node_id]);
        Stream::new(self.graph, node_id)
    }

    /// Terminal: write buffers to a batch sink.
    pub fn sink<K>(self, sink: K)
    where
        K: Sink<Input = T> + 'static,
    {
        self.graph.add_node(
            NodeKind::Sink(Box::new(TypedSinkNode(sink))),
            vec![self.node_id],
        );
    }
}
