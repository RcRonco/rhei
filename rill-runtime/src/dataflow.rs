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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use rill_core::state::context::StateContext;
use rill_core::traits::{Sink, Source, StreamFunction};

use crate::shutdown::ShutdownHandle;

// ── Node identity ────────────────────────────────────────────────────

/// Opaque identifier for a node in the dataflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(usize);

// ── Type-erased traits ───────────────────────────────────────────────

/// Type-erased source: produces batches of `Box<dyn Any + Send>`.
#[async_trait]
pub(crate) trait ErasedSource: Send {
    async fn next_batch(&mut self) -> Option<Vec<Box<dyn Any + Send>>>;
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()>;
}

/// Wraps a typed [`Source`] into an [`ErasedSource`].
struct SourceWrapper<S: Source>(S);

#[async_trait]
impl<S> ErasedSource for SourceWrapper<S>
where
    S: Source + 'static,
    S::Output: 'static,
{
    async fn next_batch(&mut self) -> Option<Vec<Box<dyn Any + Send>>> {
        let batch = self.0.next_batch().await?;
        Some(
            batch
                .into_iter()
                .map(|item| Box::new(item) as Box<dyn Any + Send>)
                .collect(),
        )
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        self.0.on_checkpoint_complete().await
    }
}

/// Type-erased sink: consumes `Box<dyn Any + Send>`.
#[async_trait]
pub(crate) trait ErasedSink: Send {
    async fn write(&mut self, item: Box<dyn Any + Send>) -> anyhow::Result<()>;
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
    async fn write(&mut self, item: Box<dyn Any + Send>) -> anyhow::Result<()> {
        let typed = *item.downcast::<K::Input>().expect("type mismatch in sink");
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
        input: Box<dyn Any + Send>,
        ctx: &mut StateContext,
    ) -> Vec<Box<dyn Any + Send>>;
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
        input: Box<dyn Any + Send>,
        ctx: &mut StateContext,
    ) -> Vec<Box<dyn Any + Send>> {
        let typed = *input
            .downcast::<F::Input>()
            .expect("type mismatch in operator");
        let results = self.0.process(typed, ctx).await;
        results
            .into_iter()
            .map(|r| Box::new(r) as Box<dyn Any + Send>)
            .collect()
    }

    fn clone_erased(&self) -> Box<dyn ErasedOperator> {
        Box::new(OperatorWrapper(self.0.clone()))
    }
}

// ── Type-erased function types ───────────────────────────────────────

/// Stateless transform: one item in, zero or more items out.
/// `Arc` for sharing across workers without cloning the closure.
pub(crate) type TransformFn =
    Arc<dyn Fn(Box<dyn Any + Send>) -> Vec<Box<dyn Any + Send>> + Send + Sync>;

/// Key extraction function.
pub(crate) type KeyFn = Arc<dyn Fn(&dyn Any) -> String + Send + Sync>;

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
        S::Output: Send + 'static,
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

impl<'a, T: Send + 'static> Stream<'a, T> {
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
        O: Send + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in map");
            vec![Box::new(f(typed)) as Box<dyn Any + Send>]
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
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in filter");
            if f(&typed) {
                vec![Box::new(typed) as Box<dyn Any + Send>]
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
        O: Send + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in flat_map");
            f(typed)
                .into_iter()
                .map(|o| Box::new(o) as Box<dyn Any + Send>)
                .collect()
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
        let erased_key_fn: KeyFn = Arc::new(move |item: &dyn Any| {
            let typed = item.downcast_ref::<T>().expect("type mismatch in key_by");
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

impl<'a, T: Send + 'static> KeyedStream<'a, T> {
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
        O: Send + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in map");
            vec![Box::new(f(typed)) as Box<dyn Any + Send>]
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
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in filter");
            if f(&typed) {
                vec![Box::new(typed) as Box<dyn Any + Send>]
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
        O: Send + 'static,
    {
        let transform: TransformFn = Arc::new(move |item: Box<dyn Any + Send>| {
            let typed = *item.downcast::<T>().expect("type mismatch in flat_map");
            f(typed)
                .into_iter()
                .map(|o| Box::new(o) as Box<dyn Any + Send>)
                .collect()
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
        let erased_key_fn: KeyFn = Arc::new(move |item: &dyn Any| {
            let typed = item.downcast_ref::<T>().expect("type mismatch in key_by");
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

// ── Compiled pipeline ────────────────────────────────────────────────

/// A segment in the compiled pipeline.
enum Segment {
    Transform(TransformFn),
    Exchange(KeyFn),
    Operator {
        name: String,
        op: Box<dyn ErasedOperator>,
    },
}

/// A compiled linear pipeline from source to sink.
pub(crate) struct CompiledPipeline {
    source: Box<dyn ErasedSource>,
    segments: Vec<Segment>,
    sink: Box<dyn ErasedSink>,
}

// ── Graph compilation ────────────────────────────────────────────────

/// Compile the dataflow graph into executable pipelines.
///
/// For V1, supports linear topologies: source → [transforms] → sink.
/// Fan-out and merge are detected and return an error.
pub(crate) fn compile(mut nodes: Vec<GraphNode>) -> anyhow::Result<Vec<CompiledPipeline>> {
    let sink_ids: Vec<NodeId> = nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Sink(_)))
        .map(|n| n.id)
        .collect();

    if sink_ids.is_empty() {
        anyhow::bail!("dataflow has no sinks — every stream must reach a sink");
    }

    let mut pipelines = Vec::new();

    for sink_id in &sink_ids {
        // Walk backwards from sink to source.
        let mut path = Vec::new();
        let mut current = *sink_id;
        loop {
            path.push(current);
            let node = &nodes[current.0];
            if node.inputs.is_empty() {
                break;
            }
            if node.inputs.len() > 1 {
                anyhow::bail!("merge nodes are not yet supported in the execution engine");
            }
            current = node.inputs[0];
        }
        path.reverse();

        if path.len() < 2 {
            anyhow::bail!("degenerate pipeline: fewer than 2 nodes");
        }
        if !matches!(nodes[path[0].0].kind, NodeKind::Source(_)) {
            anyhow::bail!(
                "pipeline does not start with a source (found node {:?})",
                path[0]
            );
        }

        // Extract middle segments.
        let mut segments = Vec::new();
        for &node_id in &path[1..path.len() - 1] {
            let kind = std::mem::replace(&mut nodes[node_id.0].kind, NodeKind::Merge);
            match kind {
                NodeKind::Transform(f) => segments.push(Segment::Transform(f)),
                NodeKind::KeyBy(f) => segments.push(Segment::Exchange(f)),
                NodeKind::Operator { name, op } => {
                    segments.push(Segment::Operator { name, op });
                }
                NodeKind::Source(_) => anyhow::bail!("unexpected source in middle of pipeline"),
                NodeKind::Sink(_) => anyhow::bail!("unexpected sink in middle of pipeline"),
                NodeKind::Merge => {
                    anyhow::bail!("merge nodes are not yet supported in the execution engine")
                }
            }
        }

        // Extract source and sink.
        let source_kind = std::mem::replace(&mut nodes[path[0].0].kind, NodeKind::Merge);
        let sink_kind = std::mem::replace(&mut nodes[path[path.len() - 1].0].kind, NodeKind::Merge);
        let NodeKind::Source(source) = source_kind else {
            anyhow::bail!("expected source node");
        };
        let NodeKind::Sink(sink) = sink_kind else {
            anyhow::bail!("expected sink node");
        };

        pipelines.push(CompiledPipeline {
            source,
            segments,
            sink,
        });
    }

    Ok(pipelines)
}

// ── Execution engine ─────────────────────────────────────────────────

/// Materialized step for execution (operators have their `StateContext`).
enum ExecStep {
    Transform(TransformFn),
    Operator {
        #[allow(dead_code)]
        name: String,
        op: Box<dyn ErasedOperator>,
        ctx: StateContext,
    },
}

impl ExecStep {
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        if let ExecStep::Operator { ctx, .. } = self {
            ctx.checkpoint().await?;
        }
        Ok(())
    }
}

/// Apply a chain of steps to a single item, returning all outputs.
async fn apply_steps(
    item: Box<dyn Any + Send>,
    steps: &mut [ExecStep],
) -> Vec<Box<dyn Any + Send>> {
    let mut items = vec![item];
    for step in steps.iter_mut() {
        let mut next = Vec::new();
        for item in items {
            match step {
                ExecStep::Transform(f) => next.extend(f(item)),
                ExecStep::Operator { op, ctx, .. } => {
                    next.extend(op.process(item, ctx).await);
                }
            }
        }
        items = next;
    }
    items
}

/// Materialize segments into executable steps, creating `StateContext` for operators.
async fn materialize_steps(
    segments: Vec<Segment>,
    executor: &crate::executor::Executor,
    worker_index: usize,
) -> anyhow::Result<Vec<ExecStep>> {
    let mut steps = Vec::new();
    for segment in segments {
        match segment {
            Segment::Transform(f) => steps.push(ExecStep::Transform(f)),
            Segment::Operator { name, op } => {
                let ctx = executor
                    .create_context_for_worker(&name, worker_index)
                    .await?;
                steps.push(ExecStep::Operator { name, op, ctx });
            }
            Segment::Exchange(_) => {
                // Handled by the caller at the section boundary level.
            }
        }
    }
    Ok(steps)
}

/// Clone segments (Arc transforms are cheap, operators use `clone_erased`).
fn clone_segments(segments: &[Segment]) -> Vec<Segment> {
    segments
        .iter()
        .map(|seg| match seg {
            Segment::Transform(f) => Segment::Transform(Arc::clone(f)),
            Segment::Exchange(f) => Segment::Exchange(Arc::clone(f)),
            Segment::Operator { name, op } => Segment::Operator {
                name: name.clone(),
                op: op.clone_erased(),
            },
        })
        .collect()
}

/// Split a flat segment list at the first Exchange boundary.
///
/// Returns `(pre_segments, Option<(key_fn, post_segments)>)`.
fn split_at_first_exchange(
    segments: Vec<Segment>,
) -> (Vec<Segment>, Option<(KeyFn, Vec<Segment>)>) {
    let mut pre = Vec::new();
    let mut iter = segments.into_iter();
    for seg in iter.by_ref() {
        if let Segment::Exchange(key_fn) = seg {
            let post: Vec<Segment> = iter.collect();
            return (pre, Some((key_fn, post)));
        }
        pre.push(seg);
    }
    (pre, None)
}

/// Execute a compiled pipeline.
pub(crate) async fn execute_pipeline(
    pipeline: CompiledPipeline,
    executor: &crate::executor::Executor,
    shutdown: Option<&ShutdownHandle>,
) -> anyhow::Result<()> {
    let n_workers = executor.workers();
    let (pre_segments, exchange) = split_at_first_exchange(pipeline.segments);

    match exchange {
        Some((key_fn, post_segments)) if n_workers > 1 => {
            execute_multi_worker(
                pipeline.source,
                pre_segments,
                key_fn,
                post_segments,
                pipeline.sink,
                executor,
                n_workers,
                shutdown,
            )
            .await
        }
        _ => {
            // Single-worker: flatten all segments and run in one async loop.
            let mut all_segments = pre_segments;
            if let Some((_key_fn, post)) = exchange {
                all_segments.extend(post);
            }
            execute_single_worker(
                pipeline.source,
                all_segments,
                pipeline.sink,
                executor,
                shutdown,
            )
            .await
        }
    }
}

/// Single-worker execution: simple async loop.
async fn execute_single_worker(
    mut source: Box<dyn ErasedSource>,
    segments: Vec<Segment>,
    mut sink: Box<dyn ErasedSink>,
    executor: &crate::executor::Executor,
    shutdown: Option<&ShutdownHandle>,
) -> anyhow::Result<()> {
    let mut steps = materialize_steps(segments, executor, 0).await?;

    let checkpoint_interval: u64 = 100;
    let mut batches_since_checkpoint: u64 = 0;

    while let Some(batch) = source.next_batch().await {
        for item in batch {
            let outputs = apply_steps(item, &mut steps).await;
            for output in outputs {
                sink.write(output).await?;
            }
        }

        batches_since_checkpoint += 1;
        if batches_since_checkpoint >= checkpoint_interval {
            for step in &mut steps {
                step.checkpoint().await?;
            }
            source.on_checkpoint_complete().await?;
            sink.flush().await?;
            batches_since_checkpoint = 0;
        }

        if let Some(handle) = shutdown
            && handle.is_shutdown()
        {
            tracing::info!("shutdown requested, performing final checkpoint...");
            break;
        }
    }

    for step in &mut steps {
        step.checkpoint().await?;
    }
    source.on_checkpoint_complete().await?;
    sink.flush().await?;
    Ok(())
}

/// Multi-worker execution: pre-exchange on main task, post-exchange per worker.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn execute_multi_worker(
    mut source: Box<dyn ErasedSource>,
    pre_segments: Vec<Segment>,
    key_fn: KeyFn,
    post_segments: Vec<Segment>,
    sink: Box<dyn ErasedSink>,
    executor: &crate::executor::Executor,
    n_workers: usize,
    shutdown: Option<&ShutdownHandle>,
) -> anyhow::Result<()> {
    let channel_capacity = 256;

    // Materialize pre-exchange steps on the main task.
    let mut pre_steps = materialize_steps(pre_segments, executor, 0).await?;

    // Per-worker input channels.
    let mut worker_txs = Vec::with_capacity(n_workers);
    let mut worker_receivers = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let (tx, rx) = tokio::sync::mpsc::channel::<Box<dyn Any + Send>>(channel_capacity);
        worker_txs.push(tx);
        worker_receivers.push(rx);
    }

    // Output channel: workers → collector → sink.
    let (output_tx, mut output_rx) =
        tokio::sync::mpsc::channel::<Box<dyn Any + Send>>(channel_capacity);

    // Spawn worker tasks.
    let mut worker_handles = Vec::with_capacity(n_workers);
    for i in 0..n_workers {
        let cloned = clone_segments(&post_segments);
        let mut steps = materialize_steps(cloned, executor, i).await?;
        let mut rx = worker_receivers.pop().unwrap();
        let out_tx = output_tx.clone();

        let handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                let outputs = apply_steps(item, &mut steps).await;
                for output in outputs {
                    if out_tx.send(output).await.is_err() {
                        return Ok(());
                    }
                }
            }
            // Input channel closed → final checkpoint for this worker.
            for step in &mut steps {
                step.checkpoint().await?;
            }
            Ok(())
        });
        worker_handles.push(handle);
    }
    worker_handles.reverse();
    drop(output_tx); // Workers hold the remaining clones.

    // Collector task: output channel → sink.
    let collector_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let mut sink = sink;
        while let Some(item) = output_rx.recv().await {
            sink.write(item).await?;
        }
        sink.flush().await?;
        Ok(())
    });

    // Main loop: source → pre-steps → hash key → route to worker.
    let checkpoint_interval: u64 = 100;
    let mut batches_since_checkpoint: u64 = 0;

    while let Some(batch) = source.next_batch().await {
        for item in batch {
            let mid_items = apply_steps(item, &mut pre_steps).await;
            for mid in mid_items {
                let key = key_fn(mid.as_ref());
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                #[allow(clippy::cast_possible_truncation)]
                let worker_idx = (hasher.finish() as usize) % n_workers;
                if worker_txs[worker_idx].send(mid).await.is_err() {
                    break;
                }
            }
        }

        batches_since_checkpoint += 1;
        if batches_since_checkpoint >= checkpoint_interval {
            for step in &mut pre_steps {
                step.checkpoint().await?;
            }
            source.on_checkpoint_complete().await?;
            batches_since_checkpoint = 0;
        }

        if let Some(handle) = shutdown
            && handle.is_shutdown()
        {
            tracing::info!("shutdown requested, draining workers...");
            break;
        }
    }

    // Final pre-step checkpoint.
    for step in &mut pre_steps {
        step.checkpoint().await?;
    }

    // Close worker channels → workers drain and checkpoint.
    drop(worker_txs);

    for handle in worker_handles {
        handle
            .await
            .map_err(|e| anyhow::anyhow!("worker panicked: {e}"))??;
    }

    source.on_checkpoint_complete().await?;

    collector_handle
        .await
        .map_err(|e| anyhow::anyhow!("collector panicked: {e}"))??;

    Ok(())
}

// ── Public execution entry point ─────────────────────────────────────

/// Compile and execute the dataflow graph.
pub(crate) async fn run_graph(
    graph: DataflowGraph,
    executor: &crate::executor::Executor,
    shutdown: Option<ShutdownHandle>,
) -> anyhow::Result<()> {
    let pipelines = compile(graph.into_nodes())?;

    if pipelines.len() == 1 {
        let pipeline = pipelines.into_iter().next().unwrap();
        execute_pipeline(pipeline, executor, shutdown.as_ref()).await
    } else {
        anyhow::bail!(
            "multiple independent pipelines are not yet supported; found {}",
            pipelines.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rill_core::connectors::vec_source::VecSource;
    use rill_core::state::context::StateContext;
    use rill_core::traits::{Sink, StreamFunction};
    use std::sync::{Arc, Mutex};

    struct CollectSink<T> {
        collected: Arc<Mutex<Vec<T>>>,
    }

    #[async_trait]
    impl<T: Send + Sync + 'static> Sink for CollectSink<T> {
        type Input = T;

        async fn write(&mut self, input: T) -> anyhow::Result<()> {
            self.collected.lock().unwrap().push(input);
            Ok(())
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rill_dataflow_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn map_transform() {
        let dir = temp_dir("map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let doubled = stream.map(|x: i32| x * 2);
        doubled.sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4, 6]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn filter_transform() {
        let dir = temp_dir("filter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.filter(|x: &i32| x % 2 == 0).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn flat_map_transform() {
        let dir = temp_dir("flat_map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "foo bar".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(
            *collected.lock().unwrap(),
            vec!["hello", "world", "foo", "bar"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn chained_filter_map() {
        let dir = temp_dir("chain");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![20, 40]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[derive(Clone)]
    struct WordCounter;

    #[async_trait]
    impl StreamFunction for WordCounter {
        type Input = String;
        type Output = String;

        async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
            let mut outputs = Vec::new();
            for word in input.split_whitespace() {
                let key = word.as_bytes();
                let count = match ctx.get(key).await.unwrap_or(None) {
                    Some(bytes) => {
                        let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        n + 1
                    }
                    None => 1,
                };
                ctx.put(key, &count.to_le_bytes());
                outputs.push(format!("{word}: {count}"));
            }
            outputs
        }
    }

    #[tokio::test]
    async fn keyed_operator_single_worker() {
        let dir = temp_dir("keyed_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 4); // hello, world, hello, rill
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keyed_multi_worker() {
        let dir = temp_dir("keyed_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results.len(), 4);
        // With multi-worker, order may vary but counts must be correct.
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keyed_filter_map_multi_worker() {
        let dir = temp_dir("keyed_filter_map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .filter(|x: &i32| x % 2 == 0)
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![20, 40, 60, 80, 100]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn builder_api() {
        let dir = temp_dir("builder");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![10i32, 20, 30]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.map(|x: i32| x + 1).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = crate::executor::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(1)
            .build();
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![11, 21, 31]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn key_affinity() {
        // Verify that the same key always routes to the same worker by checking
        // that stateful counts are correct across multiple items with the same key.
        let dir = temp_dir("affinity");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();

        // Send "alpha" 5 times and "beta" 3 times.
        let source = VecSource::new(vec![
            "alpha".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = crate::executor::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        executor.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 8);
        // Alpha should count up to 5, beta up to 3.
        assert!(results.contains(&"alpha: 5".to_string()));
        assert!(results.contains(&"beta: 3".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn shutdown_graceful() {
        let dir = temp_dir("shutdown");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = super::DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.map(|x: i32| x * 2).sink(CollectSink {
            collected: collected.clone(),
        });

        let (handle, trigger) = crate::shutdown::ShutdownHandle::new();
        // Signal shutdown immediately. The loop processes at least one batch
        // before checking the flag, then stops early.
        trigger.shutdown();

        let executor = crate::executor::Executor::new(dir.clone());
        executor.run_with_shutdown(graph, handle).await.unwrap();
        let results = collected.lock().unwrap().clone();
        // At least the first batch was processed; early termination is expected.
        assert!(!results.is_empty());
        // All produced values must be valid doubles of the input.
        for &v in &results {
            assert!(v == 2 || v == 4 || v == 6);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
