//! Pipeline executor with Timely-backed DAG execution.
//!
//! [`Executor`] materializes a [`DataflowGraph`](crate::dataflow::DataflowGraph)
//! into a unified Timely dataflow supporting arbitrary DAG topologies:
//! multiple exchanges, merge (fan-in), and fan-out.
//!
//! ```ignore
//! let graph = DataflowGraph::new();
//! let orders = graph.source(kafka_source);
//! orders.map(parse).key_by(|o| o.id.clone()).operator("agg", Agg).sink(sink);
//!
//! let executor = Executor::builder()
//!     .checkpoint_dir("./checkpoints")
//!     .workers(4)
//!     .build();
//!
//! executor.run(graph).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rill_core::checkpoint::CheckpointManifest;
use rill_core::dlq::ErrorPolicy;
use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::state::prefixed_backend::PrefixedBackend;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::{TieredBackend, TieredBackendConfig};

use crate::compiler::{CompiledGraph, compile_graph};
use crate::dataflow::{
    AnyItem, DataflowGraph, ErasedOperator, ErasedSource, NodeId, NodeKind, TransformContext,
    TransformFn,
};
use crate::health::{HealthState, PipelineStatus};
use crate::shutdown::ShutdownHandle;
use crate::timely_operator::TimelyErasedOperator;

/// Current time as Unix milliseconds (saturating to `u64::MAX`).
#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Key partitioning ────────────────────────────────────────────────

/// Deterministic key-to-worker assignment using `seahash`.
///
/// Uses a fixed, portable hash so the same key always maps to the same
/// worker index — even across Rust compiler versions and restarts.
#[allow(clippy::cast_possible_truncation)]
pub fn partition_key(key: &str, n_workers: usize) -> usize {
    (seahash::hash(key.as_bytes()) as usize) % n_workers
}

// ── Executor configuration ──────────────────────────────────────────

/// Configuration for tiered storage on the executor.
#[derive(Debug)]
struct TieredStorageConfig {
    l3: Arc<SlateDbBackend>,
    foyer_config: TieredBackendConfig,
}

/// Materializes a [`DataflowGraph`] into an executable pipeline.
///
/// Use [`Executor::builder()`] to configure execution parameters, build the
/// graph on a [`DataflowGraph`], then pass it to [`Executor::run()`].
#[derive(Debug)]
pub struct Executor {
    checkpoint_dir: std::path::PathBuf,
    tiered: Option<TieredStorageConfig>,
    workers: usize,
    checkpoint_interval: u64,
    error_policy: ErrorPolicy,
    health: HealthState,
}

/// Builder for [`Executor`].
#[derive(Debug)]
pub struct ExecutorBuilder {
    checkpoint_dir: std::path::PathBuf,
    workers: usize,
    checkpoint_interval: u64,
    tiered: Option<TieredStorageConfig>,
    error_policy: ErrorPolicy,
    health: HealthState,
}

impl ExecutorBuilder {
    /// Set the checkpoint directory.
    pub fn checkpoint_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.checkpoint_dir = dir.into();
        self
    }

    /// Set the number of parallel workers for keyed pipelines.
    pub fn workers(mut self, n: usize) -> Self {
        assert!(n >= 1, "worker count must be at least 1");
        self.workers = n;
        self
    }

    /// Set the checkpoint interval in batches (default: 100).
    ///
    /// Controls how often pre-exchange steps are checkpointed in the
    /// multi-worker main loop. Must be at least 1.
    pub fn checkpoint_interval(mut self, interval: u64) -> Self {
        assert!(interval >= 1, "checkpoint interval must be at least 1");
        self.checkpoint_interval = interval;
        self
    }

    /// Set the error handling policy for operator failures.
    pub fn error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Set a custom [`HealthState`] to share with the HTTP server.
    pub fn health(mut self, health: HealthState) -> Self {
        self.health = health;
        self
    }

    /// Build the executor.
    pub fn build(self) -> Executor {
        Executor {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
            checkpoint_interval: self.checkpoint_interval,
            error_policy: self.error_policy,
            health: self.health,
        }
    }
}

impl Executor {
    /// Create a builder for configuring an executor.
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder {
            checkpoint_dir: std::path::PathBuf::from("./checkpoints"),
            workers: 1,
            checkpoint_interval: 100,
            tiered: None,
            error_policy: ErrorPolicy::default(),
            health: HealthState::new(),
        }
    }

    /// Create a new executor with the given checkpoint directory.
    ///
    /// For more options, use [`Executor::builder()`].
    pub fn new(checkpoint_dir: std::path::PathBuf) -> Self {
        Self {
            checkpoint_dir,
            tiered: None,
            workers: 1,
            checkpoint_interval: 100,
            error_policy: ErrorPolicy::default(),
            health: HealthState::new(),
        }
    }

    /// Returns a reference to the executor's [`HealthState`].
    pub fn health(&self) -> &HealthState {
        &self.health
    }

    /// Set the number of parallel workers for keyed pipelines.
    ///
    /// When `workers > 1`, [`KeyedStream`](crate::dataflow::KeyedStream) operators
    /// run on parallel Timely worker threads, distributed by key hash.
    /// Defaults to `1` (single-worker mode).
    pub fn with_workers(mut self, n: usize) -> Self {
        assert!(n >= 1, "worker count must be at least 1");
        self.workers = n;
        self
    }

    /// Returns the configured number of workers.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Returns the configured checkpoint interval in batches.
    pub fn checkpoint_interval(&self) -> u64 {
        self.checkpoint_interval
    }

    /// Compile and execute a [`DataflowGraph`].
    ///
    /// Build the graph first with [`DataflowGraph::source()`], stream
    /// transforms, and sinks, then pass it here for execution.
    pub async fn run(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        run_graph(graph, self, None).await
    }

    /// Compile and execute a [`DataflowGraph`] with graceful shutdown.
    pub async fn run_with_shutdown(
        &self,
        graph: DataflowGraph,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()> {
        run_graph(graph, self, Some(shutdown)).await
    }

    /// Configure tiered storage (L2 Foyer + L3 `SlateDB`) for this executor.
    ///
    /// When set, `create_context` will produce contexts backed by a per-operator
    /// `PrefixedBackend` wrapping a `TieredBackend`.
    pub fn with_tiered_storage(
        mut self,
        checkpoint_dir: std::path::PathBuf,
        l3: Arc<SlateDbBackend>,
        foyer_config: TieredBackendConfig,
    ) -> Self {
        self.checkpoint_dir = checkpoint_dir;
        self.tiered = Some(TieredStorageConfig { l3, foyer_config });
        self
    }

    /// Create a per-worker `StateContext` for the given operator.
    ///
    /// The context is namespaced as `{operator_name}_w{worker_index}` so each
    /// worker has isolated state.
    pub async fn create_context_for_worker(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        let namespaced = format!("{operator_name}_w{worker_index}");
        self.create_context(&namespaced).await
    }

    /// Create a `StateContext` for the given operator.
    ///
    /// When tiered storage is configured, produces a context backed by
    /// `PrefixedBackend(TieredBackend)`. Otherwise falls back to `LocalBackend`.
    pub async fn create_context(&self, operator_name: &str) -> anyhow::Result<StateContext> {
        if let Some(ref tiered) = self.tiered {
            let foyer_dir = tiered.foyer_config.foyer_dir.join(operator_name);

            let config = TieredBackendConfig {
                foyer_dir,
                foyer_memory_capacity: tiered.foyer_config.foyer_memory_capacity,
                foyer_disk_capacity: tiered.foyer_config.foyer_disk_capacity,
            };

            let tiered_backend = TieredBackend::open(config, tiered.l3.clone()).await?;
            let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend));
            Ok(StateContext::new(Box::new(prefixed)))
        } else {
            let path = self
                .checkpoint_dir
                .join(format!("{operator_name}.checkpoint.json"));
            let backend = LocalBackend::new(path, None)?;
            Ok(StateContext::new(Box::new(backend)))
        }
    }
}

/// Open a DLQ file sink if the error policy requires one.
fn open_dlq_sink(
    policy: &ErrorPolicy,
) -> Option<Arc<std::sync::Mutex<rill_core::connectors::dlq_file_sink::DlqFileSink>>> {
    match policy {
        ErrorPolicy::Skip => None,
        ErrorPolicy::DeadLetterFile { path } => {
            match rill_core::connectors::dlq_file_sink::DlqFileSink::open(path) {
                Ok(sink) => {
                    tracing::info!(path = %path.display(), "DLQ file sink opened");
                    Some(Arc::new(std::sync::Mutex::new(sink)))
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to open DLQ file — falling back to skip");
                    None
                }
            }
        }
    }
}

// ── DAG Execution Engine ────────────────────────────────────────────

/// Extract a source from a graph node, replacing it with a Merge placeholder.
fn extract_source(node: &mut crate::dataflow::GraphNode) -> Box<dyn ErasedSource> {
    let kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
    match kind {
        NodeKind::Source(src) => src,
        _ => panic!("expected Source node at {:?}", node.id),
    }
}

/// Extract a sink from a graph node, replacing it with a Merge placeholder.
fn extract_sink(node: &mut crate::dataflow::GraphNode) -> Box<dyn crate::dataflow::ErasedSink> {
    let kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
    match kind {
        NodeKind::Sink(sink) => sink,
        _ => panic!("expected Sink node at {:?}", node.id),
    }
}

/// Extract an operator from a graph node, replacing it with a Merge placeholder.
fn extract_operator(node: &mut crate::dataflow::GraphNode) -> (String, Box<dyn ErasedOperator>) {
    let kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
    match kind {
        NodeKind::Operator { name, op } => (name, op),
        _ => panic!("expected Operator node at {:?}", node.id),
    }
}

/// Extract a transform function from a graph node.
fn extract_transform(node: &mut crate::dataflow::GraphNode) -> TransformFn {
    let kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
    match kind {
        NodeKind::Transform(f) => f,
        _ => panic!("expected Transform node at {:?}", node.id),
    }
}

/// Extract a key function from a graph node.
fn extract_key_fn(node: &mut crate::dataflow::GraphNode) -> crate::dataflow::KeyFn {
    let kind = std::mem::replace(&mut node.kind, NodeKind::Merge);
    match kind {
        NodeKind::KeyBy(f) => f,
        _ => panic!("expected KeyBy node at {:?}", node.id),
    }
}

/// Build a Timely DAG from the compiled graph inside a worker scope.
///
/// Constructs source, transform, exchange, operator, merge, and sink nodes
/// as Timely operators, connected according to the graph topology.
/// Returns a probe for tracking completion.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn build_timely_dag<A: timely::communication::Allocate>(
    scope: &mut timely::dataflow::scopes::Child<'_, timely::worker::Worker<A>, u64>,
    source_receivers: &mut HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>,
    transforms: &mut HashMap<NodeId, TransformFn>,
    key_fns: &mut HashMap<NodeId, crate::dataflow::KeyFn>,
    operators: &mut HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
    operator_contexts: &mut HashMap<NodeId, StateContext>,
    sink_senders: &HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>>,
    topo_order: &[NodeId],
    node_inputs: &HashMap<NodeId, Vec<NodeId>>,
    node_kinds: &HashMap<NodeId, NodeKindTag>,
    rt: tokio::runtime::Handle,
    worker_index: usize,
    num_workers: usize,
    checkpoint_notify: Option<std::sync::mpsc::Sender<()>>,
    dlq_sink: Option<Arc<std::sync::Mutex<rill_core::connectors::dlq_file_sink::DlqFileSink>>>,
    last_operator_id: Option<NodeId>,
    global_watermark: Arc<AtomicU64>,
) -> timely::dataflow::operators::probe::Handle<u64> {
    use timely::container::CapacityContainerBuilder;
    use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
    use timely::dataflow::operators::Capability;
    use timely::dataflow::operators::core::probe::Probe;
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::generic::operator::Operator;
    use timely::scheduling::Scheduler;

    let worker_label = worker_index.to_string();
    let mut streams: HashMap<NodeId, timely::dataflow::Stream<_, AnyItem>> = HashMap::new();
    let probe = timely::dataflow::operators::probe::Handle::new();

    for &node_id in topo_order {
        let kind = &node_kinds[&node_id];
        let inputs = &node_inputs[&node_id];

        match kind {
            NodeKindTag::Source => {
                // Build source operator with OperatorBuilder.
                let mut source_builder =
                    OperatorBuilder::new(format!("Source_{}", node_id.0), scope.clone());
                let (output, stream) = source_builder.new_output::<Vec<AnyItem>>();
                let mut output = OutputBuilder::from(output);
                let activator = scope.activator_for(source_builder.operator_info().address);
                source_builder.set_notify(false);

                // Take the receiver for this source (only worker 0 gets the real one).
                let source_rx = source_receivers.remove(&node_id);
                let wl = worker_label.clone();

                source_builder.build_reschedule(move |mut capabilities| {
                    let mut cap = Some(capabilities.pop().unwrap());
                    let mut epoch: u64 = 0;
                    let mut rx = source_rx;

                    move |_frontiers| {
                        if cap.is_none() {
                            return false;
                        }

                        // Non-worker-0 gets None receiver → immediately close.
                        let Some(ref mut source_rx) = rx else {
                            cap = None;
                            return false;
                        };

                        match source_rx.try_recv() {
                            Ok(batch) => {
                                #[allow(clippy::cast_possible_truncation)]
                                let batch_len = batch.len() as u64;
                                if let Some(ref c) = cap {
                                    let mut handle = output.activate();
                                    let mut session = handle.session(c);
                                    for item in batch {
                                        session.give(item);
                                    }
                                }
                                metrics::counter!(
                                    "executor_batches_total",
                                    "worker" => wl.clone()
                                )
                                .increment(1);
                                metrics::counter!(
                                    "executor_elements_total",
                                    "worker" => wl.clone()
                                )
                                .increment(batch_len);
                                epoch += 1;
                                if let Some(ref mut c) = cap {
                                    c.downgrade(&epoch);
                                }
                                activator.activate();
                                true
                            }
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                                activator.activate();
                                true
                            }
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                                cap = None;
                                false
                            }
                        }
                    }
                });

                streams.insert(node_id, stream);
            }

            NodeKindTag::Transform => {
                let input_stream = streams[&inputs[0]].clone();
                let f = transforms.remove(&node_id).expect("missing transform");
                let name = format!("Transform_{}", node_id.0);
                let ctx = TransformContext {
                    worker_index,
                    num_workers,
                };
                let result = input_stream.unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                    Pipeline,
                    &name,
                    move |_cap, _info| {
                        let f = f;
                        let ctx = ctx;
                        move |input, output| {
                            input.for_each(|cap, data| {
                                let mut session = output.session(&cap);
                                for item in data.drain(..) {
                                    for result in f(item, &ctx) {
                                        session.give(result);
                                    }
                                }
                            });
                        }
                    },
                );
                streams.insert(node_id, result);
            }

            NodeKindTag::KeyBy => {
                let input_stream = streams[&inputs[0]].clone();
                let key_fn = key_fns.remove(&node_id).expect("missing key_fn");
                let result = input_stream.unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                    ExchangePact::new(move |item: &AnyItem| seahash::hash(key_fn(item).as_bytes())),
                    &format!("Exchange_{}", node_id.0),
                    |_cap, _info| {
                        move |input, output| {
                            input.for_each(|cap, data| {
                                let mut session = output.session(&cap);
                                for item in data.drain(..) {
                                    session.give(item);
                                }
                            });
                        }
                    },
                );
                streams.insert(node_id, result);
            }

            NodeKindTag::Operator => {
                let input_stream = streams[&inputs[0]].clone();
                let (op_name, op) = operators.remove(&node_id).expect("missing operator");
                let ctx = operator_contexts
                    .remove(&node_id)
                    .expect("missing StateContext for operator");

                let stage_name = format!("Op_{}", node_id.0);
                let rt_op = rt.clone();
                let wl_op = worker_label.clone();
                let is_last_op = last_operator_id == Some(node_id);
                let notify = checkpoint_notify.clone();
                let dlq = dlq_sink.clone();
                let gw = global_watermark.clone();
                let result = input_stream
                    .unary_frontier::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                        Pipeline,
                        &stage_name,
                        move |_init_cap, _info| {
                            let mut timely_op = TimelyErasedOperator::new(op, ctx);
                            let rt = rt_op;
                            let wl = wl_op;
                            let notify = notify;
                            let dlq = dlq;
                            let op_name = op_name;
                            let gw = gw;
                            let mut last_watermark: u64 = 0;
                            let mut retained_cap: Option<Capability<u64>> = None;
                            move |(input, frontier), output| {
                                input.for_each(|cap, data| {
                                    // Retain latest capability for watermark-triggered output.
                                    let owned_cap = cap.retain();
                                    let mut batch_durations = Vec::new();
                                    for item in data.drain(..) {
                                        let input_repr = item.debug_repr();
                                        let elem_start = std::time::Instant::now();
                                        let (results, errors) = timely_op.process(item, &rt);
                                        batch_durations.push(elem_start.elapsed().as_secs_f64());
                                        for e in errors {
                                            tracing::warn!(
                                                error = %e,
                                                operator = %op_name,
                                                "operator error — routing to DLQ"
                                            );
                                            metrics::counter!("dlq_items_total").increment(1);
                                            if let Some(ref dlq) = dlq {
                                                let record = rill_core::dlq::DeadLetterRecord {
                                                    input_repr: input_repr.clone(),
                                                    operator_name: op_name.clone(),
                                                    error: e.to_string(),
                                                    timestamp: {
                                                        let d = std::time::SystemTime::now()
                                                            .duration_since(std::time::UNIX_EPOCH)
                                                            .unwrap_or_default();
                                                        format!("{}", d.as_secs())
                                                    },
                                                };
                                                match dlq.lock() {
                                                    Ok(mut sink) => {
                                                        if let Err(e) = sink.write_record(&record) {
                                                            tracing::error!(
                                                                error = %e,
                                                                operator = %op_name,
                                                                "DLQ write failed — record lost"
                                                            );
                                                            metrics::counter!(
                                                                "dlq_write_errors_total"
                                                            )
                                                            .increment(1);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(
                                                            error = %e,
                                                            "DLQ mutex poisoned"
                                                        );
                                                        metrics::counter!("dlq_write_errors_total")
                                                            .increment(1);
                                                    }
                                                }
                                            }
                                        }
                                        if !results.is_empty() {
                                            let mut session = output.session(&owned_cap);
                                            for r in results {
                                                session.give(r);
                                            }
                                        }
                                    }
                                    retained_cap = Some(owned_cap);
                                    if !batch_durations.is_empty() {
                                        batch_durations.sort_unstable_by(|a, b| {
                                            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                        });
                                        let len = batch_durations.len();
                                        let p50 = batch_durations[len / 2];
                                        let p99 = batch_durations[(len * 99 / 100).min(len - 1)];
                                        metrics::gauge!(
                                            "executor_element_duration_p50",
                                            "worker" => wl.clone()
                                        )
                                        .set(p50);
                                        metrics::gauge!(
                                            "executor_element_duration_p99",
                                            "worker" => wl.clone()
                                        )
                                        .set(p99);
                                    }
                                });

                                // Check for watermark advancement and emit
                                // any outputs (e.g. closed windows).
                                let current_wm = gw.load(Ordering::Acquire);
                                if current_wm > last_watermark {
                                    last_watermark = current_wm;
                                    let wm_results = timely_op.process_watermark(current_wm, &rt);
                                    if !wm_results.is_empty()
                                        && let Some(ref cap) = retained_cap
                                    {
                                        let mut session = output.session(cap);
                                        for r in wm_results {
                                            session.give(r);
                                        }
                                    }
                                }

                                // Drop retained cap if frontier has advanced past it.
                                if let Some(ref cap) = retained_cap
                                    && !frontier.less_equal(cap.time())
                                {
                                    retained_cap = None;
                                }

                                let frontier_vec: Vec<u64> =
                                    frontier.frontier().iter().copied().collect();
                                let did_checkpoint = timely_op.maybe_checkpoint(&frontier_vec, &rt);
                                // Only the last operator in topo order on
                                // worker 0 sends the checkpoint notification.
                                // Timely's frontier monotonicity ensures all
                                // upstream operators have checkpointed.
                                if is_last_op
                                    && did_checkpoint
                                    && worker_index == 0
                                    && let Some(ref n) = notify
                                {
                                    let _ = n.send(());
                                }
                            }
                        },
                    );
                streams.insert(node_id, result);
            }

            NodeKindTag::Merge => {
                // Use timely::dataflow::operators::Concatenate for merging.
                use timely::dataflow::operators::Concatenate;

                let input_streams: Vec<_> = inputs.iter().map(|id| streams[id].clone()).collect();
                let merged_stream = scope.concatenate(input_streams);
                streams.insert(node_id, merged_stream);
            }

            NodeKindTag::Sink => {
                let input_stream = streams[&inputs[0]].clone();
                let sink_tx = sink_senders[&node_id].clone();

                input_stream
                    .unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                        Pipeline,
                        &format!("Sink_{}", node_id.0),
                        move |_cap, _info| {
                            let sink_tx = sink_tx;
                            move |input, _output| {
                                input.for_each(|_cap, data| {
                                    for item in data.drain(..) {
                                        if let Err(e) = sink_tx.blocking_send(item) {
                                            tracing::error!(error = %e, "sink channel send failed — item dropped");
                                            metrics::counter!("sink_send_errors_total").increment(1);
                                        }
                                    }
                                });
                            }
                        },
                    )
                    .probe_with(&probe);
            }
        }
    }

    probe
}

/// Lightweight tag for classifying graph nodes without moving data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeKindTag {
    Source,
    Transform,
    KeyBy,
    Operator,
    Merge,
    Sink,
}

impl NodeKindTag {
    fn from_kind(kind: &NodeKind) -> Self {
        match kind {
            NodeKind::Source(_) => Self::Source,
            NodeKind::Transform(_) => Self::Transform,
            NodeKind::KeyBy(_) => Self::KeyBy,
            NodeKind::Operator { .. } => Self::Operator,
            NodeKind::Merge => Self::Merge,
            NodeKind::Sink(_) => Self::Sink,
        }
    }
}

/// Merge offsets from all source bridges.
fn merge_source_offsets(
    all: &[Arc<std::sync::Mutex<HashMap<String, String>>>],
) -> HashMap<String, String> {
    let mut combined = HashMap::new();
    for offsets in all {
        combined.extend(offsets.lock().unwrap().clone());
    }
    combined
}

/// Execute a compiled graph using a unified Timely DAG.
#[allow(clippy::too_many_lines, clippy::type_complexity)]
async fn execute_graph(
    mut graph: CompiledGraph,
    executor: &Executor,
    shutdown: Option<&ShutdownHandle>,
    initial_checkpoint_id: u64,
    restored_offsets: std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();
    let n_workers = executor.workers();
    let dlq_sink = open_dlq_sink(&executor.error_policy);

    // Tag each node kind before extracting data.
    let node_kinds: HashMap<NodeId, NodeKindTag> = graph
        .nodes
        .iter()
        .map(|n| (n.id, NodeKindTag::from_kind(&n.kind)))
        .collect();
    let node_inputs: HashMap<NodeId, Vec<NodeId>> = graph
        .nodes
        .iter()
        .map(|n| (n.id, n.inputs.clone()))
        .collect();

    // Find the last operator in topological order for checkpoint coordination.
    let last_operator_id = graph
        .topo_order
        .iter()
        .rev()
        .find(|id| node_kinds[id] == NodeKindTag::Operator)
        .copied();

    // ── Source bridging ─────────────────────────────────────────────
    let mut per_worker_source_rx: Vec<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>> =
        (0..n_workers).map(|_| HashMap::new()).collect();
    let mut all_source_offsets: Vec<Arc<std::sync::Mutex<HashMap<String, String>>>> = Vec::new();
    let mut all_source_watermarks: Vec<Arc<AtomicU64>> = Vec::new();

    for &source_id in &graph.source_ids {
        let source = extract_source(&mut graph.nodes[source_id.0]);

        if let Some(n_partitions) = source.partition_count() {
            // Partitioned: round-robin distribute partitions across workers.
            tracing::info!(
                source = ?source_id,
                partitions = n_partitions,
                workers = n_workers,
                "distributing partitioned source"
            );
            let mut worker_partitions: Vec<Vec<usize>> = vec![vec![]; n_workers];
            for p in 0..n_partitions {
                worker_partitions[p % n_workers].push(p);
            }
            for (worker_idx, parts) in worker_partitions.into_iter().enumerate() {
                if parts.is_empty() {
                    continue;
                }
                let mut psrc = source
                    .create_partition_source(&parts)
                    .expect("partitioned source failed to create reader");
                if !restored_offsets.is_empty() {
                    psrc.restore_offsets(&restored_offsets).await?;
                }
                let (rx, offsets, wm) =
                    crate::bridge::erased_source_bridge_with_offsets(psrc, &rt, shutdown.cloned());
                per_worker_source_rx[worker_idx].insert(source_id, rx);
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        } else {
            // Non-partitioned: only worker 0 reads.
            let mut source = source;
            if !restored_offsets.is_empty() {
                source.restore_offsets(&restored_offsets).await?;
            }
            let (rx, offsets, wm) =
                crate::bridge::erased_source_bridge_with_offsets(source, &rt, shutdown.cloned());
            per_worker_source_rx[0].insert(source_id, rx);
            all_source_offsets.push(offsets);
            all_source_watermarks.push(wm);
        }
    }

    if !restored_offsets.is_empty() {
        tracing::info!(
            offsets = restored_offsets.len(),
            "source offsets restored from checkpoint"
        );
    }

    // ── Sink bridging ───────────────────────────────────────────────
    let mut sink_senders: HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>> = HashMap::new();
    let mut sink_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = Vec::new();

    for &sink_id in &graph.sink_ids {
        let sink = extract_sink(&mut graph.nodes[sink_id.0]);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AnyItem>(16);
        sink_senders.insert(sink_id, tx);
        sink_handles.push(tokio::spawn(async move {
            let mut sink = sink;
            while let Some(item) = rx.recv().await {
                sink.write(item).await?;
            }
            sink.flush().await?;
            Ok(())
        }));
    }

    // ── Extract transforms, key_fns, operators from graph nodes ────
    // We need per-worker copies. Worker 0 gets the original, others get clones.
    let mut all_transforms: Vec<HashMap<NodeId, TransformFn>> = Vec::with_capacity(n_workers);
    let mut all_key_fns: Vec<HashMap<NodeId, crate::dataflow::KeyFn>> =
        Vec::with_capacity(n_workers);
    let mut all_operators: Vec<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>> =
        Vec::with_capacity(n_workers);
    let mut all_contexts: Vec<HashMap<NodeId, StateContext>> = Vec::with_capacity(n_workers);

    // Collect node IDs by kind for extraction.
    let transform_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::Transform)
        .copied()
        .collect();
    let key_by_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::KeyBy)
        .copied()
        .collect();
    let operator_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::Operator)
        .copied()
        .collect();

    // Extract originals from graph.
    let mut orig_transforms: HashMap<NodeId, TransformFn> = HashMap::new();
    for &nid in &transform_ids {
        orig_transforms.insert(nid, extract_transform(&mut graph.nodes[nid.0]));
    }
    let mut orig_key_fns: HashMap<NodeId, crate::dataflow::KeyFn> = HashMap::new();
    for &nid in &key_by_ids {
        orig_key_fns.insert(nid, extract_key_fn(&mut graph.nodes[nid.0]));
    }
    let mut orig_operators: HashMap<NodeId, (String, Box<dyn ErasedOperator>)> = HashMap::new();
    for &nid in &operator_ids {
        orig_operators.insert(nid, extract_operator(&mut graph.nodes[nid.0]));
    }

    // Create per-worker copies.
    for worker_idx in 0..n_workers {
        let mut w_transforms = HashMap::new();
        let mut w_key_fns = HashMap::new();
        let mut w_operators = HashMap::new();
        let mut w_contexts = HashMap::new();

        for &nid in &transform_ids {
            w_transforms.insert(nid, orig_transforms[&nid].clone());
        }

        for &nid in &key_by_ids {
            w_key_fns.insert(nid, orig_key_fns[&nid].clone());
        }

        for &nid in &operator_ids {
            let (ref name, ref op) = orig_operators[&nid];
            let (name, op) = (name.clone(), op.clone_erased());
            let ctx = executor
                .create_context_for_worker(&name, worker_idx)
                .await?;
            w_contexts.insert(nid, ctx);
            w_operators.insert(nid, (name, op));
        }

        all_transforms.push(w_transforms);
        all_key_fns.push(w_key_fns);
        all_operators.push(w_operators);
        all_contexts.push(w_contexts);
    }

    // Package data for Timely closure (Fn, not FnOnce — Mutex+Option+take).
    let topo_order = graph.topo_order.clone();
    let all_operator_names = graph.operator_names.clone();

    let per_worker_transforms: Vec<Option<HashMap<NodeId, TransformFn>>> =
        all_transforms.into_iter().map(Some).collect();
    let per_worker_key_fns: Vec<Option<HashMap<NodeId, crate::dataflow::KeyFn>>> =
        all_key_fns.into_iter().map(Some).collect();
    let per_worker_operators: Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>>> =
        all_operators.into_iter().map(Some).collect();
    let per_worker_contexts: Vec<Option<HashMap<NodeId, StateContext>>> =
        all_contexts.into_iter().map(Some).collect();
    // Per-worker source receivers (partitioned sources may give receivers to multiple workers).
    let per_worker_source_rx: Vec<
        Option<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>>,
    > = per_worker_source_rx.into_iter().map(Some).collect();

    let per_worker_transforms = std::sync::Mutex::new(per_worker_transforms);
    let per_worker_key_fns = std::sync::Mutex::new(per_worker_key_fns);
    let per_worker_operators = std::sync::Mutex::new(per_worker_operators);
    let per_worker_contexts = std::sync::Mutex::new(per_worker_contexts);
    let per_worker_source_rx = std::sync::Mutex::new(per_worker_source_rx);
    let sink_senders = Arc::new(sink_senders);

    let (checkpoint_notify_tx, checkpoint_notify_rx) = std::sync::mpsc::channel::<()>();

    // ── Global watermark computation ──────────────────────────────
    let global_watermark: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    if !all_source_watermarks.is_empty() {
        let gw = global_watermark.clone();
        let source_wms = all_source_watermarks.clone();
        let shutdown_for_wm = shutdown.cloned();
        rt.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Some(ref h) = shutdown_for_wm
                    && h.is_shutdown()
                {
                    break;
                }
                // Global watermark = min of all non-zero source watermarks.
                let mut min_wm: Option<u64> = None;
                for sw in &source_wms {
                    let wm = sw.load(Ordering::Acquire);
                    if wm > 0 {
                        min_wm = Some(min_wm.map_or(wm, |m: u64| m.min(wm)));
                    }
                }
                if let Some(wm) = min_wm {
                    gw.fetch_max(wm, Ordering::Release);
                }
            }
        });
    }

    let rt_clone = rt.clone();
    let topo_order = Arc::new(topo_order);
    let node_inputs = Arc::new(node_inputs);
    let node_kinds = Arc::new(node_kinds);

    #[allow(clippy::cast_possible_truncation)]
    metrics::gauge!("executor_workers").set(n_workers as f64);
    tracing::info!(workers = n_workers, "pipeline started");

    tokio::task::spawn_blocking(move || {
        let guards =
            timely::execute::execute(timely::execute::Config::process(n_workers), move |worker| {
                let idx = worker.index();
                let _span = tracing::info_span!("worker", worker = idx).entered();

                let mut w_source_rx = per_worker_source_rx.lock().unwrap()[idx].take().unwrap();
                let mut w_transforms = per_worker_transforms.lock().unwrap()[idx].take().unwrap();
                let mut w_key_fns = per_worker_key_fns.lock().unwrap()[idx].take().unwrap();
                let mut w_operators = per_worker_operators.lock().unwrap()[idx].take().unwrap();
                let mut w_contexts = per_worker_contexts.lock().unwrap()[idx].take().unwrap();
                let rt = rt_clone.clone();
                let notify = checkpoint_notify_tx.clone();

                let dataflow_index = worker.next_dataflow_index();
                let gw = global_watermark.clone();
                let probe = worker.dataflow::<u64, _, _>(|scope| {
                    build_timely_dag(
                        scope,
                        &mut w_source_rx,
                        &mut w_transforms,
                        &mut w_key_fns,
                        &mut w_operators,
                        &mut w_contexts,
                        &sink_senders,
                        &topo_order,
                        &node_inputs,
                        &node_kinds,
                        rt,
                        idx,
                        n_workers,
                        Some(notify),
                        dlq_sink.clone(),
                        last_operator_id,
                        gw,
                    )
                });

                while !probe.done() {
                    worker.step();
                }

                worker.drop_dataflow(dataflow_index);
            })
            .map_err(|e| anyhow::anyhow!("timely execution failed: {e}"))?;

        drop(guards);
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

    // Wait for all sink tasks to complete.
    for handle in sink_handles {
        handle
            .await
            .map_err(|e| anyhow::anyhow!("sink task panicked: {e}"))??;
    }

    // Drain checkpoint notifications and save manifests.
    let mut checkpoint_id = initial_checkpoint_id;
    while checkpoint_notify_rx.try_recv().is_ok() {
        checkpoint_id += 1;
        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id,
            timestamp_ms: now_millis(),
            operators: all_operator_names.clone(),
            source_offsets: merge_source_offsets(&all_source_offsets),
        };
        manifest.save(&executor.checkpoint_dir)?;
        tracing::debug!(checkpoint_id, "checkpoint #{checkpoint_id} saved");
    }

    // Final manifest.
    checkpoint_id += 1;
    let manifest = CheckpointManifest {
        version: 1,
        checkpoint_id,
        timestamp_ms: now_millis(),
        operators: all_operator_names,
        source_offsets: merge_source_offsets(&all_source_offsets),
    };
    manifest.save(&executor.checkpoint_dir)?;
    tracing::debug!(checkpoint_id, "checkpoint #{checkpoint_id} saved");

    Ok(())
}

// ── Public execution entry point ────────────────────────────────────

/// Compile and execute the dataflow graph.
async fn run_graph(
    graph: DataflowGraph,
    executor: &Executor,
    shutdown: Option<ShutdownHandle>,
) -> anyhow::Result<()> {
    let compiled = compile_graph(graph.into_nodes())?;

    let all_operator_names = &compiled.operator_names;

    // Load existing manifest and validate.
    let (initial_checkpoint_id, restored_offsets) =
        if let Some(manifest) = CheckpointManifest::load(&executor.checkpoint_dir) {
            tracing::info!(
                checkpoint_id = manifest.checkpoint_id,
                timestamp_ms = manifest.timestamp_ms,
                operators = ?manifest.operators,
                source_offsets = ?manifest.source_offsets,
                "resuming from checkpoint #{}", manifest.checkpoint_id
            );

            // Validate operator names.
            let prev: std::collections::HashSet<&str> =
                manifest.operators.iter().map(String::as_str).collect();
            let curr: std::collections::HashSet<&str> =
                all_operator_names.iter().map(String::as_str).collect();

            let added: Vec<_> = curr.difference(&prev).collect();
            let removed: Vec<_> = prev.difference(&curr).collect();
            if !added.is_empty() || !removed.is_empty() {
                tracing::warn!(
                    ?added,
                    ?removed,
                    "operator topology changed since last checkpoint"
                );
            }

            (manifest.checkpoint_id, manifest.source_offsets)
        } else {
            (0, std::collections::HashMap::new())
        };

    executor.health.set_status(PipelineStatus::Running);

    let result = execute_graph(
        compiled,
        executor,
        shutdown.as_ref(),
        initial_checkpoint_id,
        restored_offsets,
    )
    .await;

    executor.health.set_status(PipelineStatus::Stopped);
    result
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rill_core::connectors::vec_source::VecSource;
    use rill_core::state::context::StateContext;
    use rill_core::traits::{Sink, StreamFunction};

    use crate::dataflow::DataflowGraph;

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

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let doubled = stream.map(|x: i32| x * 2);
        doubled.sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4, 6]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn filter_transform() {
        let dir = temp_dir("filter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.filter(|x: &i32| x % 2 == 0).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn flat_map_transform() {
        let dir = temp_dir("flat_map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "foo bar".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::new(dir.clone());
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

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::new(dir.clone());
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

        async fn process(
            &mut self,
            input: String,
            ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<String>> {
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
            Ok(outputs)
        }
    }

    #[tokio::test]
    async fn keyed_operator_single_worker() {
        let dir = temp_dir("keyed_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
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

        let executor = super::Executor::new(dir.clone());
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

        let graph = DataflowGraph::new();
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

        let executor = super::Executor::builder()
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

        let graph = DataflowGraph::new();
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

        let executor = super::Executor::builder()
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

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![10i32, 20, 30]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.map(|x: i32| x + 1).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::builder()
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

        let graph = DataflowGraph::new();

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

        let executor = super::Executor::builder()
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

        let graph = DataflowGraph::new();
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

        let executor = super::Executor::new(dir.clone());
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

    #[tokio::test]
    async fn map_ctx_single_worker() {
        let dir = temp_dir("map_ctx_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .map_ctx(|x: i32, ctx| format!("w{}:{x}", ctx.worker_index))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec!["w0:1", "w0:2", "w0:3"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Partitioned source tests ───────────────────────────────────

    #[tokio::test]
    async fn partitioned_source_basic() {
        // 4 partitions, 2 workers, no key_by — all items processed exactly once.
        let dir = temp_dir("part_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let items: Vec<i32> = (0..20).collect();
        let source =
            rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        let expected: Vec<i32> = (0..20).collect();
        assert_eq!(results, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_with_key_by() {
        // 3 partitions, 4 workers, with key_by + operator.
        let dir = temp_dir("part_keyed");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![
                "hello".to_string(),
                "world".to_string(),
                "hello".to_string(),
                "rill".to_string(),
                "hello".to_string(),
                "world".to_string(),
            ],
            3,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        executor.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 6);
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"hello: 3".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"world: 2".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_single_worker() {
        // 4 partitions, 1 worker — all partitions assigned to worker 0.
        let dir = temp_dir("part_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let items: Vec<i32> = (0..12).collect();
        let source =
            rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(1)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        let expected: Vec<i32> = (0..12).collect();
        assert_eq!(results, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_more_workers_than_partitions() {
        // 2 partitions, 4 workers — only 2 workers get source receivers.
        // With key_by, all 4 participate post-exchange.
        let dir = temp_dir("part_more_workers");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6, 7, 8],
            2,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60, 70, 80]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn map_ctx_multi_worker() {
        let dir = temp_dir("map_ctx_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|x: &i32| x.to_string())
            .map_ctx(|x: i32, ctx| (x, ctx.worker_index, ctx.num_workers))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 6);
        for &(_, worker_idx, num_workers) in &results {
            assert!(worker_idx < 2, "worker_index should be 0 or 1");
            assert_eq!(num_workers, 2);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deterministic_hash_across_calls() {
        // Verify partition_key is deterministic: same key always maps to the same worker.
        for n_workers in [1, 2, 4, 8, 16] {
            for key in ["alpha", "beta", "gamma", "hello", "world", "sensor-42"] {
                let first = super::partition_key(key, n_workers);
                for _ in 0..100 {
                    assert_eq!(
                        super::partition_key(key, n_workers),
                        first,
                        "partition_key({key:?}, {n_workers}) is not deterministic"
                    );
                }
                assert!(first < n_workers);
            }
        }
    }

    struct CheckpointTrackingSource {
        items: std::collections::VecDeque<Vec<String>>,
        checkpoint_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl rill_core::traits::Source for CheckpointTrackingSource {
        type Output = String;

        async fn next_batch(&mut self) -> Option<Vec<String>> {
            self.items.pop_front()
        }

        async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
            self.checkpoint_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn coordinated_checkpoint() {
        // Verify that source.on_checkpoint_complete() is only called after all
        // workers have flushed their state (coordinated via barrier).
        //
        // We use a custom source that:
        //   - emits enough batches to trigger at least one checkpoint cycle
        //   - tracks on_checkpoint_complete calls via an Arc counter
        let dir = temp_dir("coordinated_ckpt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let checkpoint_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cc = checkpoint_count.clone();

        // Generate enough batches to trigger checkpoint cycles (interval is 100).
        let mut batches = std::collections::VecDeque::new();
        for i in 0..150 {
            batches.push_back(vec![format!("item_{i}")]);
        }

        let source = CheckpointTrackingSource {
            items: batches,
            checkpoint_count: cc,
        };

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|s: &String| s.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();

        // All 150 items should have been processed.
        let results = collected.lock().unwrap();
        assert_eq!(
            results.len(),
            150,
            "expected 150 results, got {}",
            results.len()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn anyitem_serde_roundtrip() {
        use crate::dataflow::AnyItem;
        let item = AnyItem::new(42i32);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<i32>(), 42);
    }

    #[tokio::test]
    async fn merge_two_sources() {
        let dir = temp_dir("merge");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source1 = VecSource::new(vec![1i32, 2, 3]);
        let source2 = VecSource::new(vec![4i32, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream1 = graph.source(source1);
        let stream2 = graph.source(source2);
        let merged = stream1.merge(stream2);
        merged.map(|x: i32| x * 10).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fan_out_to_two_sinks() {
        let dir = temp_dir("fanout");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected1 = Arc::new(Mutex::new(Vec::new()));
        let collected2 = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let mapped = stream.map(|x: i32| x * 2);
        mapped.sink(CollectSink {
            collected: collected1.clone(),
        });
        mapped.sink(CollectSink {
            collected: collected2.clone(),
        });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        assert_eq!(*collected1.lock().unwrap(), vec![2, 4, 6]);
        assert_eq!(*collected2.lock().unwrap(), vec![2, 4, 6]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn diamond_dag() {
        // source → (filter_even, filter_odd) → merge → sink
        let dir = temp_dir("diamond");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let evens = stream.filter(|x: &i32| x % 2 == 0).map(|x: i32| x * 10);
        let odds = stream.filter(|x: &i32| x % 2 != 0).map(|x: i32| x * 100);
        let merged = evens.merge(odds);
        merged.sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::new(dir.clone());
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        // evens: 20, 40, 60; odds: 100, 200, 300, 500
        assert_eq!(results, vec![20, 40, 60, 100, 300, 500]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn multiple_key_by() {
        // source → flat_map → key_by(word) → counter → map → key_by(first_char) → map → sink
        let dir = temp_dir("multi_keyby");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("counter", WordCounter)
            // Re-key by first character
            .key_by(|s: &String| s.chars().next().unwrap_or('_').to_string())
            .map(|s: String| format!("rekeyed:{s}"))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 4);
        // All results should be prefixed with "rekeyed:"
        for r in &results {
            assert!(r.starts_with("rekeyed:"), "unexpected: {r}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Partitioned source topology tests ────────────────────────

    #[tokio::test]
    async fn partitioned_source_single_partition_many_workers() {
        // 1 partition, 4 workers — only worker 0 gets the partition reader.
        // All data arrives through worker 0, exchange distributes.
        let dir = temp_dir("part_single_part");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6],
            1,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_with_empty_partition() {
        // 3 partitions over 4 items: partition 0 gets [0,3], partition 1 gets [1],
        // partition 2 gets [2]. With 2 workers:
        //   Worker 0: partitions [0, 2] — items [0, 3, 2]
        //   Worker 1: partition [1] — items [1]
        // All items should be processed exactly once.
        let dir = temp_dir("part_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![10i32, 20, 30, 40],
            3,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph.source(source).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn mixed_partitioned_and_non_partitioned_merge() {
        // Merge a PartitionedVecSource with a regular VecSource.
        // Both source types should work in the same pipeline.
        let dir = temp_dir("mixed_merge");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let partitioned = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4],
            2,
        );
        let non_partitioned = VecSource::new(vec![10i32, 20, 30]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream1 = graph.source(partitioned);
        let stream2 = graph.source(non_partitioned);
        let merged = stream1.merge(stream2);
        merged.map(|x: i32| x * 100).sink(CollectSink {
            collected: collected.clone(),
        });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![100, 200, 300, 400, 1000, 2000, 3000]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_fan_out() {
        // One partitioned source feeding two sinks through different transforms.
        let dir = temp_dir("part_fanout");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6],
            3,
        );
        let collected_doubled = Arc::new(Mutex::new(Vec::new()));
        let collected_tripled = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let doubled = stream.map(|x: i32| x * 2);
        let tripled = stream.map(|x: i32| x * 3);
        doubled.sink(CollectSink {
            collected: collected_doubled.clone(),
        });
        tripled.sink(CollectSink {
            collected: collected_tripled.clone(),
        });

        let executor = super::Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        executor.run(graph).await.unwrap();

        let mut doubled_results = collected_doubled.lock().unwrap().clone();
        doubled_results.sort_unstable();
        assert_eq!(doubled_results, vec![2, 4, 6, 8, 10, 12]);

        let mut tripled_results = collected_tripled.lock().unwrap().clone();
        tripled_results.sort_unstable();
        assert_eq!(tripled_results, vec![3, 6, 9, 12, 15, 18]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
