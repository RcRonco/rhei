//! I/O bridging and per-worker data preparation for Timely execution.
//!
//! [`WorkerSet`] packages all per-worker data (source receivers, transforms,
//! operators, state contexts, sink channels) needed by [`execute_dag`](crate::executor::execute_dag).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rill_core::dlq::ErrorPolicy;
use rill_core::state::context::StateContext;
use tokio::task::JoinHandle;

use crate::compiler::CompiledGraph;
use crate::controller::PipelineController;
use crate::dataflow::{AnyItem, ErasedOperator, ErasedSource, NodeId, NodeKind, TransformFn};
use crate::executor::NodeKindTag;
use crate::shutdown::ShutdownHandle;

/// DLQ channel sender type for per-worker DLQ writes.
pub(crate) type DlqSender = tokio::sync::mpsc::Sender<rill_core::dlq::DeadLetterRecord>;

/// All per-worker data packaged for Timely execution.
///
/// Per-worker Mutex fields are wrapped in `Arc` so they can be cloned into
/// the `'static` closure required by `timely::execute::execute`.
#[allow(clippy::type_complexity)]
pub(crate) struct WorkerSet {
    // Per-worker data (sized to total_workers, Some for local, None for remote).
    pub source_rx: Arc<
        std::sync::Mutex<Vec<Option<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>>>>,
    >,
    pub transforms: Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, TransformFn>>>>>,
    pub key_fns: Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, crate::dataflow::KeyFn>>>>>,
    pub operators:
        Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>>>>>,
    pub contexts: Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, StateContext>>>>>,
    #[allow(clippy::option_option)]
    pub dlq_tx: Arc<std::sync::Mutex<Vec<Option<Option<DlqSender>>>>>,

    // Shared state
    pub sink_senders: Arc<HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>>>,
    pub global_watermark: Arc<AtomicU64>,
    pub checkpoint_notify_rx: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    pub checkpoint_notify_tx: std::sync::mpsc::Sender<()>,

    // Graph metadata
    pub topo_order: Arc<Vec<NodeId>>,
    pub node_inputs: Arc<HashMap<NodeId, Vec<NodeId>>>,
    pub node_kinds: Arc<HashMap<NodeId, NodeKindTag>>,
    pub last_operator_id: Option<NodeId>,
    pub all_operator_names: Vec<String>,

    // Tracking for checkpoint manifests
    all_source_offsets: Vec<Arc<std::sync::Mutex<HashMap<String, String>>>>,

    // Task handles to join after execution
    sink_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    dlq_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

/// Per-worker data extracted from the shared Mutex vectors.
///
/// Returned by [`WorkerSet::take_worker_data`] for consumption inside a Timely closure.
pub(crate) struct WorkerData {
    pub source_rx: HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>,
    pub transforms: HashMap<NodeId, TransformFn>,
    pub key_fns: HashMap<NodeId, crate::dataflow::KeyFn>,
    pub operators: HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
    pub contexts: HashMap<NodeId, StateContext>,
    pub dlq_tx: Option<DlqSender>,
}

impl WorkerSet {
    /// Build a `WorkerSet` from a compiled graph and controller configuration.
    ///
    /// Performs DLQ setup, node classification, source/sink bridging,
    /// per-worker data extraction, and global watermark task spawning.
    pub(crate) async fn build(
        mut graph: CompiledGraph,
        controller: &PipelineController,
        shutdown: Option<&ShutdownHandle>,
        rt: &tokio::runtime::Handle,
        restored_offsets: HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let total_workers = controller.total_workers();
        let local_range = controller.local_worker_range();
        let n_local = local_range.len();

        // ── DLQ setup ─────────────────────────────────────────────────
        let (dlq_senders, dlq_handles) =
            setup_dlq_sinks(&controller.error_policy, total_workers, &local_range);

        // ── Node classification ───────────────────────────────────────
        let (node_kinds, node_inputs, last_operator_id) = classify_nodes(&graph);

        // ── Source bridging ───────────────────────────────────────────
        let (per_worker_source_rx, all_source_offsets, all_source_watermarks) = bridge_sources(
            &mut graph,
            total_workers,
            &local_range,
            rt,
            shutdown,
            &restored_offsets,
        )
        .await?;

        if !restored_offsets.is_empty() {
            tracing::info!(
                offsets = restored_offsets.len(),
                "source offsets restored from checkpoint"
            );
        }

        // ── Sink bridging ─────────────────────────────────────────────
        let (sink_senders, sink_handles) = bridge_sinks(&mut graph);

        // ── Per-worker data extraction ────────────────────────────────
        let (all_transforms, all_key_fns, all_operators, all_contexts) = extract_per_worker_data(
            &mut graph,
            controller,
            &node_kinds,
            &local_range,
            total_workers,
        )
        .await?;

        let topo_order = graph.topo_order.clone();
        let all_operator_names = graph.operator_names.clone();

        // Wrap per-worker source receivers.
        let per_worker_source_rx: Vec<
            Option<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>>,
        > = per_worker_source_rx.into_iter().map(Some).collect();
        // Wrap per-worker DLQ senders.
        let per_worker_dlq_tx: Vec<Option<Option<DlqSender>>> =
            dlq_senders.into_iter().map(Some).collect();

        let (checkpoint_notify_tx, checkpoint_notify_rx) = std::sync::mpsc::channel::<()>();

        // ── Global watermark computation ──────────────────────────────
        let global_watermark = spawn_global_watermark_task(rt, all_source_watermarks, shutdown);

        #[allow(clippy::cast_possible_truncation)]
        metrics::gauge!("executor_workers").set(n_local as f64);
        tracing::info!(
            local_workers = n_local,
            total_workers = total_workers,
            process_id = ?controller.process_id(),
            cluster = controller.is_cluster(),
            "pipeline started"
        );

        Ok(WorkerSet {
            source_rx: Arc::new(std::sync::Mutex::new(per_worker_source_rx)),
            transforms: Arc::new(std::sync::Mutex::new(all_transforms)),
            key_fns: Arc::new(std::sync::Mutex::new(all_key_fns)),
            operators: Arc::new(std::sync::Mutex::new(all_operators)),
            contexts: Arc::new(std::sync::Mutex::new(all_contexts)),
            dlq_tx: Arc::new(std::sync::Mutex::new(per_worker_dlq_tx)),
            sink_senders: Arc::new(sink_senders),
            global_watermark,
            checkpoint_notify_rx: std::sync::Mutex::new(checkpoint_notify_rx),
            checkpoint_notify_tx,
            topo_order: Arc::new(topo_order),
            node_inputs: Arc::new(node_inputs),
            node_kinds: Arc::new(node_kinds),
            last_operator_id,
            all_operator_names,
            all_source_offsets,
            sink_handles,
            dlq_handles,
        })
    }

    /// Join all sink and DLQ async task handles.
    ///
    /// Drops the sink senders first so that sink channels close and the
    /// async drain tasks can terminate.
    pub(crate) async fn drain(self) -> anyhow::Result<()> {
        // Drop sink senders so receivers see channel-closed and terminate.
        drop(self.sink_senders);
        for handle in self.sink_handles {
            handle
                .await
                .map_err(|e| anyhow::anyhow!("sink task panicked: {e}"))??;
        }
        for handle in self.dlq_handles {
            handle
                .await
                .map_err(|e| anyhow::anyhow!("DLQ sink task panicked: {e}"))??;
        }
        Ok(())
    }

    /// Merge offsets from all source bridges for checkpoint manifests.
    pub(crate) fn source_offsets(&self) -> HashMap<String, String> {
        merge_source_offsets(&self.all_source_offsets)
    }
}

// ── Build helpers ────────────────────────────────────────────────────

/// Classify graph nodes into kinds and collect input edges.
///
/// Returns `(node_kinds, node_inputs, last_operator_id)`.
#[allow(clippy::type_complexity)]
fn classify_nodes(
    graph: &CompiledGraph,
) -> (
    HashMap<NodeId, NodeKindTag>,
    HashMap<NodeId, Vec<NodeId>>,
    Option<NodeId>,
) {
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

    let last_operator_id = graph
        .topo_order
        .iter()
        .rev()
        .find(|id| node_kinds[id] == NodeKindTag::Operator)
        .copied();

    (node_kinds, node_inputs, last_operator_id)
}

/// Bridge all sources in the graph to per-worker mpsc receivers.
///
/// Handles both partitioned and non-partitioned source distribution.
/// Returns `(per_worker_source_rx, all_source_offsets, all_source_watermarks)`.
#[allow(clippy::type_complexity)]
async fn bridge_sources(
    graph: &mut CompiledGraph,
    total_workers: usize,
    local_range: &Range<usize>,
    rt: &tokio::runtime::Handle,
    shutdown: Option<&ShutdownHandle>,
    restored_offsets: &HashMap<String, String>,
) -> anyhow::Result<(
    Vec<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>>,
    Vec<Arc<std::sync::Mutex<HashMap<String, String>>>>,
    Vec<Arc<AtomicU64>>,
)> {
    let mut per_worker_source_rx: Vec<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>> =
        (0..total_workers).map(|_| HashMap::new()).collect();
    let mut all_source_offsets: Vec<Arc<std::sync::Mutex<HashMap<String, String>>>> = Vec::new();
    let mut all_source_watermarks: Vec<Arc<AtomicU64>> = Vec::new();

    for &source_id in &graph.source_ids {
        let source = extract_source(&mut graph.nodes[source_id.0]);

        if let Some(n_partitions) = source.partition_count() {
            // Partitioned: round-robin distribute partitions across total workers.
            tracing::info!(
                source = ?source_id,
                partitions = n_partitions,
                total_workers = total_workers,
                "distributing partitioned source"
            );
            let mut worker_partitions: Vec<Vec<usize>> = vec![vec![]; total_workers];
            for p in 0..n_partitions {
                worker_partitions[p % total_workers].push(p);
            }
            // Only create bridges for workers in local_range.
            for (worker_idx, parts) in worker_partitions.into_iter().enumerate() {
                if parts.is_empty() || !local_range.contains(&worker_idx) {
                    continue;
                }
                let mut psrc = source
                    .create_partition_source(&parts)
                    .expect("partitioned source failed to create reader");
                if !restored_offsets.is_empty() {
                    psrc.restore_offsets(restored_offsets).await?;
                }
                let (rx, offsets, wm) =
                    crate::bridge::erased_source_bridge_with_offsets(psrc, rt, shutdown.cloned());
                per_worker_source_rx[worker_idx].insert(source_id, rx);
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        } else {
            // Non-partitioned: only global worker 0 reads.
            // In cluster mode, only process 0 creates this bridge.
            if local_range.contains(&0) {
                let mut source = source;
                if !restored_offsets.is_empty() {
                    source.restore_offsets(restored_offsets).await?;
                }
                let (rx, offsets, wm) =
                    crate::bridge::erased_source_bridge_with_offsets(source, rt, shutdown.cloned());
                per_worker_source_rx[0].insert(source_id, rx);
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        }
    }

    Ok((
        per_worker_source_rx,
        all_source_offsets,
        all_source_watermarks,
    ))
}

/// Bridge all sinks in the graph to async drain tasks.
///
/// Returns `(sink_senders, sink_handles)`.
#[allow(clippy::type_complexity)]
fn bridge_sinks(
    graph: &mut CompiledGraph,
) -> (
    HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>>,
    Vec<JoinHandle<anyhow::Result<()>>>,
) {
    let mut sink_senders: HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>> = HashMap::new();
    let mut sink_handles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();

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

    (sink_senders, sink_handles)
}

/// Extract per-worker transforms, key functions, operators, and state contexts.
///
/// Creates clones for each local worker from the originals in the graph.
#[allow(clippy::type_complexity)]
async fn extract_per_worker_data(
    graph: &mut CompiledGraph,
    controller: &PipelineController,
    node_kinds: &HashMap<NodeId, NodeKindTag>,
    local_range: &Range<usize>,
    total_workers: usize,
) -> anyhow::Result<(
    Vec<Option<HashMap<NodeId, TransformFn>>>,
    Vec<Option<HashMap<NodeId, crate::dataflow::KeyFn>>>,
    Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>>>,
    Vec<Option<HashMap<NodeId, StateContext>>>,
)> {
    let mut all_transforms: Vec<Option<HashMap<NodeId, TransformFn>>> =
        (0..total_workers).map(|_| None).collect();
    let mut all_key_fns: Vec<Option<HashMap<NodeId, crate::dataflow::KeyFn>>> =
        (0..total_workers).map(|_| None).collect();
    #[allow(clippy::type_complexity)]
    let mut all_operators: Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>>> =
        (0..total_workers).map(|_| None).collect();
    let mut all_contexts: Vec<Option<HashMap<NodeId, StateContext>>> =
        (0..total_workers).map(|_| None).collect();

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

    // Create per-worker copies for local workers only.
    for worker_idx in local_range.clone() {
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
            let ctx = controller
                .create_context_for_worker(&name, worker_idx)
                .await?;
            w_contexts.insert(nid, ctx);
            w_operators.insert(nid, (name, op));
        }

        all_transforms[worker_idx] = Some(w_transforms);
        all_key_fns[worker_idx] = Some(w_key_fns);
        all_operators[worker_idx] = Some(w_operators);
        all_contexts[worker_idx] = Some(w_contexts);
    }

    Ok((all_transforms, all_key_fns, all_operators, all_contexts))
}

/// Spawn a background task that computes the global watermark every 100ms.
///
/// The global watermark is the minimum of all non-zero source watermarks.
fn spawn_global_watermark_task(
    rt: &tokio::runtime::Handle,
    all_source_watermarks: Vec<Arc<AtomicU64>>,
    shutdown: Option<&ShutdownHandle>,
) -> Arc<AtomicU64> {
    let global_watermark: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    if !all_source_watermarks.is_empty() {
        let gw = global_watermark.clone();
        let source_wms = all_source_watermarks;
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
    global_watermark
}

// ── Other helpers ───────────────────────────────────────────────────

/// Create per-worker DLQ sinks and bridge them via channels.
///
/// Returns a vector of per-worker senders (sized to `total_workers`, with `None`
/// for remote workers) and the async task handles that drive the sinks.
fn setup_dlq_sinks(
    policy: &ErrorPolicy,
    total_workers: usize,
    local_range: &Range<usize>,
) -> (Vec<Option<DlqSender>>, Vec<JoinHandle<anyhow::Result<()>>>) {
    match policy {
        ErrorPolicy::Skip => {
            let senders = vec![None; total_workers];
            (senders, Vec::new())
        }
        ErrorPolicy::DeadLetter(factory) => {
            let mut senders: Vec<Option<DlqSender>> = vec![None; total_workers];
            let mut handles = Vec::with_capacity(local_range.len());

            for i in local_range.clone() {
                match factory.create(i) {
                    Ok(sink) => {
                        let (tx, mut rx) =
                            tokio::sync::mpsc::channel::<rill_core::dlq::DeadLetterRecord>(64);
                        senders[i] = Some(tx);
                        handles.push(tokio::spawn(async move {
                            let mut sink = sink;
                            while let Some(record) = rx.recv().await {
                                sink.write(record).await?;
                            }
                            sink.flush().await?;
                            Ok(())
                        }));
                        tracing::info!(worker = i, "DLQ sink opened for worker");
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            worker = i,
                            "failed to open DLQ sink — falling back to skip for worker"
                        );
                    }
                }
            }

            (senders, handles)
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

// ── Node extraction helpers ─────────────────────────────────────────

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
