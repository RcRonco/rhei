//! Task management, I/O bridging, and per-executor data preparation for Timely execution.
//!
//! [`TaskManager`] packages all per-executor data (source receivers, transforms,
//! operators, state contexts, sink channels) and orchestrates checkpoint coordination
//! and Timely DAG execution.

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::dlq::ErrorPolicy;
use rhei_core::state::context::StateContext;
use tokio::task::JoinHandle;

use crate::compiler::CompiledGraph;
use crate::controller::PipelineController;
use crate::dataflow::{AnyItem, ErasedOperator, ErasedSource, NodeId, NodeKind, TransformFn};
use crate::executor::NodeKindTag;
use crate::shutdown::ShutdownHandle;

/// DLQ channel sender type for per-worker DLQ writes.
pub(crate) type DlqSender = tokio::sync::mpsc::Sender<rhei_core::dlq::DeadLetterRecord>;

/// All per-executor data packaged for Timely execution, plus checkpoint orchestration.
///
/// Per-executor data is assembled into [`ExecutorData`] structs and wrapped in a
/// single `Arc<Mutex<Vec<Option<…>>>>` for one-time handoff into the Timely closure.
pub(crate) struct TaskManager {
    // Per-executor data (sized to total_workers, Some for local, None for remote).
    per_executor: Arc<std::sync::Mutex<Vec<Option<ExecutorData>>>>,

    // Shared state
    pub sink_senders: Arc<HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>>>,
    pub global_watermark: Arc<AtomicU64>,
    pub checkpoint_notify_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<u64>>,
    pub checkpoint_notify_tx: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<u64>>>,

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

    // Controller-derived fields for checkpoint orchestration
    total_workers: usize,
    initial_checkpoint_id: u64,
    checkpoint_dir: PathBuf,
    process_id: Option<usize>,
    n_processes: usize,
}

/// Per-executor data extracted from the shared Mutex vectors.
///
/// Returned by [`TaskManager::take_executor_data`] for consumption inside a Timely closure.
pub(crate) struct ExecutorData {
    pub source_rx: HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>,
    pub source_wm: HashMap<NodeId, Arc<AtomicU64>>,
    pub transforms: HashMap<NodeId, TransformFn>,
    pub key_fns: HashMap<NodeId, crate::dataflow::KeyFn>,
    pub operators: HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
    pub contexts: HashMap<NodeId, StateContext>,
    pub dlq_tx: Option<DlqSender>,
}

impl TaskManager {
    /// Take per-executor data for the given worker index (one-time handoff).
    ///
    /// Panics if the data for this worker was already taken or was never populated.
    pub(crate) fn take_executor_data(&self, idx: usize) -> ExecutorData {
        self.per_executor.lock().unwrap()[idx]
            .take()
            .expect("executor data already taken")
    }

    /// Build a `TaskManager` from a compiled graph and controller configuration.
    ///
    /// Performs DLQ setup, node classification, source/sink bridging,
    /// per-worker data extraction, and global watermark task spawning.
    pub(crate) async fn build(
        mut graph: CompiledGraph,
        controller: &PipelineController,
        shutdown: Option<&ShutdownHandle>,
        rt: &tokio::runtime::Handle,
        restored_offsets: HashMap<String, String>,
        initial_checkpoint_id: u64,
    ) -> anyhow::Result<Self> {
        let total_workers = controller.total_workers();
        let local_range = controller.local_worker_range();
        let n_local = local_range.len();

        // ── DLQ setup ─────────────────────────────────────────────────
        let (mut dlq_senders, dlq_handles) =
            setup_dlq_sinks(&controller.error_policy, total_workers, &local_range);

        // ── Node classification ───────────────────────────────────────
        let (node_kinds, node_inputs, last_operator_id) = classify_nodes(&graph);

        // ── Source bridging ───────────────────────────────────────────
        let (
            mut per_worker_source_rx,
            mut per_worker_source_wm,
            all_source_offsets,
            all_source_watermarks,
        ) = bridge_sources(
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
        let (mut all_transforms, mut all_key_fns, mut all_operators, mut all_contexts) =
            extract_per_worker_data(
                &mut graph,
                controller,
                &node_kinds,
                &local_range,
                total_workers,
            )
            .await?;

        let topo_order = graph.topo_order.clone();
        let all_operator_names = graph.operator_names.clone();

        // Assemble per-executor data into ExecutorData structs.
        let mut per_executor: Vec<Option<ExecutorData>> =
            (0..total_workers).map(|_| None).collect();
        for idx in local_range.clone() {
            per_executor[idx] = Some(ExecutorData {
                source_rx: std::mem::take(&mut per_worker_source_rx[idx]),
                source_wm: std::mem::take(&mut per_worker_source_wm[idx]),
                transforms: all_transforms[idx].take().unwrap_or_default(),
                key_fns: all_key_fns[idx].take().unwrap_or_default(),
                operators: all_operators[idx].take().unwrap_or_default(),
                contexts: all_contexts[idx].take().unwrap_or_default(),
                dlq_tx: dlq_senders[idx].take(),
            });
        }

        let (checkpoint_notify_tx, checkpoint_notify_rx) = tokio::sync::mpsc::channel::<u64>(64);

        // ── Global watermark computation ──────────────────────────────
        let global_watermark = spawn_global_watermark_task(rt, all_source_watermarks, shutdown);

        let checkpoint_dir = controller.checkpoint_dir.clone();
        let process_id = controller.process_id();
        let n_processes = controller.peers.as_ref().map_or(1, Vec::len);

        #[allow(clippy::cast_possible_truncation)]
        metrics::gauge!("executor_workers").set(n_local as f64);
        tracing::info!(
            local_workers = n_local,
            total_workers = total_workers,
            process_id = ?controller.process_id(),
            cluster = controller.is_cluster(),
            "pipeline started"
        );

        Ok(TaskManager {
            per_executor: Arc::new(std::sync::Mutex::new(per_executor)),
            sink_senders: Arc::new(sink_senders),
            global_watermark,
            checkpoint_notify_rx: tokio::sync::Mutex::new(checkpoint_notify_rx),
            checkpoint_notify_tx: std::sync::Mutex::new(Some(checkpoint_notify_tx)),
            topo_order: Arc::new(topo_order),
            node_inputs: Arc::new(node_inputs),
            node_kinds: Arc::new(node_kinds),
            last_operator_id,
            all_operator_names,
            all_source_offsets,
            sink_handles,
            dlq_handles,
            total_workers,
            initial_checkpoint_id,
            checkpoint_dir,
            process_id,
            n_processes,
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

    /// Return the operator names from the compiled graph.
    pub(crate) fn operator_names(&self) -> &[String] {
        &self.all_operator_names
    }

    /// Take the checkpoint notification receiver for the mid-execution checkpoint task.
    ///
    /// Returns the receiver, replacing it with a dummy closed channel.
    pub(crate) async fn take_checkpoint_rx(&self) -> tokio::sync::mpsc::Receiver<u64> {
        let mut rx = self.checkpoint_notify_rx.lock().await;
        let (_dummy_tx, dummy_rx) = tokio::sync::mpsc::channel::<u64>(1);
        std::mem::replace(&mut *rx, dummy_rx)
    }

    /// Get handles to source offset maps (for the checkpoint task to read concurrently).
    pub(crate) fn source_offset_handles(
        &self,
    ) -> Vec<Arc<std::sync::Mutex<HashMap<String, String>>>> {
        self.all_source_offsets.clone()
    }

    /// Close the checkpoint notification channel.
    ///
    /// Drops the sender so the checkpoint task's `recv()` returns `None`
    /// and the task exits gracefully after processing queued notifications.
    pub(crate) fn close_checkpoint_channel(&self) {
        let _ = self.checkpoint_notify_tx.lock().unwrap().take();
    }

    /// Create a [`DataflowExecutor`](crate::executor::DataflowExecutor) for the given worker index.
    ///
    /// Takes `ExecutorData` from `per_executor[idx]` via `.take()` and constructs
    /// a `DataflowExecutor` ready to run inside a Timely worker closure.
    pub(crate) fn create_executor(
        &self,
        idx: usize,
        checkpoint_notify: Option<tokio::sync::mpsc::Sender<u64>>,
        shutdown_barrier: Option<Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>>,
        local_first_worker: usize,
        rt: tokio::runtime::Handle,
    ) -> crate::executor::DataflowExecutor {
        let mut data = self.take_executor_data(idx);
        let dlq_tx = data.dlq_tx.take();
        crate::executor::DataflowExecutor::new(
            self.sink_senders.clone(),
            self.topo_order.clone(),
            self.node_inputs.clone(),
            self.node_kinds.clone(),
            rt,
            idx,
            self.total_workers,
            checkpoint_notify,
            dlq_tx,
            self.last_operator_id,
            self.global_watermark.clone(),
            local_first_worker,
            data,
            shutdown_barrier,
        )
    }

    /// Run the full execution lifecycle: coordination setup, checkpoint task, Timely DAG.
    ///
    /// Returns the last `checkpoint_id` written during execution. The caller is
    /// responsible for draining sink handles and writing the final manifest.
    pub(crate) async fn run(
        self: Arc<Self>,
        controller: &PipelineController,
    ) -> anyhow::Result<u64> {
        let (coordination, coord_task_handle) = setup_coordination(controller).await?;

        let ckpt_config = CheckpointTaskConfig {
            initial_checkpoint_id: self.initial_checkpoint_id,
            all_source_offsets: self.source_offset_handles(),
            operator_names: self.all_operator_names.clone(),
            checkpoint_dir: self.checkpoint_dir.clone(),
            process_id: self.process_id,
            n_processes: self.n_processes,
        };

        // Create a shutdown barrier for coordinated process teardown.
        let (barrier_tx, barrier_rx) = if controller.is_cluster() {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let checkpoint_rx = self.take_checkpoint_rx().await;
        let checkpoint_task = tokio::spawn(async move {
            run_checkpoint_task(checkpoint_rx, ckpt_config, coordination, barrier_tx).await
        });

        let timely_config = controller.timely_config();
        let local_first_worker = controller.local_worker_range().start;
        let rt = tokio::runtime::Handle::current();

        let checkpoint_notify_tx = self.checkpoint_notify_tx.lock().unwrap().as_ref().cloned();

        // Wrap the barrier receiver so only the first local worker can take it.
        let shutdown_barrier: Option<Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>> =
            barrier_rx.map(|rx| Arc::new(std::sync::Mutex::new(Some(rx))));

        let task_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let guards = timely::execute::execute(timely_config, move |worker| {
                let idx = worker.index();
                let _span = tracing::info_span!("worker", worker = idx).entered();

                let executor = task_manager.create_executor(
                    idx,
                    checkpoint_notify_tx.clone(),
                    shutdown_barrier.clone(),
                    local_first_worker,
                    rt.clone(),
                );

                executor.run(worker);
            })
            .map_err(|e| anyhow::anyhow!("timely execution failed: {e}"))?;

            drop(guards);
            anyhow::Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

        // Close the checkpoint channel so the task drains remaining notifications
        // and exits cleanly (recv() returns None when all senders are dropped).
        self.close_checkpoint_channel();
        let last_checkpoint_id = match checkpoint_task.await {
            Ok(id) => id,
            Err(e) => return Err(anyhow::anyhow!("checkpoint task panicked: {e}")),
        };

        // Shut down coordinator task if running.
        if let Some(handle) = coord_task_handle {
            handle.abort();
            let _ = handle.await;
        }

        Ok(last_checkpoint_id)
    }
}

// ── Checkpoint coordination (moved from controller.rs) ──────────────

/// Coordination mode for checkpoint management.
///
/// In cluster mode, checkpoints are coordinated across processes via TCP.
/// Process 0 uses in-memory channels; other processes use a TCP participant.
enum CheckpointCoordination {
    /// Process 0: uses in-memory channels to the coordinator task.
    Local(crate::checkpoint_coord::LocalParticipant),
    /// Non-zero processes: TCP connection to coordinator.
    Remote(crate::checkpoint_coord::CheckpointParticipant),
}

/// Configuration for the mid-execution checkpoint task.
struct CheckpointTaskConfig {
    initial_checkpoint_id: u64,
    all_source_offsets: Vec<Arc<std::sync::Mutex<HashMap<String, String>>>>,
    operator_names: Vec<String>,
    checkpoint_dir: PathBuf,
    process_id: Option<usize>,
    n_processes: usize,
}

/// Set up cross-process checkpoint coordination for cluster mode.
///
/// Process 0 spawns a coordinator task and uses local channels.
/// Other processes connect as TCP participants with retry/backoff.
/// Returns `(coordination, coordinator_task_handle)`.
async fn setup_coordination(
    controller: &PipelineController,
) -> anyhow::Result<(
    Option<CheckpointCoordination>,
    Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
)> {
    if !controller.is_cluster() {
        return Ok((None, None));
    }

    let peers = controller.peers.as_ref().unwrap();
    let pid = controller.process_id().unwrap();
    let n_processes = peers.len();
    let coord_port = crate::checkpoint_coord::coordination_port(peers);
    let coord_addr = format!("127.0.0.1:{coord_port}");

    if pid == 0 {
        // Process 0: run coordinator + use local channels for self-participation.
        let (coordinator, channels, local_part) =
            crate::checkpoint_coord::setup_coordinator_full(&coord_addr, n_processes).await?;

        let handle = tokio::spawn(async move {
            coordinator
                .run(channels.ready_rx, channels.committed_tx)
                .await
        });

        Ok((
            Some(CheckpointCoordination::Local(local_part)),
            Some(handle),
        ))
    } else {
        // Non-zero processes: connect as TCP participant with backoff.
        let mut participant = None;
        for attempt in 0..20 {
            match crate::checkpoint_coord::CheckpointParticipant::connect(&coord_addr, pid).await {
                Ok(p) => {
                    participant = Some(p);
                    break;
                }
                Err(e) => {
                    if attempt < 19 {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    } else {
                        return Err(anyhow::anyhow!(
                            "failed to connect to checkpoint coordinator at {coord_addr}: {e}"
                        ));
                    }
                }
            }
        }
        Ok((
            Some(CheckpointCoordination::Remote(participant.unwrap())),
            None,
        ))
    }
}

/// Run the mid-execution checkpoint management loop.
///
/// Receives epoch notifications from the Timely DAG and writes checkpoint
/// manifests as each epoch completes. In cluster mode, coordinates with
/// other processes before writing the manifest.
///
/// When a [`Sentinel::Shutdown`](crate::executor::Sentinel::Shutdown) epoch
/// arrives, the task coordinates shutdown across processes (if in cluster
/// mode), then releases the barrier so workers can return and Timely's
/// TCP connections tear down simultaneously.
///
/// Returns the last `checkpoint_id` written.
async fn run_checkpoint_task(
    mut checkpoint_rx: tokio::sync::mpsc::Receiver<u64>,
    config: CheckpointTaskConfig,
    mut coordination: Option<CheckpointCoordination>,
    shutdown_barrier_tx: Option<std::sync::mpsc::Sender<()>>,
) -> u64 {
    let mut checkpoint_id = config.initial_checkpoint_id;

    while let Some(mut epoch) = checkpoint_rx.recv().await {
        // Drain any additional epoch notifications that arrived while we were
        // busy writing the last manifest. This coalesces rapid-fire
        // checkpoints into a single manifest write, preventing the channel
        // from filling up and blocking the Timely worker thread.
        while let Ok(newer_epoch) = checkpoint_rx.try_recv() {
            epoch = newer_epoch;
        }

        // Shutdown sentinel: coordinate with other processes, then release
        // the barrier so all workers can return simultaneously.
        if epoch == crate::executor::Sentinel::Shutdown as u64 {
            tracing::debug!("received shutdown sentinel — coordinating teardown");
            coordinate_epoch(epoch, &mut coordination).await;
            if let Some(ref tx) = shutdown_barrier_tx {
                let _ = tx.send(());
            }
            continue;
        }

        // Regular checkpoints are process-local: each process writes its
        // own partial manifest without waiting for other processes. This
        // avoids blocking the checkpoint task on cross-process coordination,
        // which would cause the notification channel to fill up and
        // back-pressure the Timely worker threads (blocking_send deadlock).
        // The merged manifest is written after DAG completion.

        checkpoint_id += 1;
        let offsets = merge_source_offsets(&config.all_source_offsets);
        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id,
            timestamp_ms: crate::controller::now_millis(),
            operators: config.operator_names.clone(),
            source_offsets: offsets,
        };

        if let Err(e) = write_manifest(
            &manifest,
            &config.checkpoint_dir,
            config.process_id,
            config.n_processes,
        ) {
            tracing::error!(error = %e, "mid-execution checkpoint write failed");
        } else {
            tracing::debug!(
                checkpoint_id,
                "mid-execution checkpoint #{checkpoint_id} saved"
            );
        }
    }

    checkpoint_id
}

/// Coordinate a single epoch across processes (if in cluster mode).
///
/// Sends a `Ready` message and waits for `Committed` from the coordinator.
/// In single-process mode (coordination is `None`), this is a no-op.
async fn coordinate_epoch(epoch: u64, coordination: &mut Option<CheckpointCoordination>) {
    let Some(coord) = coordination.as_mut() else {
        return;
    };
    match coord {
        CheckpointCoordination::Local(local) => {
            if let Err(e) = local.ready_tx.send(epoch).await {
                tracing::warn!(error = %e, "failed to send Ready to coordinator");
                return;
            }
            if let Some(committed_epoch) = local.committed_rx.recv().await {
                tracing::debug!(committed_epoch, "epoch committed by coordinator");
            } else {
                tracing::warn!("coordinator channel closed");
            }
        }
        CheckpointCoordination::Remote(participant) => {
            if let Err(e) = participant.send_ready(epoch).await {
                tracing::warn!(error = %e, "failed to send Ready to coordinator");
                return;
            }
            if let Ok(committed_epoch) = participant.wait_committed().await {
                tracing::debug!(committed_epoch, "epoch committed by coordinator");
            } else {
                tracing::warn!("failed to receive Committed from coordinator");
            }
        }
    }
}

/// Write a checkpoint manifest (partial in cluster mode, full in single-process).
pub(crate) fn write_manifest(
    manifest: &CheckpointManifest,
    checkpoint_dir: &std::path::Path,
    process_id: Option<usize>,
    n_processes: usize,
) -> anyhow::Result<()> {
    if let Some(pid) = process_id {
        // Cluster mode: write a partial manifest for this process.
        manifest.save_partial(checkpoint_dir, pid)?;
        tracing::debug!(
            checkpoint_id = manifest.checkpoint_id,
            process_id = pid,
            "partial checkpoint #{} saved for process {pid}",
            manifest.checkpoint_id
        );

        // Process 0 merges all partial manifests into the final manifest.
        if pid == 0 {
            let merged = CheckpointManifest::merge_partials(checkpoint_dir, n_processes);
            if let Some(merged) = merged {
                merged.save(checkpoint_dir)?;
                tracing::debug!(
                    checkpoint_id = merged.checkpoint_id,
                    "merged checkpoint saved"
                );
            } else {
                tracing::warn!("not all partial manifests available for merge");
            }
        }
    } else {
        // Single-process mode: write directly.
        manifest.save(checkpoint_dir)?;
        tracing::debug!(
            checkpoint_id = manifest.checkpoint_id,
            "checkpoint #{} saved",
            manifest.checkpoint_id
        );
    }
    Ok(())
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
/// Returns `(per_worker_source_rx, per_worker_source_wm, all_source_offsets, all_source_watermarks)`.
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
    Vec<HashMap<NodeId, Arc<AtomicU64>>>,
    Vec<Arc<std::sync::Mutex<HashMap<String, String>>>>,
    Vec<Arc<AtomicU64>>,
)> {
    let mut per_worker_source_rx: Vec<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>> =
        (0..total_workers).map(|_| HashMap::new()).collect();
    let mut per_worker_source_wm: Vec<HashMap<NodeId, Arc<AtomicU64>>> =
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
                per_worker_source_wm[worker_idx].insert(source_id, wm.clone());
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
                per_worker_source_wm[0].insert(source_id, wm.clone());
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        }
    }

    Ok((
        per_worker_source_rx,
        per_worker_source_wm,
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
    _shutdown: Option<&ShutdownHandle>,
) -> Arc<AtomicU64> {
    let global_watermark: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    if !all_source_watermarks.is_empty() {
        let gw = global_watermark.clone();
        let source_wms = all_source_watermarks;
        rt.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
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
                            tokio::sync::mpsc::channel::<rhei_core::dlq::DeadLetterRecord>(64);
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
pub(crate) fn merge_source_offsets(
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
