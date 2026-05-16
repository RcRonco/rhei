//! Task management, I/O bridging, and per-executor data preparation for Timely execution.
//!
//! [`TaskManager`] packages all per-executor data (batch source receivers, transforms,
//! operators, state contexts, sink channels) and orchestrates checkpoint coordination
//! and Timely DAG execution.

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::dlq::ErrorPolicy;
use tokio::task::JoinHandle;

use rhei_core::arrow::OperatorContext;

use crate::compiler::CompiledGraph;
use crate::controller::PipelineController;
use crate::dataflow::{BatchTransformFn, LazyBatchTransformNode, NodeId, NodeKind};
use crate::erased_batch::{ErasedBatchOperator, ErasedSink, ErasedSource};
use crate::erased_buffer::{ErasedBuffer, KeyFn};
use crate::executor::NodeKindTag;
use crate::shutdown::ShutdownHandle;

/// DLQ channel sender type for per-worker DLQ writes.
pub(crate) type DlqSender = flume::Sender<rhei_core::dlq::DeadLetterRecord>;

/// All per-executor data packaged for Timely execution, plus checkpoint orchestration.
///
/// Per-executor data is assembled into [`ExecutorData`] structs and wrapped in a
/// single `Arc<Mutex<Vec<Option<…>>>>` for one-time handoff into the Timely closure.
pub(crate) struct TaskManager {
    // Per-executor data (sized to total_workers, Some for local, None for remote).
    per_executor: Arc<std::sync::Mutex<Vec<Option<ExecutorData>>>>,

    // Shared state
    pub all_source_watermarks: Arc<Vec<Arc<AtomicU64>>>,
    pub checkpoint_notify_rx: std::sync::Mutex<Option<flume::Receiver<u64>>>,
    pub checkpoint_notify_tx: std::sync::Mutex<Option<flume::Sender<u64>>>,

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
#[allow(dead_code)] // some fields are reserved for future batch-path use
pub(crate) struct ExecutorData {
    pub source_wm: HashMap<NodeId, Arc<AtomicU64>>,
    pub source_offsets: HashMap<NodeId, Arc<std::sync::Mutex<HashMap<String, String>>>>,
    pub shutdown: Option<ShutdownHandle>,
    pub dlq_tx: Option<DlqSender>,
    // Batch (Arrow) fields
    pub source_rx: HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>,
    pub transforms: HashMap<NodeId, BatchTransformFn>,
    pub batch_operators: HashMap<NodeId, (String, Box<dyn ErasedBatchOperator>)>,
    pub batch_contexts: HashMap<NodeId, OperatorContext>,
    pub sink_senders: HashMap<NodeId, flume::Sender<ErasedBuffer>>,
    pub key_fns: HashMap<NodeId, KeyFn>,
}

impl TaskManager {
    /// Take per-executor data for the given worker index (one-time handoff).
    ///
    /// Panics if the data for this worker was already taken or was never populated.
    #[allow(clippy::expect_used)] // invariant: each worker takes data exactly once
    pub(crate) fn take_executor_data(&self, idx: usize) -> ExecutorData {
        self.per_executor.lock().unwrap_or_else(|e| {
            tracing::warn!("mutex poisoned in per_executor lock, recovering inner data");
            e.into_inner()
        })[idx]
            .take()
            .expect("executor data already taken for worker")
    }

    /// Build a `TaskManager` from a compiled graph and controller configuration.
    ///
    /// Performs DLQ setup, node classification, batch source/sink bridging,
    /// per-worker data extraction, and global watermark task spawning.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::unused_async)] // kept async for API compatibility with callers
    pub(crate) async fn build(
        mut graph: CompiledGraph,
        controller: &PipelineController,
        shutdown: Option<&ShutdownHandle>,
        restored_offsets: HashMap<String, String>,
        initial_checkpoint_id: u64,
    ) -> anyhow::Result<Self> {
        let total_workers = controller.total_workers();
        let local_range = controller.local_worker_range();
        let n_local = local_range.len();

        // ── DLQ setup ─────────────────────────────────────────────────
        let dlq_sink = controller
            .dlq_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (mut dlq_senders, dlq_handles) = setup_dlq_sinks(
            &controller.error_policy,
            dlq_sink,
            total_workers,
            &local_range,
        );

        // ── Node classification ───────────────────────────────────────
        let (node_kinds, node_inputs, last_operator_id) = classify_nodes(&graph);

        if !restored_offsets.is_empty() {
            tracing::info!(
                offsets = restored_offsets.len(),
                "source offsets restored from checkpoint"
            );
        }

        let topo_order = graph.topo_order.clone();
        let all_operator_names = graph.operator_names.clone();

        // Per-worker watermark/offset vecs initialised empty; batch extraction populates them.
        let mut per_worker_source_wm: Vec<HashMap<NodeId, Arc<AtomicU64>>> =
            (0..total_workers).map(|_| HashMap::new()).collect();
        #[allow(clippy::type_complexity)]
        let mut per_worker_source_offsets: Vec<
            HashMap<NodeId, Arc<std::sync::Mutex<HashMap<String, String>>>>,
        > = (0..total_workers).map(|_| HashMap::new()).collect();
        let mut all_source_offsets: Vec<Arc<std::sync::Mutex<HashMap<String, String>>>> =
            Vec::new();
        let mut all_source_watermarks: Vec<Arc<AtomicU64>> = Vec::new();

        // ── Batch (Arrow) data extraction ────────────────────────────
        let (
            mut per_worker_source_rx,
            mut per_worker_batch_transforms,
            mut per_worker_batch_operators,
            mut per_worker_batch_contexts,
            mut per_worker_sink_senders,
            mut per_worker_key_fns,
            sink_handles,
        ) = extract_batch_per_worker_data(
            &mut graph,
            controller,
            &node_kinds,
            &local_range,
            total_workers,
            shutdown,
            &mut per_worker_source_wm,
            &mut per_worker_source_offsets,
            &mut all_source_offsets,
            &mut all_source_watermarks,
        )?;

        // Assemble per-executor data into ExecutorData structs.
        let mut per_executor: Vec<Option<ExecutorData>> =
            (0..total_workers).map(|_| None).collect();
        for idx in local_range.clone() {
            per_executor[idx] = Some(ExecutorData {
                source_wm: std::mem::take(&mut per_worker_source_wm[idx]),
                source_offsets: std::mem::take(&mut per_worker_source_offsets[idx]),
                shutdown: shutdown.cloned(),
                dlq_tx: dlq_senders[idx].take(),
                source_rx: std::mem::take(&mut per_worker_source_rx[idx]),
                transforms: per_worker_batch_transforms[idx].take().unwrap_or_default(),
                batch_operators: per_worker_batch_operators[idx].take().unwrap_or_default(),
                batch_contexts: per_worker_batch_contexts[idx].take().unwrap_or_default(),
                sink_senders: std::mem::take(&mut per_worker_sink_senders[idx]),
                key_fns: per_worker_key_fns[idx].take().unwrap_or_default(),
            });
        }

        let (checkpoint_notify_tx, checkpoint_notify_rx) = flume::bounded::<u64>(64);

        let all_source_watermarks = Arc::new(all_source_watermarks);

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
            all_source_watermarks,
            checkpoint_notify_rx: std::sync::Mutex::new(Some(checkpoint_notify_rx)),
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
    pub(crate) async fn drain(self) -> anyhow::Result<()> {
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
    /// Returns the receiver. Can only be called once.
    #[allow(clippy::expect_used)] // invariant: checkpoint_rx is taken exactly once
    pub(crate) fn take_checkpoint_rx(&self) -> flume::Receiver<u64> {
        self.checkpoint_notify_rx
            .lock()
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "mutex poisoned in checkpoint_notify_rx lock, recovering inner data"
                );
                e.into_inner()
            })
            .take()
            .expect("checkpoint_rx already taken")
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
        let _ = self
            .checkpoint_notify_tx
            .lock()
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "mutex poisoned in checkpoint_notify_tx lock (close), recovering inner data"
                );
                e.into_inner()
            })
            .take();
    }

    /// Create a [`DataflowExecutor`](crate::executor::DataflowExecutor) for the given worker index.
    ///
    /// Takes `ExecutorData` from `per_executor[idx]` via `.take()` and constructs
    /// a `DataflowExecutor` ready to run inside a Timely worker closure.
    pub(crate) fn create_executor(
        &self,
        idx: usize,
        checkpoint_notify: Option<flume::Sender<u64>>,
        shutdown_barrier: Option<Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>>,
        local_first_worker: usize,
        rt: tokio::runtime::Handle,
    ) -> crate::executor::DataflowExecutor {
        let mut data = self.take_executor_data(idx);
        let dlq_tx = data.dlq_tx.take();
        crate::executor::DataflowExecutor::new(
            self.topo_order.clone(),
            self.node_inputs.clone(),
            self.node_kinds.clone(),
            rt,
            idx,
            self.total_workers,
            checkpoint_notify,
            dlq_tx,
            self.last_operator_id,
            self.all_source_watermarks.clone(),
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
            workers_per_process: controller.workers,
        };

        // Create a shutdown barrier for coordinated process teardown.
        let (barrier_tx, barrier_rx) = if controller.is_cluster() {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let checkpoint_rx = self.take_checkpoint_rx();
        let checkpoint_task = tokio::spawn(async move {
            run_checkpoint_task(checkpoint_rx, ckpt_config, coordination, barrier_tx).await
        });

        let timely_config = controller.timely_config()?;
        let local_first_worker = controller.local_worker_range().start;
        let rt = tokio::runtime::Handle::current();

        let checkpoint_notify_tx = self
            .checkpoint_notify_tx
            .lock()
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "mutex poisoned in checkpoint_notify_tx lock (run), recovering inner data"
                );
                e.into_inner()
            })
            .as_ref()
            .cloned();

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

            // Separate the two panic sources in WorkerGuards to avoid a
            // double-panic abort:
            //
            // WorkerGuards contains worker JoinHandles (guards) AND TCP
            // send/recv JoinHandles (CommsGuard, stored in `others`).
            // Both use .expect() in their Drop impls, so if a worker
            // panics AND a TCP thread panics (broken pipe on cluster
            // teardown or crash recovery), Drop hits two panics — an
            // unrecoverable abort that catch_unwind cannot prevent.
            //
            // Fix: call .join() first, which drains the worker handles
            // into Results (no panic). Then only CommsGuard remains as a
            // drop-time panic source — a single panic that catch_unwind
            // can handle. No leaks, no abort.
            let join_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| guards.join()));
            match join_result {
                Ok(results) => {
                    for (i, result) in results.iter().enumerate() {
                        if let Err(msg) = result {
                            tracing::error!("worker {i} panicked: {msg}");
                        }
                    }
                }
                Err(e) => {
                    let msg = e
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| e.downcast_ref::<&str>().copied())
                        .unwrap_or("unknown panic");
                    tracing::warn!(
                        "timely TCP teardown panic (expected during shutdown/recovery): {msg}"
                    );
                }
            }
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
    workers_per_process: usize,
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

    let peers = controller
        .peers
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("peers should be set in cluster mode"))?;
    let pid = controller
        .process_id()
        .ok_or_else(|| anyhow::anyhow!("process_id should be set in cluster mode"))?;
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
            Some(CheckpointCoordination::Remote(participant.unwrap_or_else(
                || panic!("participant should be connected after retries"),
            ))),
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
    checkpoint_rx: flume::Receiver<u64>,
    config: CheckpointTaskConfig,
    mut coordination: Option<CheckpointCoordination>,
    shutdown_barrier_tx: Option<std::sync::mpsc::Sender<()>>,
) -> u64 {
    let mut checkpoint_id = config.initial_checkpoint_id;

    while let Ok(mut epoch) = checkpoint_rx.recv_async().await {
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
            n_processes: Some(config.n_processes),
            workers_per_process: Some(config.workers_per_process),
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
        .find(|id| node_kinds[id] == NodeKindTag::BatchOperator)
        .copied();

    (node_kinds, node_inputs, last_operator_id)
}

// ── Batch (Arrow) per-worker data extraction ───────────────────────

/// Extract batch sources, transforms, operators, sinks, and contexts for each local worker.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity
)]
fn extract_batch_per_worker_data(
    graph: &mut CompiledGraph,
    controller: &PipelineController,
    node_kinds: &HashMap<NodeId, NodeKindTag>,
    local_range: &Range<usize>,
    total_workers: usize,
    shutdown: Option<&ShutdownHandle>,
    per_worker_source_wm: &mut [HashMap<NodeId, Arc<AtomicU64>>],
    per_worker_source_offsets: &mut [HashMap<
        NodeId,
        Arc<std::sync::Mutex<HashMap<String, String>>>,
    >],
    all_source_offsets: &mut Vec<Arc<std::sync::Mutex<HashMap<String, String>>>>,
    all_source_watermarks: &mut Vec<Arc<AtomicU64>>,
) -> anyhow::Result<(
    Vec<HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>>,
    Vec<Option<HashMap<NodeId, BatchTransformFn>>>,
    Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedBatchOperator>)>>>,
    Vec<Option<HashMap<NodeId, OperatorContext>>>,
    Vec<HashMap<NodeId, flume::Sender<ErasedBuffer>>>,
    Vec<Option<HashMap<NodeId, KeyFn>>>,
    Vec<JoinHandle<anyhow::Result<()>>>,
)> {
    let rt = tokio::runtime::Handle::current();
    let mut per_worker_source_rx: Vec<
        HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>,
    > = (0..total_workers).map(|_| HashMap::new()).collect();
    let mut per_worker_batch_transforms: Vec<Option<HashMap<NodeId, BatchTransformFn>>> =
        (0..total_workers).map(|_| None).collect();
    let mut per_worker_batch_operators: Vec<
        Option<HashMap<NodeId, (String, Box<dyn ErasedBatchOperator>)>>,
    > = (0..total_workers).map(|_| None).collect();
    let mut per_worker_batch_contexts: Vec<Option<HashMap<NodeId, OperatorContext>>> =
        (0..total_workers).map(|_| None).collect();
    let mut per_worker_sink_senders: Vec<HashMap<NodeId, flume::Sender<ErasedBuffer>>> =
        (0..total_workers).map(|_| HashMap::new()).collect();
    let mut sink_handles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();

    // Collect batch node IDs by kind.
    let source_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::Source)
        .copied()
        .collect();
    let batch_transform_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::Transform)
        .copied()
        .collect();
    let batch_operator_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::BatchOperator)
        .copied()
        .collect();
    let batch_key_by_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::KeyBy)
        .copied()
        .collect();
    let batch_sink_ids: Vec<NodeId> = graph
        .topo_order
        .iter()
        .filter(|id| node_kinds[id] == NodeKindTag::Sink)
        .copied()
        .collect();

    // Bridge batch sources: compile, create channel, spawn bridge task.
    for &source_id in &source_ids {
        let source = extract_source(&mut graph.nodes[source_id.0]);

        if let Some(n_partitions) = source.partition_count() {
            // Partitioned source: distribute partitions across all workers.
            let partitions_per_worker = assign_partitions(n_partitions, total_workers);
            for worker_idx in local_range.clone() {
                let assigned = &partitions_per_worker[worker_idx];
                if assigned.is_empty() {
                    continue;
                }
                let Some(partition_source) = source.create_partition_source(assigned) else {
                    tracing::warn!(
                        worker = worker_idx,
                        "partitioned source returned None for assigned partitions"
                    );
                    continue;
                };
                let (tx, rx) = flume::bounded(crate::bridge::DEFAULT_CHANNEL_SIZE);
                let offsets: Arc<std::sync::Mutex<HashMap<String, String>>> =
                    Arc::new(std::sync::Mutex::new(HashMap::new()));
                let wm: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
                let shutdown_handle = shutdown.cloned();
                rt.spawn(crate::bridge::local_source_bridge(
                    partition_source,
                    tx,
                    offsets.clone(),
                    wm.clone(),
                    shutdown_handle,
                ));
                per_worker_source_rx[worker_idx].insert(source_id, rx);
                per_worker_source_wm[worker_idx].insert(source_id, wm.clone());
                per_worker_source_offsets[worker_idx].insert(source_id, offsets.clone());
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        } else {
            // Non-partitioned: only worker 0 reads.
            if local_range.contains(&0) {
                let (tx, rx) = flume::bounded(crate::bridge::DEFAULT_CHANNEL_SIZE);
                let offsets: Arc<std::sync::Mutex<HashMap<String, String>>> =
                    Arc::new(std::sync::Mutex::new(HashMap::new()));
                let wm: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
                let shutdown_handle = shutdown.cloned();
                rt.spawn(crate::bridge::local_source_bridge(
                    source,
                    tx,
                    offsets.clone(),
                    wm.clone(),
                    shutdown_handle,
                ));
                per_worker_source_rx[0].insert(source_id, rx);
                per_worker_source_wm[0].insert(source_id, wm.clone());
                per_worker_source_offsets[0].insert(source_id, offsets.clone());
                all_source_offsets.push(offsets);
                all_source_watermarks.push(wm);
            }
        }
    }

    // Extract batch transforms.
    let mut orig_batch_transforms: HashMap<NodeId, BatchTransformFn> = HashMap::new();
    for &nid in &batch_transform_ids {
        orig_batch_transforms.insert(nid, extract_batch_transform(&mut graph.nodes[nid.0]));
    }

    // Extract batch operators.
    let mut orig_batch_operators: HashMap<NodeId, (String, Box<dyn ErasedBatchOperator>)> =
        HashMap::new();
    for &nid in &batch_operator_ids {
        orig_batch_operators.insert(nid, extract_batch_operator(&mut graph.nodes[nid.0]));
    }

    // Extract batch key_by functions.
    let mut orig_key_fns: HashMap<NodeId, KeyFn> = HashMap::new();
    for &nid in &batch_key_by_ids {
        orig_key_fns.insert(nid, extract_key_by(&mut graph.nodes[nid.0]));
    }

    // Bridge batch sinks.
    for &sink_id in &batch_sink_ids {
        let sink = extract_batch_sink(&mut graph.nodes[sink_id.0]);
        let (tx, rx) = flume::bounded::<ErasedBuffer>(crate::bridge::DEFAULT_CHANNEL_SIZE);
        // All workers share the same sender (Pipeline pact).
        for idx in local_range.clone() {
            per_worker_sink_senders[idx].insert(sink_id, tx.clone());
        }
        sink_handles.push(tokio::spawn(crate::bridge::sink_drain(sink, rx)));
    }

    // Distribute per-worker copies for local workers.
    let mut per_worker_key_fns: Vec<Option<HashMap<NodeId, KeyFn>>> =
        (0..total_workers).map(|_| None).collect();

    for worker_idx in local_range.clone() {
        let mut w_transforms = HashMap::new();
        let mut w_operators = HashMap::new();
        let mut w_contexts = HashMap::new();
        let mut w_key_fns = HashMap::new();

        for &nid in &batch_transform_ids {
            w_transforms.insert(nid, orig_batch_transforms[&nid].clone());
        }

        for &nid in &batch_operator_ids {
            let (ref name, ref op) = orig_batch_operators[&nid];
            let (name, op) = (name.clone(), op.clone_erased());
            let ctx = controller.create_context_for_worker(&name, worker_idx)?;
            w_contexts.insert(nid, OperatorContext::new(ctx));
            w_operators.insert(nid, (name, op));
        }

        for &nid in &batch_key_by_ids {
            w_key_fns.insert(nid, orig_key_fns[&nid].clone());
        }

        per_worker_batch_transforms[worker_idx] = Some(w_transforms);
        per_worker_batch_operators[worker_idx] = Some(w_operators);
        per_worker_batch_contexts[worker_idx] = Some(w_contexts);
        per_worker_key_fns[worker_idx] = Some(w_key_fns);
    }

    Ok((
        per_worker_source_rx,
        per_worker_batch_transforms,
        per_worker_batch_operators,
        per_worker_batch_contexts,
        per_worker_sink_senders,
        per_worker_key_fns,
        sink_handles,
    ))
}

// ── Batch node extraction helpers ──────────────────────────────────

/// Create a lightweight placeholder `NodeKind` for `std::mem::replace`.
///
/// Uses a `Transform` with a no-op closure. The placeholder is never
/// compiled; it only satisfies the borrow checker during extraction.
fn placeholder_node_kind() -> NodeKind {
    NodeKind::Transform(LazyBatchTransformNode(Box::new(|| {
        Arc::new(|buf| vec![buf])
    })))
}

fn extract_source(node: &mut crate::dataflow::GraphNode) -> Box<dyn ErasedSource> {
    let kind = std::mem::replace(&mut node.kind, placeholder_node_kind());
    match kind {
        NodeKind::Source(src) => src.compile(),
        _ => panic!("expected Source node at {:?}", node.id),
    }
}

fn extract_batch_transform(node: &mut crate::dataflow::GraphNode) -> BatchTransformFn {
    let kind = std::mem::replace(&mut node.kind, placeholder_node_kind());
    match kind {
        NodeKind::Transform(f) => f.compile(),
        _ => panic!("expected Transform node at {:?}", node.id),
    }
}

fn extract_batch_operator(
    node: &mut crate::dataflow::GraphNode,
) -> (String, Box<dyn ErasedBatchOperator>) {
    let kind = std::mem::replace(&mut node.kind, placeholder_node_kind());
    match kind {
        NodeKind::BatchOperator { name, op } => (name, op.compile()),
        _ => panic!("expected BatchOperator node at {:?}", node.id),
    }
}

fn extract_batch_sink(node: &mut crate::dataflow::GraphNode) -> Box<dyn ErasedSink> {
    let kind = std::mem::replace(&mut node.kind, placeholder_node_kind());
    match kind {
        NodeKind::Sink(sink) => sink.compile(),
        _ => panic!("expected Sink node at {:?}", node.id),
    }
}

fn extract_key_by(node: &mut crate::dataflow::GraphNode) -> KeyFn {
    let kind = std::mem::replace(&mut node.kind, placeholder_node_kind());
    match kind {
        NodeKind::KeyBy(key_by) => key_by.compile(),
        _ => panic!("expected KeyBy node at {:?}", node.id),
    }
}

// ── Other helpers ───────────────────────────────────────────────────

/// Create per-worker DLQ sinks and bridge them via channels.
///
/// Returns a vector of per-worker senders (sized to `total_workers`, with `None`
/// for remote workers) and the async task handles that drive the sinks.
fn setup_dlq_sinks(
    policy: &ErrorPolicy,
    dlq_sink: Option<Box<dyn rhei_core::dlq::DlqSink>>,
    total_workers: usize,
    local_range: &Range<usize>,
) -> (Vec<Option<DlqSender>>, Vec<JoinHandle<anyhow::Result<()>>>) {
    match policy {
        ErrorPolicy::Skip => {
            let senders = vec![None; total_workers];
            (senders, Vec::new())
        }
        ErrorPolicy::SendToDlq => {
            let sink = dlq_sink.unwrap_or_else(|| {
                tracing::warn!("SendToDlq policy set but no DLQ sink provided, using log sink");
                Box::new(rhei_core::dlq::LogDlqSink)
            });
            let mut senders: Vec<Option<DlqSender>> = vec![None; total_workers];
            let mut handles = Vec::new();
            let (tx, rx) = flume::bounded::<rhei_core::dlq::DeadLetterRecord>(256);
            for idx in local_range.clone() {
                senders[idx] = Some(tx.clone());
            }
            drop(tx);
            handles.push(tokio::spawn(dlq_drain(rx, sink)));
            (senders, handles)
        }
    }
}

/// Drains DLQ records from the channel into the user-provided sink.
async fn dlq_drain(
    rx: flume::Receiver<rhei_core::dlq::DeadLetterRecord>,
    mut sink: Box<dyn rhei_core::dlq::DlqSink>,
) -> anyhow::Result<()> {
    while let Ok(record) = rx.recv_async().await {
        if let Err(e) = sink.write(record).await {
            tracing::warn!(error = %e, "DLQ sink write failed");
        }
    }
    sink.flush().await?;
    Ok(())
}

/// Assign partitions round-robin to workers.
///
/// Returns a `Vec<Vec<usize>>` of length `num_workers`, where each inner vec
/// contains the partition indices assigned to that worker.
fn assign_partitions(num_partitions: usize, num_workers: usize) -> Vec<Vec<usize>> {
    let mut assignments: Vec<Vec<usize>> = vec![vec![]; num_workers];
    for partition in 0..num_partitions {
        assignments[partition % num_workers].push(partition);
    }
    assignments
}

/// Merge offsets from all source bridges.
pub(crate) fn merge_source_offsets(
    all: &[Arc<std::sync::Mutex<HashMap<String, String>>>],
) -> HashMap<String, String> {
    let mut combined = HashMap::new();
    for offsets in all {
        combined.extend(
            offsets
                .lock()
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        "mutex poisoned in source_offsets merge lock, recovering inner data"
                    );
                    e.into_inner()
                })
                .clone(),
        );
    }
    combined
}
