//! Pure DAG compilation and Timely execution engine.
//!
//! This module contains [`DataflowExecutor`] for building and running Timely dataflows
//! from compiled graphs.
//!
//! For pipeline configuration and lifecycle orchestration, see
//! [`controller::PipelineController`](crate::controller::PipelineController).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use timely::communication::Allocate;
use timely::dataflow::operators::probe;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

use crate::dataflow::{NodeId, NodeKind};
use crate::erased_buffer::BatchKeyFn;
use crate::task_manager::{DlqSender, ExecutorData};

// Backward-compatible re-exports so `executor::Executor` still works.
#[doc(hidden)]
pub use crate::controller::PipelineController as Executor;
#[doc(hidden)]
pub use crate::controller::PipelineControllerBuilder as ExecutorBuilder;

/// Type alias for a Timely worker scope parameterized by allocator.
type Scope<'a, A> = Child<'a, Worker<A>, u64>;
type ScopedStream<'a, A, R> = timely::dataflow::Stream<Scope<'a, A>, Vec<R>>;
type ErasedStream<'a, A> = ScopedStream<'a, A, crate::erased_buffer::ErasedBuffer>;

/// Special sentinel values in the `u64` timeline shared by watermarks and epochs.
///
/// These live at the top of the `u64` range, well above any real timestamp or epoch.
#[repr(u64)]
pub(crate) enum Sentinel {
    /// All data has arrived — sources set their watermark to this value on exhaustion.
    ///
    /// The global watermark task propagates this once every source bridge has exited.
    /// Downstream operators (e.g. `TumblingWindow`) use it to close pending windows.
    SourceExhausted = u64::MAX - 1,

    /// Shutdown coordination — sent through the checkpoint channel after `probe.done()`.
    ///
    /// In cluster mode, the checkpoint task coordinates with other processes before
    /// releasing the shutdown barrier, ensuring all processes tear down TCP simultaneously.
    Shutdown = u64::MAX,
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

// ── Node kind classification ────────────────────────────────────────

/// Lightweight tag for classifying graph nodes without moving data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // all variants are batch-only after row API removal
pub(crate) enum NodeKindTag {
    Source,
    BatchTransform,
    BatchOperator,
    Sink,
    BatchKeyBy,
    BatchMerge,
}

impl NodeKindTag {
    pub(crate) fn from_kind(kind: &NodeKind) -> Self {
        match kind {
            NodeKind::Source(_) => Self::Source,
            NodeKind::BatchTransform(_) => Self::BatchTransform,
            NodeKind::BatchOperator { .. } => Self::BatchOperator,
            NodeKind::Sink(_) => Self::Sink,
            NodeKind::BatchKeyBy(_) => Self::BatchKeyBy,
            NodeKind::BatchMerge => Self::BatchMerge,
        }
    }
}

// ── DataflowExecutor ────────────────────────────────────────────────

/// Compiles and runs a Timely dataflow from compiled graph metadata.
///
/// Constructed per-worker via [`TaskManager::create_executor`](crate::task_manager::TaskManager::create_executor),
/// owns shared references to graph topology and per-worker configuration.
/// Each `build_*` method constructs one category of Timely operator.
#[allow(dead_code)] // some fields are reserved for future use
pub(crate) struct DataflowExecutor {
    batch_sink_senders: Arc<HashMap<NodeId, flume::Sender<crate::erased_buffer::ErasedBuffer>>>,
    topo_order: Arc<Vec<NodeId>>,
    node_inputs: Arc<HashMap<NodeId, Vec<NodeId>>>,
    node_kinds: Arc<HashMap<NodeId, NodeKindTag>>,
    rt: tokio::runtime::Handle,
    worker_index: usize,
    num_workers: usize,
    checkpoint_notify: Option<flume::Sender<u64>>,
    dlq_tx: Option<DlqSender>,
    last_operator_id: Option<NodeId>,
    all_source_watermarks: Arc<Vec<Arc<AtomicU64>>>,
    /// First worker index on this process (used for checkpoint notifications).
    local_first_worker: usize,
    /// Owned per-worker data for this executor (taken once during `run()`).
    data: Option<ExecutorData>,
    /// Shutdown barrier for coordinated process teardown (cluster mode only).
    shutdown_barrier: Option<Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>>,
}

impl DataflowExecutor {
    /// Create a new `DataflowExecutor` with all required fields.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        topo_order: Arc<Vec<NodeId>>,
        node_inputs: Arc<HashMap<NodeId, Vec<NodeId>>>,
        node_kinds: Arc<HashMap<NodeId, NodeKindTag>>,
        rt: tokio::runtime::Handle,
        worker_index: usize,
        num_workers: usize,
        checkpoint_notify: Option<flume::Sender<u64>>,
        dlq_tx: Option<DlqSender>,
        last_operator_id: Option<NodeId>,
        all_source_watermarks: Arc<Vec<Arc<AtomicU64>>>,
        local_first_worker: usize,
        data: ExecutorData,
        shutdown_barrier: Option<Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>>,
    ) -> Self {
        let batch_sink_senders: HashMap<NodeId, flume::Sender<crate::erased_buffer::ErasedBuffer>> =
            data.batch_sink_senders.clone();
        Self {
            batch_sink_senders: Arc::new(batch_sink_senders),
            topo_order,
            node_inputs,
            node_kinds,
            rt,
            worker_index,
            num_workers,
            checkpoint_notify,
            dlq_tx,
            last_operator_id,
            all_source_watermarks,
            local_first_worker,
            data: Some(data),
            shutdown_barrier,
        }
    }

    /// Run the Timely dataflow: compile, step until done, then coordinate shutdown.
    ///
    /// Pins the worker thread to a CPU core, compiles the batch dataflow graph,
    /// and steps the Timely worker until the probe signals completion. On
    /// shutdown, the first local worker coordinates with other processes via the
    /// shutdown barrier (cluster mode) to ensure simultaneous teardown.
    pub(crate) fn run<A: Allocate>(mut self, worker: &mut Worker<A>) {
        let _span = tracing::info_span!("worker", worker = self.worker_index).entered();

        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if !core_ids.is_empty() {
            let core = core_ids[self.worker_index % core_ids.len()];
            if core_affinity::set_for_current(core) {
                tracing::info!(worker = self.worker_index, core = core.id, "pinned to core");
            }
        }

        #[allow(clippy::expect_used)] // invariant: run() is called exactly once
        let mut data = self.data.take().expect("executor data already taken");

        let dataflow_index = worker.next_dataflow_index();
        let probe = worker.dataflow::<u64, _, _>(|scope| self.compile(scope, &mut data));

        while !probe.done() {
            worker.step();
        }

        // Coordinated shutdown barrier: the first local worker on each
        // process signals readiness and waits for all processes to be
        // ready before returning. This ensures WorkerGuards/CommsGuard
        // drop simultaneously across processes, preventing TCP teardown
        // panics from broken pipes.
        if self.worker_index == self.local_first_worker {
            if let Some(ref n) = self.checkpoint_notify {
                let _ = n.send(Sentinel::Shutdown as u64);
            }
            if let Some(ref barrier) = self.shutdown_barrier
                && let Some(rx) = barrier
                    .lock()
                    .unwrap_or_else(|e| {
                        tracing::warn!("shutdown barrier mutex poisoned, recovering: {e}");
                        e.into_inner()
                    })
                    .take()
            {
                tracing::debug!("worker {} waiting on shutdown barrier", self.worker_index);
                let _ = rx.recv();
                tracing::debug!("worker {} shutdown barrier released", self.worker_index);
            }
        }

        worker.drop_dataflow(dataflow_index);
    }

    /// Build the full Timely dataflow, dispatching to per-node builders.
    ///
    /// Iterates `topo_order`, matches each node kind to its builder method,
    /// and returns a probe handle for tracking completion.
    fn compile<A: Allocate>(
        &self,
        scope: &mut Scope<'_, A>,
        data: &mut ExecutorData,
    ) -> probe::Handle<u64> {
        let mut batch_streams: HashMap<NodeId, ErasedStream<_>> = HashMap::new();
        let probe = probe::Handle::new();

        for &node_id in self.topo_order.iter() {
            let kind = &self.node_kinds[&node_id];
            let inputs = &self.node_inputs[&node_id];

            match kind {
                NodeKindTag::Source => {
                    let stream = self.build_batch_source(
                        scope,
                        node_id,
                        &mut data.batch_source_rx,
                        &mut data.source_wm,
                    );
                    batch_streams.insert(node_id, stream);
                }
                NodeKindTag::BatchTransform => {
                    let input_stream = batch_streams[&inputs[0]].clone();
                    let stream = self.build_batch_transform(
                        node_id,
                        input_stream,
                        &mut data.batch_transforms,
                    );
                    batch_streams.insert(node_id, stream);
                }
                NodeKindTag::BatchOperator => {
                    let input_stream = batch_streams[&inputs[0]].clone();
                    let stream = self.build_batch_operator(
                        node_id,
                        input_stream,
                        &mut data.batch_operators,
                        &mut data.batch_contexts,
                    );
                    batch_streams.insert(node_id, stream);
                }
                NodeKindTag::BatchKeyBy => {
                    let input_stream = batch_streams[&inputs[0]].clone();
                    let stream =
                        self.build_batch_key_by(node_id, input_stream, &mut data.batch_key_fns);
                    batch_streams.insert(node_id, stream);
                }
                NodeKindTag::BatchMerge => {
                    let input_streams: Vec<_> =
                        inputs.iter().map(|id| batch_streams[id].clone()).collect();
                    let stream = self.build_batch_merge(scope, input_streams);
                    batch_streams.insert(node_id, stream);
                }
                NodeKindTag::Sink => {
                    let input_stream = batch_streams[&inputs[0]].clone();
                    self.build_batch_sink(node_id, input_stream, &probe);
                }
            }
        }

        probe
    }

    // ── Batch (Arrow) build methods ─────────────────────────────────

    /// Build a batch source operator that reads `ErasedBuffer` from a flume channel.
    fn build_batch_source<'a, A: Allocate>(
        &self,
        scope: &mut Scope<'a, A>,
        node_id: NodeId,
        batch_source_rx: &mut HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>,
        source_watermarks: &mut HashMap<NodeId, Arc<AtomicU64>>,
    ) -> ErasedStream<'a, A> {
        use timely::dataflow::operators::generic::OutputBuilder;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
        use timely::scheduling::Scheduler;

        let mut source_builder =
            OperatorBuilder::new(format!("Source_{}", node_id.0), scope.clone());
        let (output, stream) =
            source_builder.new_output::<Vec<crate::erased_buffer::ErasedBuffer>>();
        let mut output = OutputBuilder::from(output);
        let activator = scope.activator_for(source_builder.operator_info().address);
        source_builder.set_notify(false);

        let rx = batch_source_rx.remove(&node_id);
        let worker_label = self.worker_index.to_string();
        let all_wms = self.all_source_watermarks.clone();
        let per_source_wm = source_watermarks.remove(&node_id);

        source_builder.build_reschedule(move |mut capabilities| {
            #[allow(clippy::expect_used)]
            let mut cap = Some(
                capabilities
                    .pop()
                    .expect("batch source operator should have initial capability"),
            );
            let mut epoch: u64 = 0;
            let mut source_rx = rx;
            let mut draining = false;

            move |_frontiers| {
                if cap.is_none() {
                    return false;
                }

                let Some(ref mut rx) = source_rx else {
                    cap = None;
                    return false;
                };

                if draining {
                    if compute_min_watermark(&all_wms) >= Sentinel::SourceExhausted as u64 {
                        cap = None;
                        return false;
                    }
                    activator.activate();
                    return true;
                }

                match rx.try_recv() {
                    Ok((buf, wm)) => {
                        #[allow(clippy::cast_possible_truncation)]
                        let rows = buf.num_rows() as u64;
                        if let Some(ref c) = cap {
                            let mut handle = output.activate();
                            let mut session = handle.session(c);
                            session.give(buf);
                        }
                        if let Some(wm) = wm
                            && let Some(ref wm_atomic) = per_source_wm
                        {
                            wm_atomic.fetch_max(wm, Ordering::Release);
                        }
                        metrics::counter!(
                            "executor_batches_total",
                            "worker" => worker_label.clone()
                        )
                        .increment(1);
                        metrics::counter!(
                            "executor_elements_total",
                            "worker" => worker_label.clone()
                        )
                        .increment(rows);
                        if let Some(wm) = wm {
                            epoch = epoch.max(wm);
                        } else {
                            epoch += 1;
                        }
                        if let Some(ref mut c) = cap {
                            c.downgrade(&epoch);
                        }
                        activator.activate();
                        true
                    }
                    Err(flume::TryRecvError::Empty) => {
                        activator.activate();
                        true
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        let exhausted = per_source_wm.as_ref().is_some_and(|wm| {
                            wm.load(Ordering::Acquire) >= Sentinel::SourceExhausted as u64
                        });
                        if exhausted {
                            epoch = epoch.max(Sentinel::SourceExhausted as u64);
                            if let Some(ref mut c) = cap {
                                c.downgrade(&epoch);
                            }
                            draining = true;
                            activator.activate();
                            true
                        } else {
                            cap = None;
                            false
                        }
                    }
                }
            }
        });

        stream
    }

    /// Build a batch transform (stateless `map`/`filter`/`flat_map`).
    #[allow(clippy::unused_self)]
    fn build_batch_transform<'a, A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ErasedStream<'a, A>,
        batch_transforms: &mut HashMap<NodeId, crate::dataflow::BatchTransformFn>,
    ) -> ErasedStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)]
        let f = batch_transforms
            .remove(&node_id)
            .expect("missing batch transform for node");
        let name = format!("BatchTransform_{}", node_id.0);
        input_stream
            .unary::<CapacityContainerBuilder<Vec<crate::erased_buffer::ErasedBuffer>>, _, _, _>(
                Pipeline,
                &name,
                move |_cap, _info| {
                    let f = f;
                    move |input, output| {
                        input.for_each(|cap, data| {
                            let mut session = output.session(&cap);
                            for buf in data.drain(..) {
                                for result_buf in f(buf) {
                                    session.give(result_buf);
                                }
                            }
                        });
                    }
                },
            )
    }

    /// Build a `key_by` exchange: split rows by key hash, then route to target workers.
    fn build_batch_key_by<'a, A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ErasedStream<'a, A>,
        batch_key_fns: &mut HashMap<NodeId, BatchKeyFn>,
    ) -> ErasedStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Exchange as _;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)]
        let key_fn = batch_key_fns
            .remove(&node_id)
            .expect("missing batch key_fn for node");
        let num_workers = self.num_workers;

        // Stage 1: Pipeline pact — split each buffer into per-worker sub-buffers.
        let partitioned = input_stream.unary::<CapacityContainerBuilder<
            Vec<crate::erased_buffer::ErasedBuffer>,
        >, _, _, _>(
            Pipeline,
            &format!("KeyBy_Split_{}", node_id.0),
            move |_cap, _info| {
                let key_fn = key_fn;
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut session = output.session(&cap);
                        for buf in data.drain(..) {
                            for sub_buf in buf.partition_for_exchange(&key_fn, num_workers) {
                                session.give(sub_buf);
                            }
                        }
                    });
                }
            },
        );

        // Stage 2: Exchange pact — route each sub-buffer to its target worker.
        partitioned
            .exchange(|buf: &crate::erased_buffer::ErasedBuffer| buf.exchange_target().unwrap_or(0))
    }

    /// Build a merge node: concatenates multiple input streams into one.
    #[allow(clippy::unused_self)]
    fn build_batch_merge<'a, A: Allocate>(
        &self,
        scope: &mut Scope<'a, A>,
        input_streams: Vec<ErasedStream<'a, A>>,
    ) -> ErasedStream<'a, A> {
        use timely::dataflow::operators::Concatenate;
        scope.concatenate(input_streams)
    }

    /// Build a batch stateful operator with watermark, timer, and checkpoint support.
    #[allow(clippy::too_many_lines)]
    fn build_batch_operator<'a, A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ErasedStream<'a, A>,
        batch_operators: &mut HashMap<
            NodeId,
            (String, Box<dyn crate::erased_batch::ErasedBatchOperator>),
        >,
        batch_contexts: &mut HashMap<NodeId, rhei_core::arrow::OperatorContext>,
    ) -> ErasedStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Capability;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)]
        let (op_name, op) = batch_operators
            .remove(&node_id)
            .expect("missing batch operator for node");
        #[allow(clippy::expect_used)]
        let ctx = batch_contexts
            .remove(&node_id)
            .expect("missing batch operator ctx for node");

        let oc = OperatorCfg {
            rt: self.rt.clone(),
            worker_label: self.worker_index.to_string(),
            is_last_op: self.last_operator_id == Some(node_id),
            notify: self.checkpoint_notify.clone(),
            dlq: self.dlq_tx.clone(),
            worker_index: self.worker_index,
            local_first_worker: self.local_first_worker,
        };

        input_stream.unary_frontier::<CapacityContainerBuilder<Vec<crate::erased_buffer::ErasedBuffer>>, _, _, _>(
            Pipeline,
            &format!("BatchOp_{}", node_id.0),
            move |_init_cap, _info| {
                let mut timely_op =
                    crate::timely_operator::TimelyBatchOperator::new(op, ctx);
                if let Err(e) = timely_op.open(&oc.rt) {
                    tracing::error!(
                        error = %e,
                        operator = %op_name,
                        "batch operator open failed"
                    );
                    metrics::counter!(
                        "operator_lifecycle_errors_total",
                        "phase" => "open"
                    )
                    .increment(1);
                }
                let mut last_watermark: u64 = 0;
                let mut retained_cap: Option<Capability<u64>> = None;
                let mut closed = false;
                move |(input, frontier), output| {
                    let mut emit = |bufs: Vec<crate::erased_buffer::ErasedBuffer>,
                                    cap: &Option<Capability<u64>>| {
                        if let Some(c) = cap
                            && !bufs.is_empty()
                        {
                            let mut s = output.session(c);
                            for b in bufs {
                                s.give(b);
                            }
                        }
                    };
                    input.for_each(|cap, data| {
                        let owned_cap = cap.retain(0);
                        let buffers: Vec<crate::erased_buffer::ErasedBuffer> =
                            std::mem::take(data);
                        for buf in buffers {
                            let (results, errors) = timely_op.process(buf, &oc.rt);
                            for e in &errors {
                                tracing::warn!(
                                    error = %e,
                                    operator = %op_name,
                                    "batch operator error"
                                );
                                metrics::counter!("dlq_items_total").increment(1);
                                if let Some(ref dlq) = oc.dlq {
                                    let record = rhei_core::dlq::DeadLetterRecord {
                                        input_repr: String::new(),
                                        operator_name: op_name.clone(),
                                        error: e.to_string(),
                                        timestamp: format!(
                                            "{}",
                                            std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis()
                                        ),
                                    };
                                    let _ = dlq.try_send(record);
                                }
                            }
                            emit(results, &Some(owned_cap.clone()));
                        }
                        retained_cap = Some(owned_cap);
                    });
                    let wm = frontier_min_or_max(frontier.frontier());
                    let time_results =
                        timely_op.advance_time(wm, &mut last_watermark, &oc.rt);
                    emit(time_results, &retained_cap);
                    if let Some(ref cap) = retained_cap
                        && !frontier.less_equal(cap.time())
                    {
                        retained_cap = None;
                    }
                    let fv: Vec<u64> = frontier.frontier().iter().copied().collect();
                    if let Some(epoch) = try_batch_checkpoint(
                        &mut timely_op,
                        &fv,
                        &oc.rt,
                        oc.is_last_op,
                        oc.worker_index,
                        oc.local_first_worker,
                    ) && let Some(ref n) = oc.notify
                    {
                        let _ = n.send(epoch);
                    }
                    if frontier.frontier().is_empty() && !closed {
                        if let Err(e) = timely_op.close(&oc.rt) {
                            tracing::error!(
                                error = %e,
                                operator = %op_name,
                                "batch operator close failed"
                            );
                            metrics::counter!(
                                "operator_lifecycle_errors_total",
                                "phase" => "close"
                            )
                            .increment(1);
                        }
                        closed = true;
                    }
                }
            },
        )
    }

    /// Build a batch sink node that forwards `ErasedBuffer` batches to an async channel.
    fn build_batch_sink<A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ErasedStream<'_, A>,
        probe: &probe::Handle<u64>,
    ) {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::core::probe::Probe;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)]
        let sink_tx = self
            .batch_sink_senders
            .get(&node_id)
            .expect("missing batch sink sender")
            .clone();

        input_stream
            .unary::<CapacityContainerBuilder<Vec<crate::erased_buffer::ErasedBuffer>>, _, _, _>(
                Pipeline,
                &format!("Sink_{}", node_id.0),
                move |_cap, _info| {
                    let sink_tx = sink_tx;
                    move |input, _output| {
                        input.for_each(|_cap, data| {
                            for buf in data.drain(..) {
                                if let Err(e) = sink_tx.send(buf) {
                                    tracing::error!(error = %e, "batch sink send failed");
                                    metrics::counter!("sink_send_errors_total").increment(1);
                                }
                            }
                        });
                    }
                },
            )
            .probe_with(probe);
    }
}

// ── Operator helper types ───────────────────────────────────────────

/// Bundles per-operator configuration extracted from `DataflowExecutor`
/// to reduce the number of variables captured by the Timely closure.
#[allow(dead_code)] // some fields are reserved for future use
struct OperatorCfg {
    rt: tokio::runtime::Handle,
    worker_label: String,
    is_last_op: bool,
    notify: Option<flume::Sender<u64>>,
    dlq: Option<DlqSender>,
    worker_index: usize,
    local_first_worker: usize,
}

// ── Operator helper functions ───────────────────────────────────────

/// Compute the minimum of all non-zero source watermarks.
///
/// Used by the source operator's draining logic to detect when all sources
/// have been exhausted (all watermarks >= `SourceExhausted`). Downstream
/// operators use the Timely frontier for watermark advancement instead.
fn compute_min_watermark(all: &[Arc<AtomicU64>]) -> u64 {
    let mut min_wm: Option<u64> = None;
    for wm in all {
        let v = wm.load(Ordering::Acquire);
        if v > 0 {
            min_wm = Some(min_wm.map_or(v, |m: u64| m.min(v)));
        }
    }
    min_wm.unwrap_or(0)
}

/// Run checkpoint on the first local worker of the last batch operator.
///
/// Returns `Some(epoch)` when the checkpoint fires, `None` otherwise.
/// Uses `local_first_worker` instead of hardcoded worker 0 so that
/// every process in a cluster sends checkpoint notifications.
fn try_batch_checkpoint(
    timely_op: &mut crate::timely_operator::TimelyBatchOperator,
    frontier_vec: &[u64],
    rt: &tokio::runtime::Handle,
    is_last_op: bool,
    worker_index: usize,
    local_first_worker: usize,
) -> Option<u64> {
    let epoch = match timely_op.maybe_checkpoint(frontier_vec, rt) {
        Ok(epoch) => epoch,
        Err(e) => {
            tracing::error!(error = %e, "batch checkpoint failed");
            metrics::counter!(
                "operator_lifecycle_errors_total",
                "phase" => "checkpoint"
            )
            .increment(1);
            None
        }
    };
    if is_last_op && worker_index == local_first_worker {
        epoch
    } else {
        None
    }
}

/// Compute the minimum frontier timestamp, or `u64::MAX` when the frontier is empty.
fn frontier_min_or_max(frontier: timely::progress::frontier::AntichainRef<'_, u64>) -> u64 {
    if frontier.is_empty() {
        u64::MAX
    } else {
        frontier.iter().copied().min().unwrap_or(0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, deprecated)]
mod tests {
    #[test]
    fn partition_key_deterministic() {
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

    /// Verify that flume checkpoint channel works synchronously (no .await needed).
    #[test]
    fn flume_checkpoint_channel_sync() {
        let (tx, rx) = flume::bounded::<u64>(64);

        // Send synchronously — this is how the Timely worker thread sends.
        tx.send(42).unwrap();
        tx.send(100).unwrap();

        // Receive synchronously.
        assert_eq!(rx.try_recv().unwrap(), 42);
        assert_eq!(rx.try_recv().unwrap(), 100);
        assert!(rx.try_recv().is_err());
    }

    /// Verify that the local runtime detection works: on a Tokio-owned thread,
    /// `Handle::try_current()` returns Ok, so we skip creating a new runtime.
    #[tokio::test]
    async fn local_runtime_fallback_on_tokio_thread() {
        // We're inside a #[tokio::test], so try_current should succeed.
        assert!(
            tokio::runtime::Handle::try_current().is_ok(),
            "should detect existing Tokio runtime"
        );
    }

    /// Verify that on a plain thread, we can create a new `current_thread` runtime.
    #[test]
    fn local_runtime_created_on_plain_thread() {
        let handle = std::thread::spawn(|| {
            // No Tokio runtime on this thread.
            assert!(tokio::runtime::Handle::try_current().is_err());

            // Should be able to create a current_thread runtime.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("should create runtime on plain thread");

            rt.block_on(async { 42 })
        })
        .join()
        .unwrap();

        assert_eq!(handle, 42);
    }
}
