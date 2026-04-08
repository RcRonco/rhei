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

use rhei_core::state::context::StateContext;
use timely::communication::Allocate;
use timely::dataflow::operators::probe;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

use crate::any_item::AnyItem;
use crate::dataflow::{NodeId, NodeKind};
use crate::erased::{ErasedOperator, TransformFn};
use crate::task_manager::{DlqSender, ExecutorData};
use crate::timely_operator::TimelyErasedOperator;

// Backward-compatible re-exports so `executor::Executor` still works.
#[doc(hidden)]
pub use crate::controller::PipelineController as Executor;
#[doc(hidden)]
pub use crate::controller::PipelineControllerBuilder as ExecutorBuilder;

/// Type alias for a Timely worker scope parameterized by allocator.
type Scope<'a, A> = Child<'a, Worker<A>, u64>;
type ScopedStream<'a, A, R> = timely::dataflow::Stream<Scope<'a, A>, Vec<R>>;
type ScopedAnyStream<'a, A> = ScopedStream<'a, A, AnyItem>;

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
pub(crate) enum NodeKindTag {
    Source,
    Transform,
    KeyBy,
    Operator,
    Merge,
    Sink,
}

impl NodeKindTag {
    pub(crate) fn from_kind(kind: &NodeKind) -> Self {
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

// ── DataflowExecutor ────────────────────────────────────────────────

/// Compiles and runs a Timely dataflow from compiled graph metadata.
///
/// Constructed per-worker via [`TaskManager::create_executor`](crate::task_manager::TaskManager::create_executor),
/// owns shared references to graph topology and per-worker configuration.
/// Each `build_*` method constructs one category of Timely operator.
pub(crate) struct DataflowExecutor {
    sink_senders: Arc<HashMap<NodeId, flume::Sender<AnyItem>>>,
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
        sink_senders: Arc<HashMap<NodeId, flume::Sender<AnyItem>>>,
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
        Self {
            sink_senders,
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
    /// Creates a per-worker `current_thread` Tokio runtime and bridges sources
    /// locally via `spawn_local`, co-locating source I/O with the Timely worker
    /// on the same core. The step loop alternates between `worker.step()` and
    /// ticking the local runtime to advance source tasks.
    ///
    /// When the worker thread already has a Tokio runtime context (e.g. when
    /// Timely runs worker 0 on the `spawn_blocking` thread), the shared runtime
    /// is used via `block_in_place` instead of creating a new one. The cold-path
    /// state fetches in `async_operator.rs` use `block_in_place(|| rt.block_on())`
    /// which is safe in either case.
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

        // Bridge sources on the shared Tokio runtime. Source bridges run
        // continuously and concurrently with the Timely step loop, matching
        // the pre-per-worker-runtime behavior. This is critical because the
        // out-of-band watermark (shared atomic) must stay roughly in sync
        // with the in-band data flowing through Timely exchanges — spawning
        // sources on a cooperative per-worker runtime makes the watermark
        // race ahead of the exchange, causing downstream operators to close
        // windows before all items arrive.
        //
        // TODO: once watermarks are propagated in-band (as special stream
        // elements), sources can be moved to a per-worker `current_thread`
        // runtime for core co-location.
        let mut source_rx: HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>> =
            HashMap::new();
        for (node_id, source) in data.sources.drain() {
            let (tx, rx) = flume::bounded(crate::bridge::DEFAULT_CHANNEL_SIZE);
            let offsets = data
                .source_offsets
                .remove(&node_id)
                .unwrap_or_else(|| Arc::new(std::sync::Mutex::new(HashMap::new())));
            let wm = data.source_wm[&node_id].clone();
            let shutdown = data.shutdown.clone();
            self.rt.spawn(crate::bridge::local_source_bridge(
                source, tx, offsets, wm, shutdown,
            ));
            source_rx.insert(node_id, rx);
        }

        let dataflow_index = worker.next_dataflow_index();
        let probe =
            worker.dataflow::<u64, _, _>(|scope| self.compile(scope, &mut data, &mut source_rx));

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
        source_rx: &mut HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>,
    ) -> probe::Handle<u64> {
        let mut streams: HashMap<NodeId, ScopedAnyStream<_>> = HashMap::new();
        let probe = probe::Handle::new();

        for &node_id in self.topo_order.iter() {
            let kind = &self.node_kinds[&node_id];
            let inputs = &self.node_inputs[&node_id];

            match kind {
                NodeKindTag::Source => {
                    let stream = self.build_source(scope, node_id, source_rx, &mut data.source_wm);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Transform => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream =
                        self.build_transform(scope, node_id, input_stream, &mut data.transforms);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::KeyBy => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream = Self::build_key_by(node_id, input_stream, &mut data.key_fns);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Operator => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream = self.build_operator(
                        node_id,
                        input_stream,
                        &mut data.operators,
                        &mut data.contexts,
                    );
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Merge => {
                    let stream = Self::build_merge(scope, inputs, &streams);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Sink => {
                    let input_stream = streams[&inputs[0]].clone();
                    self.build_sink(node_id, input_stream, &probe);
                }
            }
        }

        probe
    }

    /// Build a source operator with reschedule, capability management, and metrics.
    #[allow(clippy::too_many_lines)]
    fn build_source<'a, A: Allocate>(
        &self,
        scope: &mut Scope<'a, A>,
        node_id: NodeId,
        source_receivers: &mut HashMap<NodeId, flume::Receiver<crate::bridge::SourceBatch>>,
        source_watermarks: &mut HashMap<NodeId, Arc<AtomicU64>>,
    ) -> ScopedAnyStream<'a, A> {
        use timely::dataflow::operators::generic::OutputBuilder;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
        use timely::scheduling::Scheduler;

        let mut source_builder =
            OperatorBuilder::new(format!("Source_{}", node_id.0), scope.clone());
        let (output, stream) = source_builder.new_output::<Vec<AnyItem>>();
        let mut output = OutputBuilder::from(output);
        let activator = scope.activator_for(source_builder.operator_info().address);
        source_builder.set_notify(false);

        let source_rx = source_receivers.remove(&node_id);
        let worker_label = self.worker_index.to_string();
        let all_wms = self.all_source_watermarks.clone();
        let per_source_wm = source_watermarks.remove(&node_id);

        source_builder.build_reschedule(move |mut capabilities| {
            #[allow(clippy::expect_used)] // invariant: Timely always provides an initial capability
            let mut cap = Some(
                capabilities
                    .pop()
                    .expect("source operator should have initial capability"),
            );
            let mut epoch: u64 = 0;
            let mut rx = source_rx;
            let mut draining = false;

            move |_frontiers| {
                if cap.is_none() {
                    return false;
                }

                // Non-worker-0 gets None receiver → immediately close.
                let Some(ref mut source_rx) = rx else {
                    cap = None;
                    return false;
                };

                // Draining: source naturally exhausted, wait for global watermark
                // to reach SourceExhausted so downstream operators can close final
                // windows before we drop the capability.
                if draining {
                    if compute_min_watermark(&all_wms) >= Sentinel::SourceExhausted as u64 {
                        cap = None;
                        return false;
                    }
                    activator.activate();
                    return true;
                }

                match source_rx.try_recv() {
                    Ok((batch, wm)) => {
                        #[allow(clippy::cast_possible_truncation)]
                        let batch_len = batch.len() as u64;
                        if let Some(ref c) = cap {
                            let mut handle = output.activate();
                            let mut session = handle.session(c);
                            for item in batch {
                                session.give(item);
                            }
                        }
                        // Update the shared watermark for exhaustion tracking.
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
                        .increment(batch_len);
                        // Use the watermark as the epoch so the Timely frontier
                        // tracks event time. Downstream operators read the
                        // frontier (in-band) instead of the shared atomic
                        // (out-of-band), preventing the watermark from racing
                        // ahead of data in the exchange.
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
                        // Check if this source was naturally exhausted (bridge set
                        // SourceExhausted) vs shut down (watermark unchanged).
                        // Only drain on exhaustion — shutdown resumes from checkpoint.
                        let exhausted = per_source_wm.as_ref().is_some_and(|wm| {
                            wm.load(Ordering::Acquire) >= Sentinel::SourceExhausted as u64
                        });
                        if exhausted {
                            // Advance epoch to SourceExhausted so the downstream
                            // frontier reaches this value, allowing time-based
                            // operators to close all remaining windows.
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

    /// Build a transform (map/filter/flatmap) as a Pipeline unary operator.
    fn build_transform<'a, A: Allocate>(
        &self,
        _scope: &mut Scope<'a, A>,
        node_id: NodeId,
        input_stream: ScopedAnyStream<'a, A>,
        transforms: &mut HashMap<NodeId, TransformFn>,
    ) -> ScopedAnyStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)] // invariant: graph compilation guarantees transform exists
        let f = transforms
            .remove(&node_id)
            .expect("missing transform for node");
        let name = format!("Transform_{}", node_id.0);
        let worker_index = self.worker_index;
        let num_workers = self.num_workers;
        let ctx = crate::dataflow::TransformContext {
            worker_index,
            num_workers,
        };
        input_stream.unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
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
        )
    }

    /// Build a key-by exchange as an `ExchangePact` unary operator.
    fn build_key_by<'a, A: Allocate>(
        node_id: NodeId,
        input_stream: ScopedAnyStream<'a, A>,
        key_fns: &mut HashMap<NodeId, crate::erased::KeyFn>,
    ) -> ScopedAnyStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Exchange as ExchangePact;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)] // invariant: graph compilation guarantees key_fn exists
        let key_fn = key_fns.remove(&node_id).expect("missing key_fn for node");
        input_stream.unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
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
        )
    }

    /// Build a stateful operator with DLQ, watermark, timer, and checkpoint support.
    #[allow(clippy::too_many_lines)]
    fn build_operator<'a, A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ScopedAnyStream<'a, A>,
        operators: &mut HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
        operator_contexts: &mut HashMap<NodeId, StateContext>,
    ) -> ScopedAnyStream<'a, A> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Capability;
        use timely::dataflow::operators::generic::operator::Operator;

        #[allow(clippy::expect_used)] // invariant: graph compilation guarantees operator exists
        let (op_name, op) = operators
            .remove(&node_id)
            .expect("missing operator for node");
        #[allow(clippy::expect_used)] // invariant: graph compilation guarantees operator ctx exists
        let ctx = operator_contexts
            .remove(&node_id)
            .expect("missing operator ctx for node");
        let oc = OperatorCfg {
            rt: self.rt.clone(),
            worker_label: self.worker_index.to_string(),
            is_last_op: self.last_operator_id == Some(node_id),
            notify: self.checkpoint_notify.clone(),
            dlq: self.dlq_tx.clone(),
            worker_index: self.worker_index,
            local_first_worker: self.local_first_worker,
        };

        input_stream.unary_frontier::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
            Pipeline,
            &format!("Op_{}", node_id.0),
            move |_init_cap, _info| {
                let mut timely_op = TimelyErasedOperator::new(op, ctx);
                if let Err(e) = timely_op.open(&oc.rt) {
                    tracing::error!(
                        error = %e,
                        operator = %op_name,
                        "operator open failed"
                    );
                    metrics::counter!(
                        "operator_lifecycle_errors_total",
                        "phase" => "open"
                    )
                    .increment(1);
                }
                let mut last_watermark: u64 = 0;
                let mut retained_cap: Option<Capability<u64>> = None;
                let mut cap_retained_since: Option<std::time::Instant> = None;
                let mut closed = false;
                let cap_worker_label = oc.worker_label.clone();
                move |(input, frontier), output| {
                    let mut emit = |items: Vec<AnyItem>, cap: &Option<Capability<u64>>| {
                        if let Some(c) = cap
                            && !items.is_empty()
                        {
                            let mut s = output.session(c);
                            for r in items {
                                s.give(r);
                            }
                        }
                    };
                    input.for_each(|cap, data| {
                        let owned_cap = cap.retain(0);
                        let batch: Vec<AnyItem> = std::mem::take(data);
                        if batch.is_empty() {
                            return;
                        }
                        let t = std::time::Instant::now();
                        let (results, errors) = timely_op.process_batch(batch, &oc.rt);
                        record_batch_durations(&[t.elapsed().as_secs_f64()], &oc.worker_label);
                        for e in &errors {
                            route_errors_to_dlq(
                                std::slice::from_ref(e),
                                "batch",
                                &op_name,
                                oc.dlq.as_ref(),
                            );
                        }
                        emit(results, &Some(owned_cap.clone()));
                        if retained_cap.is_none() {
                            cap_retained_since = Some(std::time::Instant::now());
                        }
                        retained_cap = Some(owned_cap);
                    });
                    let wm = frontier_min_or_max(frontier.frontier());
                    let time_results = timely_op.advance_time(wm, &mut last_watermark, &oc.rt);
                    emit(time_results, &retained_cap);
                    if let Some(ref cap) = retained_cap
                        && !frontier.less_equal(cap.time())
                    {
                        if let Some(since) = cap_retained_since.take() {
                            let held_secs = since.elapsed().as_secs_f64();
                            metrics::gauge!(
                                "capability_held_duration_seconds",
                                "worker" => cap_worker_label.clone()
                            )
                            .set(held_secs);
                            if held_secs > 30.0 {
                                tracing::warn!(
                                    held_seconds = held_secs,
                                    epoch = cap.time(),
                                    "capability held for over 30s — \
                                     possible stall in operator processing"
                                );
                            }
                        }
                        retained_cap = None;
                    }
                    let fv: Vec<u64> = frontier.frontier().iter().copied().collect();
                    if let Some(epoch) = try_checkpoint(
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
                                "operator close failed"
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

    /// Build a merge node using Timely's concatenate.
    fn build_merge<'a, A: Allocate>(
        scope: &mut Scope<'a, A>,
        inputs: &[NodeId],
        streams: &HashMap<NodeId, ScopedAnyStream<'a, A>>,
    ) -> ScopedAnyStream<'a, A> {
        use timely::dataflow::operators::Concatenate;

        let input_streams: Vec<_> = inputs.iter().map(|id| streams[id].clone()).collect();
        scope.concatenate(input_streams)
    }

    /// Build a sink node that forwards items to an async mpsc channel.
    fn build_sink<A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: ScopedAnyStream<'_, A>,
        probe: &probe::Handle<u64>,
    ) {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::core::probe::Probe;
        use timely::dataflow::operators::generic::operator::Operator;

        let sink_tx = self.sink_senders[&node_id].clone();

        input_stream
            .unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                Pipeline,
                &format!("Sink_{}", node_id.0),
                move |_cap, _info| {
                    let sink_tx = sink_tx;
                    move |input, _output| {
                        input.for_each(|_cap, data| {
                            for item in data.drain(..) {
                                if let Err(e) = sink_tx.send(item) {
                                    tracing::error!(error = %e, "sink channel send failed — item dropped");
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

/// Route operator errors to the DLQ channel, logging and counting each.
fn route_errors_to_dlq(
    errors: &[anyhow::Error],
    input_repr: &str,
    op_name: &str,
    dlq: Option<&DlqSender>,
) {
    for e in errors {
        tracing::warn!(
            error = %e,
            operator = %op_name,
            "operator error — routing to DLQ"
        );
        metrics::counter!("dlq_items_total").increment(1);
        if let Some(dlq) = dlq {
            let record = rhei_core::dlq::DeadLetterRecord {
                input_repr: input_repr.to_owned(),
                operator_name: op_name.to_owned(),
                error: e.to_string(),
                timestamp: {
                    let d = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    format!("{}", d.as_secs())
                },
            };
            if let Err(e) = dlq.send(record) {
                tracing::error!(
                    error = %e,
                    operator = %op_name,
                    "DLQ send failed — record lost"
                );
                metrics::counter!("dlq_write_errors_total").increment(1);
            }
        }
    }
}

/// Record p50/p99 batch element durations as Prometheus gauges.
fn record_batch_durations(durations: &[f64], worker_label: &str) {
    if durations.is_empty() {
        return;
    }
    let mut sorted = durations.to_vec();
    sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = sorted.len();
    let p50 = sorted[len / 2];
    let p99 = sorted[(len * 99 / 100).min(len - 1)];
    metrics::gauge!("executor_element_duration_p50", "worker" => worker_label.to_owned()).set(p50);
    metrics::gauge!("executor_element_duration_p99", "worker" => worker_label.to_owned()).set(p99);
}

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

/// Run checkpoint on the first local worker of the last operator.
///
/// Returns `Some(epoch)` when the checkpoint fires, `None` otherwise.
/// Uses `local_first_worker` instead of hardcoded worker 0 so that
/// every process in a cluster sends checkpoint notifications.
fn try_checkpoint(
    timely_op: &mut TimelyErasedOperator,
    frontier_vec: &[u64],
    rt: &tokio::runtime::Handle,
    is_last_op: bool,
    worker_index: usize,
    local_first_worker: usize,
) -> Option<u64> {
    let epoch = match timely_op.maybe_checkpoint(frontier_vec, rt) {
        Ok(epoch) => epoch,
        Err(e) => {
            tracing::error!(error = %e, "checkpoint failed");
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
    use crate::any_item::AnyItem;

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

    #[test]
    fn anyitem_serde_roundtrip() {
        let item = AnyItem::new(42i32);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<i32>(), 42);
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

    /// Verify that flume sink channel works with blocking send (no Tokio context needed).
    #[test]
    fn flume_sink_send_no_tokio_context() {
        let (tx, rx) = flume::bounded::<AnyItem>(16);

        // Send without any Tokio runtime — this is how build_sink sends items.
        let item = AnyItem::new("test".to_string());
        tx.send(item).unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.downcast::<String>(), "test");
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
