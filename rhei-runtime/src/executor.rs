//! Pure DAG compilation and Timely execution engine.
//!
//! This module contains [`TimelyCompiler`] for building Timely dataflows from
//! compiled graphs, and the [`execute_dag`] entry point that launches Timely workers.
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

use crate::dataflow::{AnyItem, ErasedOperator, NodeId, NodeKind, TransformFn};
use crate::timely_operator::TimelyErasedOperator;
use crate::worker::{DlqSender, WorkerSet};

// Backward-compatible re-exports so `executor::Executor` still works.
#[doc(hidden)]
pub use crate::controller::PipelineController as Executor;
#[doc(hidden)]
pub use crate::controller::PipelineControllerBuilder as ExecutorBuilder;

/// Type alias for a Timely worker scope parameterized by allocator.
type Scope<'a, A> = Child<'a, Worker<A>, u64>;

// ── Key partitioning ────────────────────────────────────────────────

/// Deterministic key-to-worker assignment using `seahash`.
///
/// Uses a fixed, portable hash so the same key always maps to the same
/// worker index — even across Rust compiler versions and restarts.
#[allow(clippy::cast_possible_truncation)]
pub fn partition_key(key: &str, n_workers: usize) -> usize {
    (seahash::hash(key.as_bytes()) as usize) % n_workers
}

// ── DAG Execution ───────────────────────────────────────────────────

/// Launch Timely execution over a prepared [`WorkerSet`].
///
/// Spawns a blocking task, creates one Timely dataflow per worker,
/// and runs `worker.step()` until the probe signals completion.
pub(crate) async fn execute_dag(
    timely_config: timely::execute::Config,
    worker_set: &WorkerSet,
    rt: tokio::runtime::Handle,
    total_workers: usize,
) -> anyhow::Result<()> {
    let sink_senders = worker_set.sink_senders.clone();
    let topo_order = worker_set.topo_order.clone();
    let node_inputs = worker_set.node_inputs.clone();
    let node_kinds = worker_set.node_kinds.clone();
    let last_operator_id = worker_set.last_operator_id;
    let global_watermark = worker_set.global_watermark.clone();
    let checkpoint_notify_tx = worker_set.checkpoint_notify_tx.clone();

    // Clone Arc fields for per-worker take_worker_data inside the closure.
    let source_rx = worker_set.source_rx.clone();
    let transforms = worker_set.transforms.clone();
    let key_fns = worker_set.key_fns.clone();
    let operators = worker_set.operators.clone();
    let contexts = worker_set.contexts.clone();
    let dlq_tx = worker_set.dlq_tx.clone();

    tokio::task::spawn_blocking(move || {
        let guards = timely::execute::execute(timely_config, move |worker| {
            let idx = worker.index();
            let _span = tracing::info_span!("worker", worker = idx).entered();

            // Take per-worker data from shared Mutex vectors.
            let mut w_data = take_worker_data_from_arcs(
                idx,
                &source_rx,
                &transforms,
                &key_fns,
                &operators,
                &contexts,
                &dlq_tx,
            );

            let compiler = TimelyCompiler {
                sink_senders: sink_senders.clone(),
                topo_order: topo_order.clone(),
                node_inputs: node_inputs.clone(),
                node_kinds: node_kinds.clone(),
                rt: rt.clone(),
                worker_index: idx,
                num_workers: total_workers,
                checkpoint_notify: Some(checkpoint_notify_tx.clone()),
                dlq_tx: w_data.dlq_tx.take(),
                last_operator_id,
                global_watermark: global_watermark.clone(),
            };

            let dataflow_index = worker.next_dataflow_index();
            let probe = worker.dataflow::<u64, _, _>(|scope| {
                compiler.compile(
                    scope,
                    &mut w_data.source_rx,
                    &mut w_data.transforms,
                    &mut w_data.key_fns,
                    &mut w_data.operators,
                    &mut w_data.contexts,
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

    Ok(())
}

/// Inline helper to take per-worker data from Arc<Mutex<Vec<Option<...>>>>.
///
/// This avoids needing `WorkerSet` inside the `'static` Timely closure — we
/// only pass the individual Arc fields.
#[allow(clippy::type_complexity, clippy::option_option)]
fn take_worker_data_from_arcs(
    idx: usize,
    source_rx: &Arc<
        std::sync::Mutex<Vec<Option<HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>>>>,
    >,
    transforms: &Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, TransformFn>>>>>,
    key_fns: &Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, crate::dataflow::KeyFn>>>>>,
    operators: &Arc<
        std::sync::Mutex<Vec<Option<HashMap<NodeId, (String, Box<dyn ErasedOperator>)>>>>,
    >,
    contexts: &Arc<std::sync::Mutex<Vec<Option<HashMap<NodeId, StateContext>>>>>,
    dlq_tx: &Arc<std::sync::Mutex<Vec<Option<Option<DlqSender>>>>>,
) -> crate::worker::WorkerData {
    crate::worker::WorkerData {
        source_rx: source_rx.lock().unwrap()[idx].take().unwrap_or_default(),
        transforms: transforms.lock().unwrap()[idx].take().unwrap_or_default(),
        key_fns: key_fns.lock().unwrap()[idx].take().unwrap_or_default(),
        operators: operators.lock().unwrap()[idx].take().unwrap_or_default(),
        contexts: contexts.lock().unwrap()[idx].take().unwrap_or_default(),
        dlq_tx: dlq_tx.lock().unwrap()[idx].take().unwrap_or_default(),
    }
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

// ── TimelyCompiler ──────────────────────────────────────────────────

/// Compiles a Timely dataflow from compiled graph metadata.
///
/// Constructed per-worker inside the Timely closure, owns shared references
/// to graph topology and per-worker configuration. Each `build_*` method
/// constructs one category of Timely operator.
pub(crate) struct TimelyCompiler {
    sink_senders: Arc<HashMap<NodeId, tokio::sync::mpsc::Sender<AnyItem>>>,
    topo_order: Arc<Vec<NodeId>>,
    node_inputs: Arc<HashMap<NodeId, Vec<NodeId>>>,
    node_kinds: Arc<HashMap<NodeId, NodeKindTag>>,
    rt: tokio::runtime::Handle,
    worker_index: usize,
    num_workers: usize,
    checkpoint_notify: Option<tokio::sync::mpsc::Sender<u64>>,
    dlq_tx: Option<DlqSender>,
    last_operator_id: Option<NodeId>,
    global_watermark: Arc<AtomicU64>,
}

impl TimelyCompiler {
    /// Build the full Timely dataflow, dispatching to per-node builders.
    ///
    /// Iterates `topo_order`, matches each node kind to its builder method,
    /// and returns a probe handle for tracking completion.
    fn compile<A: Allocate>(
        &self,
        scope: &mut Scope<'_, A>,
        source_receivers: &mut HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>,
        transforms: &mut HashMap<NodeId, TransformFn>,
        key_fns: &mut HashMap<NodeId, crate::dataflow::KeyFn>,
        operators: &mut HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
        operator_contexts: &mut HashMap<NodeId, StateContext>,
    ) -> probe::Handle<u64> {
        let mut streams: HashMap<NodeId, timely::dataflow::Stream<_, AnyItem>> = HashMap::new();
        let probe = probe::Handle::new();

        for &node_id in self.topo_order.iter() {
            let kind = &self.node_kinds[&node_id];
            let inputs = &self.node_inputs[&node_id];

            match kind {
                NodeKindTag::Source => {
                    let stream = self.build_source(scope, node_id, source_receivers);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Transform => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream = self.build_transform(scope, node_id, input_stream, transforms);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::KeyBy => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream = Self::build_key_by(node_id, input_stream, key_fns);
                    streams.insert(node_id, stream);
                }
                NodeKindTag::Operator => {
                    let input_stream = streams[&inputs[0]].clone();
                    let stream =
                        self.build_operator(node_id, input_stream, operators, operator_contexts);
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
    fn build_source<'a, A: Allocate>(
        &self,
        scope: &mut Scope<'a, A>,
        node_id: NodeId,
        source_receivers: &mut HashMap<NodeId, tokio::sync::mpsc::Receiver<Vec<AnyItem>>>,
    ) -> timely::dataflow::Stream<Scope<'a, A>, AnyItem> {
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
                            "worker" => worker_label.clone()
                        )
                        .increment(1);
                        metrics::counter!(
                            "executor_elements_total",
                            "worker" => worker_label.clone()
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

        stream
    }

    /// Build a transform (map/filter/flatmap) as a Pipeline unary operator.
    fn build_transform<'a, A: Allocate>(
        &self,
        _scope: &mut Scope<'a, A>,
        node_id: NodeId,
        input_stream: timely::dataflow::Stream<Scope<'a, A>, AnyItem>,
        transforms: &mut HashMap<NodeId, TransformFn>,
    ) -> timely::dataflow::Stream<Scope<'a, A>, AnyItem> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::operator::Operator;

        let f = transforms.remove(&node_id).expect("missing transform");
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
        input_stream: timely::dataflow::Stream<Scope<'a, A>, AnyItem>,
        key_fns: &mut HashMap<NodeId, crate::dataflow::KeyFn>,
    ) -> timely::dataflow::Stream<Scope<'a, A>, AnyItem> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Exchange as ExchangePact;
        use timely::dataflow::operators::generic::operator::Operator;

        let key_fn = key_fns.remove(&node_id).expect("missing key_fn");
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

    /// Build a stateful operator with DLQ, watermark, and checkpoint support.
    fn build_operator<'a, A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: timely::dataflow::Stream<Scope<'a, A>, AnyItem>,
        operators: &mut HashMap<NodeId, (String, Box<dyn ErasedOperator>)>,
        operator_contexts: &mut HashMap<NodeId, StateContext>,
    ) -> timely::dataflow::Stream<Scope<'a, A>, AnyItem> {
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Capability;
        use timely::dataflow::operators::generic::operator::Operator;

        let (op_name, op) = operators.remove(&node_id).expect("missing operator");
        let ctx = operator_contexts
            .remove(&node_id)
            .expect("missing StateContext for operator");

        let stage_name = format!("Op_{}", node_id.0);
        let rt_op = self.rt.clone();
        let worker_label = self.worker_index.to_string();
        let is_last_op = self.last_operator_id == Some(node_id);
        let notify = self.checkpoint_notify.clone();
        let dlq = self.dlq_tx.clone();
        let gw = self.global_watermark.clone();
        let worker_index = self.worker_index;

        input_stream.unary_frontier::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
            Pipeline,
            &stage_name,
            move |_init_cap, _info| {
                let mut timely_op = TimelyErasedOperator::new(op, ctx);
                let rt = rt_op;
                let wl = worker_label;
                let mut last_watermark: u64 = 0;
                let mut retained_cap: Option<Capability<u64>> = None;
                move |(input, frontier), output| {
                    input.for_each(|cap, data| {
                        let owned_cap = cap.retain();
                        let mut batch_durations = Vec::new();
                        for item in data.drain(..) {
                            let input_repr = item.debug_repr();
                            let elem_start = std::time::Instant::now();
                            let (results, errors) = timely_op.process(item, &rt);
                            batch_durations.push(elem_start.elapsed().as_secs_f64());
                            route_errors_to_dlq(&errors, &input_repr, &op_name, dlq.as_ref());
                            if !results.is_empty() {
                                let mut session = output.session(&owned_cap);
                                for r in results {
                                    session.give(r);
                                }
                            }
                        }
                        retained_cap = Some(owned_cap);
                        record_batch_durations(&batch_durations, &wl);
                    });

                    let wm_results =
                        advance_watermark(&mut timely_op, &gw, &mut last_watermark, &rt);
                    if !wm_results.is_empty()
                        && let Some(ref cap) = retained_cap
                    {
                        let mut session = output.session(cap);
                        for r in wm_results {
                            session.give(r);
                        }
                    }

                    // Drop retained cap if frontier has advanced past it.
                    if let Some(ref cap) = retained_cap
                        && !frontier.less_equal(cap.time())
                    {
                        retained_cap = None;
                    }

                    let frontier_vec: Vec<u64> = frontier.frontier().iter().copied().collect();
                    if let Some(epoch) =
                        try_checkpoint(&mut timely_op, &frontier_vec, &rt, is_last_op, worker_index)
                        && let Some(ref n) = notify
                    {
                        let _ = n.blocking_send(epoch);
                    }
                }
            },
        )
    }

    /// Build a merge node using Timely's concatenate.
    fn build_merge<'a, A: Allocate>(
        scope: &mut Scope<'a, A>,
        inputs: &[NodeId],
        streams: &HashMap<NodeId, timely::dataflow::Stream<Scope<'a, A>, AnyItem>>,
    ) -> timely::dataflow::Stream<Scope<'a, A>, AnyItem> {
        use timely::dataflow::operators::Concatenate;

        let input_streams: Vec<_> = inputs.iter().map(|id| streams[id].clone()).collect();
        scope.concatenate(input_streams)
    }

    /// Build a sink node that forwards items to an async mpsc channel.
    fn build_sink<A: Allocate>(
        &self,
        node_id: NodeId,
        input_stream: timely::dataflow::Stream<Scope<'_, A>, AnyItem>,
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
                                if let Err(e) = sink_tx.blocking_send(item) {
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
            if let Err(e) = dlq.blocking_send(record) {
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

/// Check for watermark advancement, returning any outputs (e.g. closed windows).
fn advance_watermark(
    timely_op: &mut TimelyErasedOperator,
    gw: &AtomicU64,
    last_watermark: &mut u64,
    rt: &tokio::runtime::Handle,
) -> Vec<AnyItem> {
    let current_wm = gw.load(Ordering::Acquire);
    if current_wm > *last_watermark {
        *last_watermark = current_wm;
        timely_op.process_watermark(current_wm, rt)
    } else {
        Vec::new()
    }
}

/// Run checkpoint and return `Some(epoch)` when worker 0's last operator checkpoints.
fn try_checkpoint(
    timely_op: &mut TimelyErasedOperator,
    frontier_vec: &[u64],
    rt: &tokio::runtime::Handle,
    is_last_op: bool,
    worker_index: usize,
) -> Option<u64> {
    let epoch = timely_op.maybe_checkpoint(frontier_vec, rt);
    if is_last_op && worker_index == 0 {
        epoch
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::AnyItem;

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
}
