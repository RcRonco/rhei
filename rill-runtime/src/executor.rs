//! Pipeline executor with Timely-backed multi-worker support.
//!
//! [`Executor`] materializes a [`DataflowGraph`](crate::dataflow::DataflowGraph)
//! into executable pipelines. Single-worker and multi-worker execution paths
//! both use Timely Dataflow under the hood, providing frontier tracking and
//! epoch-based checkpointing.
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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::state::prefixed_backend::PrefixedBackend;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::{TieredBackend, TieredBackendConfig};

use crate::compiler::{
    CompiledPipeline, Segment, clone_segments, compile, split_at_first_exchange,
};
use crate::dataflow::{
    DataflowGraph, ErasedOperator, ErasedSink, ErasedSource, KeyFn, TransformFn,
};
use crate::shutdown::ShutdownHandle;
use crate::timely_operator::TimelyErasedOperator;

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
}

/// Builder for [`Executor`].
#[derive(Debug)]
pub struct ExecutorBuilder {
    checkpoint_dir: std::path::PathBuf,
    workers: usize,
    tiered: Option<TieredStorageConfig>,
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

    /// Build the executor.
    pub fn build(self) -> Executor {
        Executor {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
        }
    }
}

impl Executor {
    /// Create a builder for configuring an executor.
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder {
            checkpoint_dir: std::path::PathBuf::from("./checkpoints"),
            workers: 1,
            tiered: None,
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
        }
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

// ── Execution engine ────────────────────────────────────────────────

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
    executor: &Executor,
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

/// Wrapper around `Box<dyn Any + Send>` that satisfies Timely's `Clone` requirement.
///
/// Timely's `Container` trait requires `Clone` on the element type. With `Pipeline`
/// pact and a linear (non-fan-out) dataflow, containers are never actually cloned —
/// this bound exists for `Exchange` pact compatibility. The `Clone` impl panics if
/// called, serving as a canary for accidental use with fan-out or Exchange pact.
struct AnyItem(Box<dyn Any + Send>);

impl Clone for AnyItem {
    fn clone(&self) -> Self {
        unreachable!("AnyItem::clone should never be called with Pipeline pact")
    }
}

/// Build a Timely dataflow from compiled segments inside a worker scope.
///
/// Constructs: source operator → chain of transform/operator stages → sink terminal.
/// Returns a probe for tracking completion.
///
/// - Transform segments become `unary` operators with Pipeline pact
/// - Operator segments become `unary_frontier` operators wrapping [`TimelyErasedOperator`]
/// - The source reads batches from an `mpsc::Receiver` (one batch = one epoch)
/// - The sink sends items via an `mpsc::Sender`
#[allow(clippy::too_many_lines)]
fn build_timely_dataflow<A: timely::communication::Allocate>(
    scope: &mut timely::dataflow::scopes::Child<'_, timely::worker::Worker<A>, u64>,
    source_rx: &mut tokio::sync::mpsc::Receiver<Vec<Box<dyn Any + Send>>>,
    segments: Vec<Segment>,
    operator_contexts: &mut [Option<StateContext>],
    sink_tx: tokio::sync::mpsc::Sender<Box<dyn Any + Send>>,
    rt: tokio::runtime::Handle,
) -> timely::dataflow::operators::probe::Handle<u64> {
    use timely::container::CapacityContainerBuilder;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::core::probe::Probe;
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::generic::operator::Operator;
    use timely::scheduling::Scheduler;

    // --- Source operator: drains channel, emits with capability per batch ---
    let mut source_builder = OperatorBuilder::new("Source".to_owned(), scope.clone());
    let (output, stream) = source_builder.new_output::<Vec<AnyItem>>();
    let mut output = OutputBuilder::from(output);
    let activator = scope.activator_for(source_builder.operator_info().address);
    source_builder.set_notify(false);

    // Move source_rx into the closure by taking from the mutable ref.
    let mut source_rx_owned = {
        let (dummy_tx, dummy_rx) = tokio::sync::mpsc::channel(1);
        drop(dummy_tx);
        std::mem::replace(source_rx, dummy_rx)
    };

    source_builder.build_reschedule(move |mut capabilities| {
        let mut cap = Some(capabilities.pop().unwrap());
        let mut epoch: u64 = 0;

        move |_frontiers| {
            if cap.is_none() {
                return false;
            }

            match source_rx_owned.try_recv() {
                Ok(batch) => {
                    #[allow(clippy::cast_possible_truncation)]
                    let batch_len = batch.len() as u64;
                    if let Some(ref c) = cap {
                        let mut handle = output.activate();
                        let mut session = handle.session(c);
                        for item in batch {
                            session.give(AnyItem(item));
                        }
                    }
                    metrics::counter!("executor_batches_total").increment(1);
                    metrics::counter!("executor_elements_total").increment(batch_len);
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

    // --- Chain segments as sequential Timely stages ---
    let mut current: timely::dataflow::Stream<_, AnyItem> = stream;
    let mut ctx_idx = 0;

    for (i, segment) in segments.into_iter().enumerate() {
        match segment {
            Segment::Transform(f) => {
                let name = format!("Transform_{i}");
                current = current.unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                    Pipeline,
                    &name,
                    move |_cap, _info| {
                        let f = f;
                        move |input, output| {
                            input.for_each(|cap, data| {
                                let mut session = output.session(&cap);
                                for item in data.drain(..) {
                                    for result in f(item.0) {
                                        session.give(AnyItem(result));
                                    }
                                }
                            });
                        }
                    },
                );
            }
            Segment::Operator { name, op } => {
                let stage_name = format!("Op_{i}_{name}");
                let ctx = operator_contexts[ctx_idx]
                    .take()
                    .expect("missing StateContext for operator");
                ctx_idx += 1;

                let rt_op = rt.clone();
                current = current
                    .unary_frontier::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
                        Pipeline,
                        &stage_name,
                        move |_init_cap, _info| {
                            let mut timely_op = TimelyErasedOperator::new(op, ctx);
                            let rt = rt_op;
                            move |(input, frontier), output| {
                                input.for_each(|cap, data| {
                                    let mut batch_durations = Vec::new();
                                    for item in data.drain(..) {
                                        let elem_start = std::time::Instant::now();
                                        let results = timely_op.process(item.0, &rt);
                                        batch_durations.push(elem_start.elapsed().as_secs_f64());
                                        if !results.is_empty() {
                                            let mut session = output.session(&cap);
                                            for r in results {
                                                session.give(AnyItem(r));
                                            }
                                        }
                                    }
                                    if !batch_durations.is_empty() {
                                        batch_durations.sort_unstable_by(|a, b| {
                                            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                        });
                                        let len = batch_durations.len();
                                        let p50 = batch_durations[len / 2];
                                        let p99 = batch_durations[(len * 99 / 100).min(len - 1)];
                                        metrics::gauge!("executor_element_duration_p50").set(p50);
                                        metrics::gauge!("executor_element_duration_p99").set(p99);
                                    }
                                });

                                let frontier_vec: Vec<u64> =
                                    frontier.frontier().iter().copied().collect();
                                timely_op.maybe_checkpoint(&frontier_vec, &rt);
                            }
                        },
                    );
            }
            Segment::Exchange(_) => {
                // Exchange segments are handled at the split level, not here.
            }
        }
    }

    // --- Sink terminal: send items via channel ---
    current
        .unary::<CapacityContainerBuilder<Vec<AnyItem>>, _, _, _>(
            Pipeline,
            "Sink",
            move |_cap, _info| {
                let sink_tx = sink_tx;
                move |input, _output| {
                    input.for_each(|_cap, data| {
                        for item in data.drain(..) {
                            let _ = sink_tx.blocking_send(item.0);
                        }
                    });
                }
            },
        )
        .probe()
}

/// Execute a compiled pipeline.
async fn execute_pipeline(
    pipeline: CompiledPipeline,
    executor: &Executor,
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

/// Single-worker execution: Timely-backed with `Config::process(1)`.
///
/// Bridges the source and sink to channels, runs all segments inside a
/// Timely dataflow with Pipeline pact, providing frontier-based checkpointing.
#[allow(clippy::too_many_lines)]
async fn execute_single_worker(
    source: Box<dyn ErasedSource>,
    segments: Vec<Segment>,
    sink: Box<dyn ErasedSink>,
    executor: &Executor,
    shutdown: Option<&ShutdownHandle>,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();

    // Extract operator names upfront to avoid holding &Segment across await.
    let operator_names: Vec<String> = segments
        .iter()
        .filter_map(|seg| {
            if let Segment::Operator { name, .. } = seg {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    // Pre-create StateContexts for operator segments (must be done on async task).
    let mut operator_contexts: Vec<Option<StateContext>> = Vec::new();
    for name in &operator_names {
        let ctx = executor.create_context_for_worker(name, 0).await?;
        operator_contexts.push(Some(ctx));
    }

    // Bridge source to channel.
    let source_rx = crate::bridge::erased_source_bridge(source, &rt, shutdown.cloned());

    // Create sink channel manually so we can await the sink task.
    let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel::<Box<dyn Any + Send>>(16);
    let sink_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let mut sink = sink;
        while let Some(item) = sink_rx.recv().await {
            sink.write(item).await?;
        }
        sink.flush().await?;
        Ok(())
    });

    // Package for the Timely closure (Fn, not FnOnce — use Mutex+Option+take).
    let source_rx = std::sync::Mutex::new(Some(source_rx));
    let segments = std::sync::Mutex::new(Some(segments));
    let operator_contexts = std::sync::Mutex::new(Some(operator_contexts));
    let sink_tx = std::sync::Mutex::new(Some(sink_tx));
    let rt_clone = rt.clone();

    metrics::gauge!("executor_workers").set(1.0);
    tracing::info!(workers = 1, "pipeline started");

    tokio::task::spawn_blocking(move || {
        let guards = timely::execute::execute(timely::execute::Config::process(1), move |worker| {
            let _span = tracing::info_span!("worker", worker = 0).entered();

            // Take per-worker data.
            let mut source_rx = source_rx.lock().unwrap().take().unwrap();
            let segments = segments.lock().unwrap().take().unwrap();
            let mut op_contexts = operator_contexts.lock().unwrap().take().unwrap();
            let sink_tx = sink_tx.lock().unwrap().take().unwrap();
            let rt = rt_clone.clone();

            let dataflow_index = worker.next_dataflow_index();
            let probe = worker.dataflow::<u64, _, _>(|scope| {
                build_timely_dataflow(
                    scope,
                    &mut source_rx,
                    segments,
                    &mut op_contexts,
                    sink_tx,
                    rt,
                )
            });

            while !probe.done() {
                worker.step();
            }

            worker.drop_dataflow(dataflow_index);
        })
        .map_err(|e| anyhow::anyhow!("timely execution failed: {e}"))?;

        // Drop guards to join worker threads. Sink sender is dropped here,
        // signaling the sink task that no more items are coming.
        drop(guards);
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

    // Wait for the sink bridge task to finish writing and flushing.
    sink_handle
        .await
        .map_err(|e| anyhow::anyhow!("sink task panicked: {e}"))??;

    Ok(())
}

/// Multi-worker execution: pre-exchange on Tokio task, post-exchange in Timely.
///
/// Pre-exchange transforms run in a Tokio task (stateless transforms + hash routing).
/// Post-exchange segments run inside per-worker Timely dataflows with Pipeline pact,
/// providing frontier tracking and epoch-based checkpointing.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn execute_multi_worker(
    mut source: Box<dyn ErasedSource>,
    pre_segments: Vec<Segment>,
    key_fn: KeyFn,
    post_segments: Vec<Segment>,
    sink: Box<dyn ErasedSink>,
    executor: &Executor,
    n_workers: usize,
    shutdown: Option<&ShutdownHandle>,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();
    let channel_capacity = 256;

    // Materialize pre-exchange steps on the main task (pure async).
    let mut pre_steps = materialize_steps(pre_segments, executor, 0).await?;

    // Per-worker input channels (Tokio → Timely worker).
    let mut worker_txs = Vec::with_capacity(n_workers);
    #[allow(clippy::type_complexity)]
    let mut worker_receivers: Vec<
        Option<tokio::sync::mpsc::Receiver<Vec<Box<dyn Any + Send>>>>,
    > = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Box<dyn Any + Send>>>(channel_capacity);
        worker_txs.push(tx);
        worker_receivers.push(Some(rx));
    }

    // Output channel: Timely workers → collector → sink.
    let (output_tx, mut output_rx) =
        tokio::sync::mpsc::channel::<Box<dyn Any + Send>>(channel_capacity);

    // Extract operator names upfront to avoid holding &Segment across await.
    let operator_names: Vec<String> = post_segments
        .iter()
        .filter_map(|seg| {
            if let Segment::Operator { name, .. } = seg {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    // Pre-create per-worker segments and StateContexts.
    let mut per_worker_segments: Vec<Option<Vec<Segment>>> = Vec::with_capacity(n_workers);
    let mut per_worker_contexts: Vec<Option<Vec<Option<StateContext>>>> =
        Vec::with_capacity(n_workers);
    for i in 0..n_workers {
        let cloned = clone_segments(&post_segments);
        let mut contexts = Vec::new();
        for name in &operator_names {
            let ctx = executor.create_context_for_worker(name, i).await?;
            contexts.push(Some(ctx));
        }
        per_worker_segments.push(Some(cloned));
        per_worker_contexts.push(Some(contexts));
    }

    // Package per-worker data for Timely closure (Fn, not FnOnce).
    let worker_receivers = std::sync::Mutex::new(worker_receivers);
    let per_worker_segments = std::sync::Mutex::new(per_worker_segments);
    let per_worker_contexts = std::sync::Mutex::new(per_worker_contexts);
    let output_tx_arc = Arc::new(std::sync::Mutex::new(Some(output_tx)));

    let rt_clone = rt.clone();

    #[allow(clippy::cast_possible_truncation)]
    metrics::gauge!("executor_workers").set(n_workers as f64);
    tracing::info!(workers = n_workers, "pipeline started");

    // Spawn Timely workers in a blocking thread.
    let timely_handle: tokio::task::JoinHandle<anyhow::Result<()>> =
        tokio::task::spawn_blocking(move || {
            let guards = timely::execute::execute(
                timely::execute::Config::process(n_workers),
                move |worker| {
                    let idx = worker.index();
                    let _span = tracing::info_span!("worker", worker = idx).entered();

                    // Take this worker's data.
                    let mut source_rx = worker_receivers.lock().unwrap()[idx].take().unwrap();
                    let segments = per_worker_segments.lock().unwrap()[idx].take().unwrap();
                    let mut op_contexts = per_worker_contexts.lock().unwrap()[idx].take().unwrap();
                    let sink_tx = output_tx_arc.lock().unwrap().as_ref().unwrap().clone();

                    let rt = rt_clone.clone();

                    let dataflow_index = worker.next_dataflow_index();
                    let probe = worker.dataflow::<u64, _, _>(|scope| {
                        build_timely_dataflow(
                            scope,
                            &mut source_rx,
                            segments,
                            &mut op_contexts,
                            sink_tx,
                            rt,
                        )
                    });

                    while !probe.done() {
                        worker.step();
                    }

                    worker.drop_dataflow(dataflow_index);
                },
            )
            .map_err(|e| anyhow::anyhow!("timely execution failed: {e}"))?;

            drop(guards);
            Ok(())
        });

    // Collector task: output channel → sink.
    let collector_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let mut sink = sink;
        while let Some(item) = output_rx.recv().await {
            sink.write(item).await?;
        }
        sink.flush().await?;
        Ok(())
    });

    // Main loop: source → pre-steps → hash key → route to per-worker channels.
    // Each send is a batch (Vec) that becomes one Timely epoch.
    let checkpoint_interval: u64 = 100;
    let mut batches_since_checkpoint: u64 = 0;

    while let Some(batch) = source.next_batch().await {
        #[allow(clippy::cast_possible_truncation)]
        let batch_len = batch.len() as u64;
        metrics::counter!("executor_batches_total").increment(1);
        metrics::counter!("executor_elements_total").increment(batch_len);

        // Apply pre-exchange transforms and bucket by worker.
        let mut worker_buckets: Vec<Vec<Box<dyn Any + Send>>> =
            (0..n_workers).map(|_| Vec::new()).collect();

        for item in batch {
            let mid_items = apply_steps(item, &mut pre_steps).await;
            for mid in mid_items {
                let key = key_fn(mid.as_ref());
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                #[allow(clippy::cast_possible_truncation)]
                let worker_idx = (hasher.finish() as usize) % n_workers;
                worker_buckets[worker_idx].push(mid);
            }
        }

        // Send non-empty buckets to workers as batches.
        for (i, bucket) in worker_buckets.into_iter().enumerate() {
            if !bucket.is_empty() && worker_txs[i].send(bucket).await.is_err() {
                break;
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

    // Close worker channels → Timely sources drop capabilities → dataflows complete.
    drop(worker_txs);

    // Wait for Timely workers to finish.
    timely_handle
        .await
        .map_err(|e| anyhow::anyhow!("timely workers panicked: {e}"))??;

    source.on_checkpoint_complete().await?;

    // Drop the output_tx so collector sees channel close.
    // (Timely workers already dropped their clones when exiting.)

    collector_handle
        .await
        .map_err(|e| anyhow::anyhow!("collector panicked: {e}"))??;

    Ok(())
}

// ── Public execution entry point ────────────────────────────────────

/// Compile and execute the dataflow graph.
async fn run_graph(
    graph: DataflowGraph,
    executor: &Executor,
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
}
