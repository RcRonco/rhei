use std::sync::Arc;

use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::state::prefixed_backend::PrefixedBackend;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::{TieredBackend, TieredBackendConfig};
use rill_core::traits::{Sink, Source, StreamFunction};

use crate::async_operator::AsyncOperator;
use crate::shutdown::ShutdownHandle;

/// Configuration for tiered storage on the executor.
#[derive(Debug)]
struct TieredStorageConfig {
    l3: Arc<SlateDbBackend>,
    foyer_config: TieredBackendConfig,
}

/// Materializes a `LogicalPlan` into an executable pipeline.
///
/// In Phase 1, this is a simple sequential executor that processes elements
/// through the pipeline stages. Full Timely dataflow integration comes later.
#[derive(Debug)]
pub struct Executor {
    checkpoint_dir: std::path::PathBuf,
    tiered: Option<TieredStorageConfig>,
}

impl Executor {
    /// Create a new executor with the given checkpoint directory.
    pub fn new(checkpoint_dir: std::path::PathBuf) -> Self {
        Self {
            checkpoint_dir,
            tiered: None,
        }
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

    /// Run a simple linear pipeline: source -> operators -> sink.
    ///
    /// This is the Phase 1 executor that processes elements sequentially.
    /// A full Timely-based executor will replace this in Phase 2.
    pub async fn run_linear<S, F, K>(
        &self,
        source: &mut S,
        operators: &mut [OperatorSlot<F>],
        sink: &mut K,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input>,
        F: StreamFunction + 'static,
        F::Output: Clone,
        K: Sink<Input = F::Output>,
    {
        self.run_linear_inner(source, operators, sink, None).await
    }

    /// Run a linear pipeline with graceful shutdown support.
    ///
    /// When the [`ShutdownHandle`] fires, the executor finishes the current
    /// batch, checkpoints all operator state, commits source offsets, flushes
    /// the sink, and returns `Ok(())`.
    pub async fn run_linear_with_shutdown<S, F, K>(
        &self,
        source: &mut S,
        operators: &mut [OperatorSlot<F>],
        sink: &mut K,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input>,
        F: StreamFunction + 'static,
        F::Output: Clone,
        K: Sink<Input = F::Output>,
    {
        self.run_linear_inner(source, operators, sink, Some(shutdown))
            .await
    }

    async fn run_linear_inner<S, F, K>(
        &self,
        source: &mut S,
        operators: &mut [OperatorSlot<F>],
        sink: &mut K,
        shutdown: Option<ShutdownHandle>,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input>,
        F: StreamFunction + 'static,
        F::Output: Clone,
        K: Sink<Input = F::Output>,
    {
        let checkpoint_interval: u64 = 100;
        let mut batches_since_checkpoint: u64 = 0;
        let mut batch_count: u64 = 0;
        let mut element_count: u64 = 0;

        while let Some(batch) = source.next_batch().await {
            batch_count += 1;
            metrics::counter!("executor_batches_total").increment(1);

            for item in batch {
                element_count += 1;
                metrics::counter!("executor_elements_total").increment(1);
                let elem_start = std::time::Instant::now();

                let mut current: Vec<F::Output> = Vec::new();

                // Feed through first operator
                if let Some(op) = operators.get_mut(0) {
                    let results = op.async_op.process_element(item, None);
                    current = results;
                }

                // Feed through remaining operators (if any chaining is needed)
                // For now, single-operator pipelines are the common case.

                // Write to sink
                for output in current {
                    sink.write(output).await?;
                }

                metrics::histogram!("executor_element_duration_seconds")
                    .record(elem_start.elapsed().as_secs_f64());
            }

            batches_since_checkpoint += 1;
            if batches_since_checkpoint >= checkpoint_interval {
                let ckpt_start = std::time::Instant::now();
                for op in operators.iter_mut() {
                    op.async_op.context_mut().checkpoint().await?;
                }
                metrics::histogram!("executor_checkpoint_duration_seconds")
                    .record(ckpt_start.elapsed().as_secs_f64());
                source.on_checkpoint_complete().await?;
                sink.flush().await?;
                batches_since_checkpoint = 0;
            }

            // Check for shutdown after processing each batch
            if let Some(ref handle) = shutdown
                && handle.is_shutdown()
            {
                tracing::info!("shutdown requested, performing final checkpoint...");
                break;
            }
        }

        // Final checkpoint
        let ckpt_start = std::time::Instant::now();
        for op in operators.iter_mut() {
            op.async_op.context_mut().checkpoint().await?;
        }
        metrics::histogram!("executor_checkpoint_duration_seconds")
            .record(ckpt_start.elapsed().as_secs_f64());

        source.on_checkpoint_complete().await?;
        sink.flush().await?;
        tracing::info!(
            batches = batch_count,
            elements = element_count,
            "pipeline completed"
        );
        Ok(())
    }

    /// Run a Timely dataflow pipeline: source -> operator -> sink.
    ///
    /// Bridges async Source/Sink to sync channels, wraps the operator in a
    /// `TimelyAsyncOperator`, and runs `timely::execute_directly()` inside
    /// `spawn_blocking`. Each source batch is one epoch.
    ///
    /// For multi-operator chaining, use [`run_dataflow_chain`](Self::run_dataflow_chain).
    #[allow(clippy::too_many_lines)]
    pub async fn run_dataflow<S, F, K>(
        &self,
        source: S,
        operators: Vec<OperatorSlot<F>>,
        sink: K,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input> + 'static,
        F: StreamFunction + 'static,
        F::Input: Clone + 'static,
        F::Output: Clone + 'static,
        K: Sink<Input = F::Output> + 'static,
    {
        tracing::info!("starting dataflow");
        let rt = tokio::runtime::Handle::current();

        // Bridge async source/sink to sync channels
        let source_rx = crate::bridge::source_bridge(source, &rt);
        let sink_tx = crate::bridge::sink_bridge(sink, &rt);

        // Extract the first operator
        let mut operators = operators.into_iter();
        let inner_op = operators
            .next()
            .expect("at least one operator required")
            .async_op;

        let rt_clone = rt.clone();

        let source_rx = std::sync::Mutex::new(source_rx);
        let inner_op = std::sync::Mutex::new(inner_op);

        tokio::task::spawn_blocking(move || {
            timely::execute_directly(move |worker| {
                use timely::container::CapacityContainerBuilder;
                use timely::dataflow::channels::pact::Pipeline;
                use timely::dataflow::operators::Inspect;
                use timely::dataflow::operators::core::probe::Probe;
                use timely::dataflow::operators::generic::OutputBuilder;
                use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
                use timely::dataflow::operators::generic::operator::Operator;
                use timely::scheduling::Scheduler;

                let mut source_rx = source_rx.into_inner().unwrap();
                let inner_op = inner_op.into_inner().unwrap();

                let timely_op = crate::timely_operator::TimelyAsyncOperator::new(inner_op);

                let rt_op = rt_clone.clone();

                let dataflow_index = worker.next_dataflow_index();
                let probe = worker.dataflow::<u64, _, _>(|scope| {
                    let mut source_builder =
                        OperatorBuilder::new("Source".to_owned(), scope.clone());
                    let (output, stream) = source_builder.new_output::<Vec<F::Input>>();
                    let mut output = OutputBuilder::from(output);
                    let activator = scope.activator_for(source_builder.operator_info().address);
                    source_builder.set_notify(false);

                    source_builder.build_reschedule(move |mut capabilities| {
                        let mut cap = Some(capabilities.pop().unwrap());
                        let mut epoch: u64 = 0;

                        move |_frontiers| {
                            if cap.is_none() {
                                return false;
                            }

                            match source_rx.try_recv() {
                                Ok(batch) => {
                                    if let Some(ref c) = cap {
                                        let mut handle = output.activate();
                                        let mut session = handle.session(c);
                                        for item in batch {
                                            session.give(item);
                                        }
                                    }
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

                    let processed = stream
                        .unary_frontier::<CapacityContainerBuilder<Vec<F::Output>>, _, _, _>(
                            Pipeline,
                            "AsyncOp",
                            move |_init_cap, _info| {
                                let mut timely_op = timely_op;
                                move |(input, frontier), output| {
                                    input.for_each(|cap, data| {
                                        let epoch = *cap.time();
                                        for item in data.drain(..) {
                                            let results = timely_op.process(item, epoch);
                                            if !results.is_empty() {
                                                let mut session = output.session(&cap);
                                                for r in results {
                                                    session.give(r);
                                                }
                                            }
                                        }
                                    });

                                    if timely_op.has_pending() {
                                        let _results = timely_op.poll_pending();
                                    }

                                    let frontier_vec: Vec<u64> =
                                        frontier.frontier().iter().copied().collect();
                                    timely_op.release_finished_epochs(&frontier_vec);
                                    timely_op.maybe_checkpoint(&frontier_vec, &rt_op);
                                }
                            },
                        );

                    processed
                        .inspect(move |item: &F::Output| {
                            let _ = sink_tx.blocking_send(item.clone());
                        })
                        .probe()
                });

                while !probe.done() {
                    worker.step();
                }

                worker.drop_dataflow(dataflow_index);
            });
        })
        .await
        .map_err(|e| anyhow::anyhow!("dataflow execution failed: {e}"))?;

        Ok(())
    }

    /// Run a Timely dataflow pipeline with multiple chained operators.
    ///
    /// Like [`run_dataflow`](Self::run_dataflow), but chains all operators
    /// as sequential Timely stages. Requires `F::Output: Into<F::Input>` so
    /// intermediate results can flow between stages.
    #[allow(clippy::too_many_lines)]
    pub async fn run_dataflow_chain<S, F, K>(
        &self,
        source: S,
        operators: Vec<OperatorSlot<F>>,
        sink: K,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input> + 'static,
        F: StreamFunction + 'static,
        F::Input: Clone + 'static,
        F::Output: Clone + Into<F::Input> + 'static,
        K: Sink<Input = F::Output> + 'static,
    {
        assert!(!operators.is_empty(), "at least one operator required");
        tracing::info!(operators = operators.len(), "starting dataflow");
        let rt = tokio::runtime::Handle::current();

        // Bridge async source/sink to sync channels
        let source_rx = crate::bridge::source_bridge(source, &rt);
        let sink_tx = crate::bridge::sink_bridge(sink, &rt);

        // Collect all operators
        let async_ops: Vec<AsyncOperator<F>> =
            operators.into_iter().map(|slot| slot.async_op).collect();

        let rt_clone = rt.clone();

        // Wrap !Sync captures in Mutex so the closure satisfies
        // execute_directly's Send+Sync bound. We unwrap them immediately
        // inside the single-threaded timely worker.
        let source_rx = std::sync::Mutex::new(source_rx);
        let async_ops = std::sync::Mutex::new(async_ops);

        // Run Timely computation in a blocking thread
        tokio::task::spawn_blocking(move || {
            timely::execute_directly(move |worker| {
                use timely::container::CapacityContainerBuilder;
                use timely::dataflow::channels::pact::Pipeline;
                use timely::dataflow::operators::Inspect;
                use timely::dataflow::operators::core::probe::Probe;
                use timely::dataflow::operators::generic::OutputBuilder;
                use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
                use timely::dataflow::operators::generic::operator::Operator;
                use timely::scheduling::Scheduler;

                // Unwrap Mutex wrappers (single-threaded from here)
                let mut source_rx = source_rx.into_inner().unwrap();
                let async_ops = async_ops.into_inner().unwrap();

                let rt_op = rt_clone.clone();

                // Capture index so we can explicitly drop the dataflow after
                // the probe loop. Without this, execute_directly's post-closure
                // loop calls step_or_park(None) → std::thread::park() which
                // hangs forever on spawn_blocking threads.
                let dataflow_index = worker.next_dataflow_index();
                let probe = worker.dataflow::<u64, _, _>(|scope| {
                    // --- Source operator: drains channel, emits with capability ---
                    let mut source_builder =
                        OperatorBuilder::new("Source".to_owned(), scope.clone());
                    let (output, stream) = source_builder.new_output::<Vec<F::Input>>();
                    let mut output = OutputBuilder::from(output);
                    let activator = scope.activator_for(source_builder.operator_info().address);
                    source_builder.set_notify(false);

                    source_builder.build_reschedule(move |mut capabilities| {
                        let mut cap = Some(capabilities.pop().unwrap());
                        let mut epoch: u64 = 0;

                        move |_frontiers| {
                            if cap.is_none() {
                                return false;
                            }

                            match source_rx.try_recv() {
                                Ok(batch) => {
                                    if let Some(ref c) = cap {
                                        let mut handle = output.activate();
                                        let mut session = handle.session(c);
                                        for item in batch {
                                            session.give(item);
                                        }
                                    }
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

                    // --- Chain operators as sequential Timely stages ---
                    // The first operator consumes F::Input and produces F::Output.
                    // Subsequent operators consume F::Output (converted to F::Input
                    // via Into) and produce F::Output.
                    let mut timely_ops: Vec<_> = async_ops
                        .into_iter()
                        .map(crate::timely_operator::TimelyAsyncOperator::new)
                        .collect();

                    // First operator: Stream<F::Input> -> Stream<F::Output>
                    let first_op = timely_ops.remove(0);
                    let rt_first = rt_op.clone();
                    let mut current = stream
                        .unary_frontier::<CapacityContainerBuilder<Vec<F::Output>>, _, _, _>(
                            Pipeline,
                            "AsyncOp_0",
                            move |_init_cap, _info| {
                                let mut timely_op = first_op;
                                let rt = rt_first;
                                move |(input, frontier), output| {
                                    input.for_each(|cap, data| {
                                        let epoch = *cap.time();
                                        for item in data.drain(..) {
                                            let results = timely_op.process(item, epoch);
                                            if !results.is_empty() {
                                                let mut session = output.session(&cap);
                                                for r in results {
                                                    session.give(r);
                                                }
                                            }
                                        }
                                    });

                                    if timely_op.has_pending() {
                                        let _results = timely_op.poll_pending();
                                    }

                                    let frontier_vec: Vec<u64> =
                                        frontier.frontier().iter().copied().collect();
                                    timely_op.release_finished_epochs(&frontier_vec);
                                    timely_op.maybe_checkpoint(&frontier_vec, &rt);
                                }
                            },
                        );

                    // Remaining operators: each takes Stream<F::Output>, converts
                    // via Into<F::Input>, processes, and produces Stream<F::Output>.
                    for (i, op) in timely_ops.into_iter().enumerate() {
                        let rt_chain = rt_op.clone();
                        let name = format!("AsyncOp_{}", i + 1);
                        current = current
                            .unary_frontier::<CapacityContainerBuilder<Vec<F::Output>>, _, _, _>(
                                Pipeline,
                                &name,
                                move |_init_cap, _info| {
                                    let mut timely_op = op;
                                    let rt = rt_chain;
                                    move |(input, frontier), output| {
                                        input.for_each(|cap, data| {
                                            let epoch = *cap.time();
                                            for item in data.drain(..) {
                                                let converted: F::Input = item.into();
                                                let results =
                                                    timely_op.process(converted, epoch);
                                                if !results.is_empty() {
                                                    let mut session =
                                                        output.session(&cap);
                                                    for r in results {
                                                        session.give(r);
                                                    }
                                                }
                                            }
                                        });

                                        if timely_op.has_pending() {
                                            let _results = timely_op.poll_pending();
                                        }

                                        let frontier_vec: Vec<u64> =
                                            frontier.frontier().iter().copied().collect();
                                        timely_op.release_finished_epochs(&frontier_vec);
                                        timely_op.maybe_checkpoint(&frontier_vec, &rt);
                                    }
                                },
                            );
                    }

                    // --- Sink: inspect + probe ---
                    current
                        .inspect(move |item: &F::Output| {
                            let _ = sink_tx.blocking_send(item.clone());
                        })
                        .probe()
                });

                // Drive computation until done
                while !probe.done() {
                    worker.step();
                }

                // Explicitly drop so execute_directly's post-closure loop
                // sees has_dataflows() == false and exits immediately.
                worker.drop_dataflow(dataflow_index);
            });
        })
        .await
        .map_err(|e| anyhow::anyhow!("dataflow execution failed: {e}"))?;

        Ok(())
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

/// Holds an operator wrapped in its async executor.
#[derive(Debug)]
pub struct OperatorSlot<F: StreamFunction + 'static> {
    /// Human-readable name identifying this operator in the pipeline.
    pub name: String,
    /// The async operator instance that processes elements.
    pub async_op: AsyncOperator<F>,
}

impl<F: StreamFunction + 'static> OperatorSlot<F> {
    /// Create a new operator slot with the given name, function, and state context.
    ///
    /// Pass `Some(tokio::runtime::Handle::current())` to enable cold-path async
    /// completion (state misses). Pass `None` for tests that only exercise the
    /// synchronous hot path.
    pub fn new(
        name: impl Into<String>,
        func: F,
        ctx: StateContext,
        rt: Option<tokio::runtime::Handle>,
    ) -> Self {
        Self {
            name: name.into(),
            async_op: AsyncOperator::new(func, ctx, rt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rill_core::connectors::vec_source::VecSource;
    use rill_core::traits::{Sink, StreamFunction};
    use std::sync::{Arc, Mutex};

    /// A stateful word-count operator.
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

    /// A sink that collects output into a shared vec.
    struct CollectSink {
        collected: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Sink for CollectSink {
        type Input = String;

        async fn write(&mut self, input: String) -> anyhow::Result<()> {
            self.collected.lock().unwrap().push(input);
            Ok(())
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rill_exec_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn word_count_end_to_end() {
        let dir = temp_dir("wordcount");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = Executor::new(dir.clone());
        let ctx = executor.create_context("word_counter").await.unwrap();

        let mut source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);

        let mut operators = vec![OperatorSlot::new("word_counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];

        let collected = Arc::new(Mutex::new(Vec::new()));
        let mut sink = CollectSink {
            collected: collected.clone(),
        };

        executor
            .run_linear(&mut source, &mut operators, &mut sink)
            .await
            .unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(
            results,
            vec![
                "hello: 1".to_string(),
                "world: 1".to_string(),
                "hello: 2".to_string(),
                "rill: 1".to_string(),
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn state_persists_across_checkpoints() {
        let dir = temp_dir("persist");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // First run
        {
            let executor = Executor::new(dir.clone());
            let ctx = executor.create_context("counter").await.unwrap();

            let mut source = VecSource::new(vec!["hello world".to_string()]);
            let mut operators = vec![OperatorSlot::new("counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];
            let collected = Arc::new(Mutex::new(Vec::new()));
            let mut sink = CollectSink {
                collected: collected.clone(),
            };

            executor
                .run_linear(&mut source, &mut operators, &mut sink)
                .await
                .unwrap();

            let results = collected.lock().unwrap().clone();
            assert_eq!(results, vec!["hello: 1", "world: 1"]);
        }

        // Second run — state should be restored from checkpoint
        {
            let executor = Executor::new(dir.clone());
            let ctx = executor.create_context("counter").await.unwrap();

            let mut source = VecSource::new(vec!["hello rill".to_string()]);
            let mut operators = vec![OperatorSlot::new("counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];
            let collected = Arc::new(Mutex::new(Vec::new()));
            let mut sink = CollectSink {
                collected: collected.clone(),
            };

            executor
                .run_linear(&mut source, &mut operators, &mut sink)
                .await
                .unwrap();

            let results = collected.lock().unwrap().clone();
            // hello count should be 2 (resumed from checkpoint)
            assert_eq!(results, vec!["hello: 2", "rill: 1"]);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn timely_word_count_end_to_end() {
        let dir = temp_dir("timely_wordcount");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = Executor::new(dir.clone());
        let ctx = executor.create_context("word_counter").await.unwrap();

        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);

        let operators = vec![OperatorSlot::new("word_counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];

        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectSink {
            collected: collected.clone(),
        };

        executor
            .run_dataflow(source, operators, sink)
            .await
            .unwrap();

        // Allow sink bridge task to flush
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let results = collected.lock().unwrap().clone();
        assert_eq!(
            results,
            vec![
                "hello: 1".to_string(),
                "world: 1".to_string(),
                "hello: 2".to_string(),
                "rill: 1".to_string(),
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn timely_state_persists_across_checkpoints() {
        let dir = temp_dir("timely_persist");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // First run
        {
            let executor = Executor::new(dir.clone());
            let ctx = executor.create_context("counter").await.unwrap();

            let source = VecSource::new(vec!["hello world".to_string()]);
            let operators = vec![OperatorSlot::new("counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];
            let collected = Arc::new(Mutex::new(Vec::new()));
            let sink = CollectSink {
                collected: collected.clone(),
            };

            executor
                .run_dataflow(source, operators, sink)
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let results = collected.lock().unwrap().clone();
            assert_eq!(results, vec!["hello: 1", "world: 1"]);
        }

        // Second run — state should be restored from checkpoint
        {
            let executor = Executor::new(dir.clone());
            let ctx = executor.create_context("counter").await.unwrap();

            let source = VecSource::new(vec!["hello rill".to_string()]);
            let operators = vec![OperatorSlot::new("counter", WordCounter, ctx, Some(tokio::runtime::Handle::current()))];
            let collected = Arc::new(Mutex::new(Vec::new()));
            let sink = CollectSink {
                collected: collected.clone(),
            };

            executor
                .run_dataflow(source, operators, sink)
                .await
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let results = collected.lock().unwrap().clone();
            // hello count should be 2 (resumed from checkpoint)
            assert_eq!(results, vec!["hello: 2", "rill: 1"]);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
