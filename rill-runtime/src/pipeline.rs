//! Fluent pipeline builder for composing heterogeneous stream processing steps.
//!
//! Instead of requiring all operators to share a single [`StreamFunction`] type,
//! the pipeline builder chains steps with compile-time type checking:
//!
//! ```ignore
//! executor
//!     .pipeline(source)
//!     .filter(|line: &String| !line.is_empty())
//!     .operator("word_counter", WordCounter, ctx)
//!     .map(|result: String| format!("[output] {result}"))
//!     .sink(sink)
//!     .await?;
//! ```

use async_trait::async_trait;
use rill_core::state::context::StateContext;
use rill_core::traits::{Sink, Source, StreamFunction};

use crate::shutdown::ShutdownHandle;

// ── ProcessStep trait ──────────────────────────────────────────────────────

/// A unified interface for all pipeline steps — stateless (`map`, `filter`,
/// `flat_map`) and stateful (operators with [`StateContext`]).
///
/// Stateless steps have no-op checkpoints. Stateful steps checkpoint their
/// internal state.
#[async_trait]
pub trait ProcessStep: Send {
    /// The element type consumed by this step.
    type Input: Send;
    /// The element type produced by this step.
    type Output: Send;

    /// Process a single input element and return zero or more output elements.
    async fn process(&mut self, input: Self::Input) -> Vec<Self::Output>;

    /// Checkpoint any internal state. Default is a no-op for stateless steps.
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

// ── Stateless steps ────────────────────────────────────────────────────────

/// A step that applies a mapping function to each element.
pub struct MapStep<F, I, O>
where
    F: FnMut(I) -> O + Send,
    I: Send,
    O: Send,
{
    f: F,
    _marker: std::marker::PhantomData<fn(I) -> O>,
}

impl<F, I, O> std::fmt::Debug for MapStep<F, I, O>
where
    F: FnMut(I) -> O + Send,
    I: Send,
    O: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapStep").finish_non_exhaustive()
    }
}

#[async_trait]
impl<F, I, O> ProcessStep for MapStep<F, I, O>
where
    F: FnMut(I) -> O + Send,
    I: Send,
    O: Send,
{
    type Input = I;
    type Output = O;

    async fn process(&mut self, input: I) -> Vec<O> {
        vec![(self.f)(input)]
    }
}

/// A step that filters elements based on a predicate.
pub struct FilterStep<F, T>
where
    F: FnMut(&T) -> bool + Send,
    T: Send,
{
    f: F,
    _marker: std::marker::PhantomData<fn(&T) -> bool>,
}

impl<F, T> std::fmt::Debug for FilterStep<F, T>
where
    F: FnMut(&T) -> bool + Send,
    T: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterStep").finish_non_exhaustive()
    }
}

#[async_trait]
impl<F, T> ProcessStep for FilterStep<F, T>
where
    F: FnMut(&T) -> bool + Send,
    T: Send,
{
    type Input = T;
    type Output = T;

    async fn process(&mut self, input: T) -> Vec<T> {
        if (self.f)(&input) {
            vec![input]
        } else {
            vec![]
        }
    }
}

/// A step that maps each element to zero or more output elements.
pub struct FlatMapStep<F, I, O>
where
    F: FnMut(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    f: F,
    _marker: std::marker::PhantomData<fn(I) -> Vec<O>>,
}

impl<F, I, O> std::fmt::Debug for FlatMapStep<F, I, O>
where
    F: FnMut(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMapStep").finish_non_exhaustive()
    }
}

#[async_trait]
impl<F, I, O> ProcessStep for FlatMapStep<F, I, O>
where
    F: FnMut(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    type Input = I;
    type Output = O;

    async fn process(&mut self, input: I) -> Vec<O> {
        (self.f)(input)
    }
}

// ── Stateful step ──────────────────────────────────────────────────────────

/// A step that wraps a [`StreamFunction`] with its own [`StateContext`].
///
/// Checkpointing delegates to the internal `StateContext`.
pub struct OperatorStep<F: StreamFunction> {
    func: F,
    ctx: StateContext,
}

impl<F: StreamFunction> std::fmt::Debug for OperatorStep<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorStep").finish_non_exhaustive()
    }
}

#[async_trait]
impl<F> ProcessStep for OperatorStep<F>
where
    F: StreamFunction + 'static,
{
    type Input = F::Input;
    type Output = F::Output;

    async fn process(&mut self, input: F::Input) -> Vec<F::Output> {
        self.func.process(input, &mut self.ctx).await
    }

    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.ctx.checkpoint().await
    }
}

// ── Composition ────────────────────────────────────────────────────────────

/// Chains two process steps sequentially.
///
/// The output of step `A` feeds into step `B`. Checkpointing recurses
/// through both steps, so stateful operators anywhere in the chain get
/// checkpointed correctly.
#[derive(Debug)]
pub struct ChainedStep<A, B> {
    a: A,
    b: B,
}

#[async_trait]
impl<A, B> ProcessStep for ChainedStep<A, B>
where
    A: ProcessStep,
    B: ProcessStep<Input = A::Output>,
{
    type Input = A::Input;
    type Output = B::Output;

    async fn process(&mut self, input: A::Input) -> Vec<B::Output> {
        let intermediate = self.a.process(input).await;
        let mut output = Vec::new();
        for item in intermediate {
            output.extend(self.b.process(item).await);
        }
        output
    }

    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.a.checkpoint().await?;
        self.b.checkpoint().await
    }
}

// ── Builder types ──────────────────────────────────────────────────────────

/// First stage of the pipeline builder, holding a source.
///
/// Call `.map()`, `.filter()`, `.flat_map()`, or `.operator()` to add
/// processing steps, or `.sink()` for a direct source-to-sink pipeline.
#[derive(Debug)]
pub struct PipelineSource<'a, S> {
    #[allow(dead_code)]
    executor: &'a crate::executor::Executor,
    source: S,
}

impl<'a, S> PipelineSource<'a, S> {
    pub(crate) fn new(executor: &'a crate::executor::Executor, source: S) -> Self {
        Self { executor, source }
    }
}

impl<'a, S> PipelineSource<'a, S>
where
    S: Source,
{
    /// Apply a mapping function to each element.
    pub fn map<F, O>(self, f: F) -> PipelineWithSteps<'a, S, MapStep<F, S::Output, O>, O>
    where
        F: FnMut(S::Output) -> O + Send,
        O: Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: MapStep {
                f,
                _marker: std::marker::PhantomData,
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Filter elements based on a predicate.
    pub fn filter<F>(self, f: F) -> PipelineWithSteps<'a, S, FilterStep<F, S::Output>, S::Output>
    where
        F: FnMut(&S::Output) -> bool + Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: FilterStep {
                f,
                _marker: std::marker::PhantomData,
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Apply a flat-map function to each element.
    pub fn flat_map<F, O>(self, f: F) -> PipelineWithSteps<'a, S, FlatMapStep<F, S::Output, O>, O>
    where
        F: FnMut(S::Output) -> Vec<O> + Send,
        O: Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: FlatMapStep {
                f,
                _marker: std::marker::PhantomData,
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Add a stateful operator step.
    pub fn operator<Func>(
        self,
        _name: &str,
        func: Func,
        ctx: StateContext,
    ) -> PipelineWithSteps<'a, S, OperatorStep<Func>, Func::Output>
    where
        Func: StreamFunction<Input = S::Output> + 'static,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: OperatorStep { func, ctx },
            _out: std::marker::PhantomData,
        }
    }

    /// Run a direct source-to-sink pipeline with no processing steps.
    pub async fn sink<K>(mut self, mut sink: K) -> anyhow::Result<()>
    where
        K: Sink<Input = S::Output>,
    {
        while let Some(batch) = self.source.next_batch().await {
            for item in batch {
                sink.write(item).await?;
            }
        }
        self.source.on_checkpoint_complete().await?;
        sink.flush().await?;
        Ok(())
    }
}

/// A pipeline with at least one processing step.
///
/// Call `.map()`, `.filter()`, `.flat_map()`, or `.operator()` to chain
/// additional steps, or `.sink()` / `.sink_with_shutdown()` to execute.
pub struct PipelineWithSteps<'a, S, Step, Out> {
    #[allow(dead_code)]
    executor: &'a crate::executor::Executor,
    source: S,
    step: Step,
    _out: std::marker::PhantomData<Out>,
}

impl<S, Step, Out> std::fmt::Debug for PipelineWithSteps<'_, S, Step, Out>
where
    S: std::fmt::Debug,
    Step: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineWithSteps")
            .field("source", &self.source)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl<'a, S, Step, Out> PipelineWithSteps<'a, S, Step, Out>
where
    S: Source,
    Step: ProcessStep<Input = S::Output, Output = Out>,
    Out: Send,
{
    /// Apply a mapping function to each element.
    pub fn map<F, NewOut>(
        self,
        f: F,
    ) -> PipelineWithSteps<'a, S, ChainedStep<Step, MapStep<F, Out, NewOut>>, NewOut>
    where
        F: FnMut(Out) -> NewOut + Send,
        NewOut: Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: ChainedStep {
                a: self.step,
                b: MapStep {
                    f,
                    _marker: std::marker::PhantomData,
                },
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Filter elements based on a predicate.
    pub fn filter<F>(
        self,
        f: F,
    ) -> PipelineWithSteps<'a, S, ChainedStep<Step, FilterStep<F, Out>>, Out>
    where
        F: FnMut(&Out) -> bool + Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: ChainedStep {
                a: self.step,
                b: FilterStep {
                    f,
                    _marker: std::marker::PhantomData,
                },
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Apply a flat-map function to each element.
    pub fn flat_map<F, NewOut>(
        self,
        f: F,
    ) -> PipelineWithSteps<'a, S, ChainedStep<Step, FlatMapStep<F, Out, NewOut>>, NewOut>
    where
        F: FnMut(Out) -> Vec<NewOut> + Send,
        NewOut: Send,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: ChainedStep {
                a: self.step,
                b: FlatMapStep {
                    f,
                    _marker: std::marker::PhantomData,
                },
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Add a stateful operator step.
    pub fn operator<Func>(
        self,
        _name: &str,
        func: Func,
        ctx: StateContext,
    ) -> PipelineWithSteps<'a, S, ChainedStep<Step, OperatorStep<Func>>, Func::Output>
    where
        Func: StreamFunction<Input = Out> + 'static,
    {
        PipelineWithSteps {
            executor: self.executor,
            source: self.source,
            step: ChainedStep {
                a: self.step,
                b: OperatorStep { func, ctx },
            },
            _out: std::marker::PhantomData,
        }
    }

    /// Execute the pipeline, writing outputs to the given sink.
    pub async fn sink<K>(self, sink: K) -> anyhow::Result<()>
    where
        K: Sink<Input = Out>,
    {
        self.run(sink, None).await
    }

    /// Execute the pipeline with graceful shutdown support.
    ///
    /// When the [`ShutdownHandle`] fires, the executor finishes the current
    /// batch, checkpoints all operator state, commits source offsets, flushes
    /// the sink, and returns `Ok(())`.
    pub async fn sink_with_shutdown<K>(
        self,
        sink: K,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()>
    where
        K: Sink<Input = Out>,
    {
        self.run(sink, Some(shutdown)).await
    }

    async fn run<K>(mut self, mut sink: K, shutdown: Option<ShutdownHandle>) -> anyhow::Result<()>
    where
        K: Sink<Input = Out>,
    {
        let checkpoint_interval: u64 = 100;
        let mut batches_since_checkpoint: u64 = 0;

        while let Some(batch) = self.source.next_batch().await {
            for item in batch {
                let outputs = self.step.process(item).await;
                for output in outputs {
                    sink.write(output).await?;
                }
            }

            batches_since_checkpoint += 1;
            if batches_since_checkpoint >= checkpoint_interval {
                self.step.checkpoint().await?;
                self.source.on_checkpoint_complete().await?;
                sink.flush().await?;
                batches_since_checkpoint = 0;
            }

            if let Some(ref handle) = shutdown
                && handle.is_shutdown()
            {
                tracing::info!("shutdown requested, performing final checkpoint...");
                break;
            }
        }

        // Final checkpoint
        self.step.checkpoint().await?;
        self.source.on_checkpoint_complete().await?;
        sink.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rill_core::connectors::vec_source::VecSource;
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
        std::env::temp_dir().join(format!("rill_pipeline_{name}_{}", std::process::id()))
    }

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
    async fn map_step() {
        let dir = temp_dir("map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(*collected.lock().unwrap(), vec![10, 20, 30]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn filter_step() {
        let dir = temp_dir("filter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .filter(|x: &i32| *x % 2 == 0)
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(*collected.lock().unwrap(), vec![2, 4]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn chained_filter_map() {
        let dir = temp_dir("chain");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .filter(|x: &i32| *x % 2 == 0)
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(*collected.lock().unwrap(), vec![20, 40]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn flat_map_step() {
        let dir = temp_dir("flatmap");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let source = VecSource::new(vec!["hello world".to_string(), "foo".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .flat_map(|s: String| s.split_whitespace().map(String::from).collect())
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(*collected.lock().unwrap(), vec!["hello", "world", "foo"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn operator_step_with_state() {
        let dir = temp_dir("operator");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let ctx = executor.create_context("word_counter").await.unwrap();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .operator("word_counter", WordCounter, ctx)
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(
            *collected.lock().unwrap(),
            vec!["hello: 1", "world: 1", "hello: 2", "rill: 1"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn direct_source_to_sink() {
        let dir = temp_dir("direct");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        executor
            .pipeline(source)
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(*collected.lock().unwrap(), vec![1, 2, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn heterogeneous_pipeline() {
        let dir = temp_dir("hetero");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let executor = crate::executor::Executor::new(dir.clone());
        let ctx = executor.create_context("word_counter").await.unwrap();
        let source = VecSource::new(vec![
            "hello world".to_string(),
            String::new(),
            "hello rill".to_string(),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        // filter -> operator -> map: heterogeneous pipeline
        executor
            .pipeline(source)
            .filter(|line: &String| !line.is_empty())
            .operator("word_counter", WordCounter, ctx)
            .map(|result: String| format!("[output] {result}"))
            .sink(CollectSink {
                collected: collected.clone(),
            })
            .await
            .unwrap();

        assert_eq!(
            *collected.lock().unwrap(),
            vec![
                "[output] hello: 1",
                "[output] world: 1",
                "[output] hello: 2",
                "[output] rill: 1",
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
