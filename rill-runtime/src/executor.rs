use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::traits::{Sink, Source, StreamFunction};

use crate::async_operator::AsyncOperator;

/// Materializes a `LogicalPlan` into an executable pipeline.
///
/// In Phase 1, this is a simple sequential executor that processes elements
/// through the pipeline stages. Full Timely dataflow integration comes later.
pub struct Executor {
    checkpoint_dir: std::path::PathBuf,
}

impl Executor {
    pub fn new(checkpoint_dir: std::path::PathBuf) -> Self {
        Self { checkpoint_dir }
    }

    /// Run a simple linear pipeline: source -> operators -> sink.
    ///
    /// This is the Phase 1 executor that processes elements sequentially.
    /// A full Timely-based executor will replace this in Phase 2.
    pub async fn run_linear<S, F, K>(
        &self,
        source: &mut S,
        operators: &mut Vec<OperatorSlot<F>>,
        sink: &mut K,
    ) -> anyhow::Result<()>
    where
        S: Source<Output = F::Input>,
        F: StreamFunction + 'static,
        F::Output: Clone,
        K: Sink<Input = F::Output>,
    {
        while let Some(batch) = source.next_batch().await {
            for item in batch {
                let mut current: Vec<F::Output> = Vec::new();

                // Feed through first operator
                if let Some(op) = operators.first_mut() {
                    let results = op.async_op.process_element(item, None);
                    current = results;
                }

                // Feed through remaining operators (if any chaining is needed)
                // For now, single-operator pipelines are the common case.

                // Write to sink
                for output in current {
                    sink.write(output).await?;
                }
            }
        }

        // Checkpoint all operator state
        for op in operators.iter_mut() {
            op.async_op.context_mut().checkpoint().await?;
        }

        sink.flush().await?;
        Ok(())
    }

    /// Create a `StateContext` backed by the local filesystem.
    pub fn create_context(&self, operator_name: &str) -> anyhow::Result<StateContext> {
        let path = self
            .checkpoint_dir
            .join(format!("{operator_name}.checkpoint.json"));
        let backend = LocalBackend::new(path, None)?;
        Ok(StateContext::new(Box::new(backend)))
    }
}

/// Holds an operator wrapped in its async executor.
pub struct OperatorSlot<F: StreamFunction + 'static> {
    pub name: String,
    pub async_op: AsyncOperator<F>,
}

impl<F: StreamFunction + 'static> OperatorSlot<F> {
    pub fn new(name: impl Into<String>, func: F, ctx: StateContext) -> Self {
        Self {
            name: name.into(),
            async_op: AsyncOperator::new(func, ctx),
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
        let ctx = executor.create_context("word_counter").unwrap();

        let mut source = VecSource::new(vec![
            "hello world".to_string(),
            "hello rill".to_string(),
        ]);

        let mut operators = vec![OperatorSlot::new("word_counter", WordCounter, ctx)];

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
            let ctx = executor.create_context("counter").unwrap();

            let mut source = VecSource::new(vec!["hello world".to_string()]);
            let mut operators = vec![OperatorSlot::new("counter", WordCounter, ctx)];
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
            let ctx = executor.create_context("counter").unwrap();

            let mut source = VecSource::new(vec!["hello rill".to_string()]);
            let mut operators = vec![OperatorSlot::new("counter", WordCounter, ctx)];
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
}
