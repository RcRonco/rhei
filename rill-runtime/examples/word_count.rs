//! Word-count example demonstrating typed state with [`KeyedState`].
//!
//! Uses [`KeyedState<K, V>`](rill_core::operators::KeyedState) for ergonomic
//! typed state access instead of manual byte serialization. Compare with the
//! raw `StateContext` byte API to see the improvement.
//!
//! Run with: `cargo run -p rill-runtime --example word_count`

use async_trait::async_trait;
use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::operators::KeyedState;
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;
use rill_runtime::executor::{Executor, OperatorSlot};

/// A stateful word-count operator using [`KeyedState`] for typed state access.
///
/// Splits each input line into words and maintains a running count for each
/// word. Emits `"word: count"` strings.
struct WordCounter;

#[async_trait]
impl StreamFunction for WordCounter {
    type Input = String;
    type Output = String;

    async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
        let mut outputs = Vec::new();
        for word in input.split_whitespace() {
            let key = word.to_string();
            let count = {
                let mut state = KeyedState::<String, u64>::new(ctx, "count");
                let count = state.get(&key).await.unwrap_or(None).unwrap_or(0) + 1;
                state.put(&key, &count);
                count
            };
            outputs.push(format!("{word}: {count}"));
        }
        outputs
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rill_word_count_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::new(dir.clone());
    let ctx = executor.create_context("word_counter").await?;

    let mut source = VecSource::new(vec![
        "hello world".to_string(),
        "hello rill".to_string(),
        "rill is a stream processor".to_string(),
        "hello world again".to_string(),
    ]);

    let mut operators = vec![OperatorSlot::new(
        "word_counter",
        WordCounter,
        ctx,
        Some(tokio::runtime::Handle::current()),
    )];
    let mut sink: PrintSink<String> = PrintSink::new().with_prefix("output");

    executor
        .run_linear(&mut source, &mut operators, &mut sink)
        .await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
