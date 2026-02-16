//! Word-count example demonstrating the fluent pipeline builder.
//!
//! Shows two modes:
//! 1. **Single-worker** — the original pipeline builder API with `.operator()`.
//! 2. **Multi-worker** — the keyed pipeline with `.key_by()` and `.with_workers()`.
//!
//! Run with: `cargo run -p rill-runtime --example word_count`

use async_trait::async_trait;
use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::operators::KeyedState;
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;
use rill_runtime::executor::Executor;

/// A stateful word-count operator using [`KeyedState`] for typed state access.
///
/// Splits each input line into words and maintains a running count for each
/// word. Emits `"word: count"` strings.
#[derive(Clone)]
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

    let lines = vec![
        "hello world".to_string(),
        "hello rill".to_string(),
        "rill is a stream processor".to_string(),
        "hello world again".to_string(),
    ];

    // ── Single-worker pipeline (original API) ────────────────────────
    println!("=== Single-worker pipeline ===");
    {
        let executor = Executor::new(dir.clone());
        let ctx = executor.create_context("word_counter").await?;

        executor
            .pipeline(VecSource::new(lines.clone()))
            .operator("word_counter", WordCounter, ctx)
            .map(|result: String| format!("[output] {result}"))
            .sink(PrintSink::new())
            .await?;
    }

    // ── Multi-worker keyed pipeline ──────────────────────────────────
    println!("\n=== Multi-worker keyed pipeline (2 workers) ===");
    {
        let dir2 = dir.join("keyed");
        std::fs::create_dir_all(&dir2)?;
        let executor = Executor::new(dir2).with_workers(2);

        executor
            .pipeline(VecSource::new(lines))
            .flat_map(|line: String| {
                line.split_whitespace().map(String::from).collect::<Vec<_>>()
            })
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .map(|result: String| format!("[output] {result}"))
            .sink(PrintSink::new())
            .await?;
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
