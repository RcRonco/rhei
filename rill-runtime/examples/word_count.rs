//! Word-count example using the dataflow API.
//!
//! Demonstrates `Stream`, `KeyedStream`, and stateful operators with
//! automatic per-worker state context creation.
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

    // ── Single-worker ────────────────────────────────────────────────
    println!("=== Single-worker ===");
    {
        let executor = Executor::builder()
            .checkpoint_dir(dir.join("single"))
            .build();

        let stream = executor.source(VecSource::new(lines.clone()));
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .map(|result: String| format!("[output] {result}"))
            .sink(PrintSink::new());

        executor.run().await?;
    }

    // ── Multi-worker (2 workers) ─────────────────────────────────────
    println!("\n=== Multi-worker (2 workers) ===");
    {
        let executor = Executor::builder()
            .checkpoint_dir(dir.join("multi"))
            .workers(2)
            .build();

        let stream = executor.source(VecSource::new(lines));
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .map(|result: String| format!("[output] {result}"))
            .sink(PrintSink::new());

        executor.run().await?;
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
