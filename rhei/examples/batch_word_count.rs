//! Batch word-count example using the Arrow columnar pipeline.
//!
//! Demonstrates: `BatchVecSource` → `flat_map` → `BatchTumblingWindow` → `BatchPrintSink`
//!
//! Each line is split into words, then a tumbling window counts words per
//! 1-second window (using synthetic timestamps). Windows close on source
//! exhaustion.
//!
//! Run with: `cargo run -p rhei --example batch_word_count`

use rhei::{
    BatchPrintSink, BatchTumblingWindow, BatchVecSource, DataflowGraph, PipelineController,
    RheiSchema as RheiSchemaDerive,
};

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Line {
    text: String,
    ts: u64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Word {
    word: String,
    ts: u64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct WordCount {
    word: String,
    window_start: u64,
    window_end: u64,
    count: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_batch_word_count_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let lines = vec![
        Line {
            text: "hello world".into(),
            ts: 1,
        },
        Line {
            text: "hello rhei".into(),
            ts: 2,
        },
        Line {
            text: "rhei is a stream processor".into(),
            ts: 3,
        },
        Line {
            text: "hello world again".into(),
            ts: 12,
        },
    ];

    let source = BatchVecSource::new(lines).with_batch_size(4);

    let window = BatchTumblingWindow::new(
        10,
        |view: WordView<'_>| view.word.to_string(),
        |view: WordView<'_>| view.ts,
        |acc: &mut u64, _view: WordView<'_>| *acc += 1,
        |key: &str, window_start: u64, window_end: u64, acc: &u64| WordCount {
            word: key.to_string(),
            window_start,
            window_end,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .flat_map(|view: LineView<'_>| {
            view.text
                .split_whitespace()
                .map(|w| Word {
                    word: w.to_string(),
                    ts: view.ts,
                })
                .collect::<Vec<_>>()
        })
        .operator("word_window", window)
        .sink(BatchPrintSink::<WordCount>::new().with_prefix("output"));

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
