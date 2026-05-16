#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Word-count example using the batch Arrow dataflow API.
//!
//! Demonstrates `Stream`, `key_by`, and a stateful `StreamFunction` with
//! per-worker `KeyedState` access.
//!
//! Run with: `cargo run -p rhei-runtime --example word_count`

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};
use rhei_core::connectors::batch::{PrintSink, VecSource};
use rhei_core::operators::keyed_state::KeyedState;
use rhei_runtime::Executor;
use rhei_runtime::dataflow::DataflowGraph;

// ── Line schema (single Utf8 column) ────────────────────────────────

#[derive(Clone)]
struct Line {
    text: String,
}

struct LineBuilder {
    text: arrow_array::builder::StringBuilder,
}

#[derive(Debug)]
struct LineView<'a> {
    text: &'a str,
}

struct LineCols<'a> {
    #[allow(dead_code)]
    text: &'a arrow_array::StringArray,
}

impl RheiBuilder for LineBuilder {
    type Item = Line;
    fn append(&mut self, item: Line) {
        self.text.append_value(&item.text);
    }
    fn append_null(&mut self) {
        self.text.append_null();
    }
    fn len(&self) -> usize {
        self.text.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(Line::arrow_schema(), vec![Arc::new(self.text.finish())])
            .expect("Line schema mismatch")
    }
}

impl RheiSchema for Line {
    type Builder = LineBuilder;
    type View<'a> = LineView<'a>;
    type Columns<'a> = LineCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        LineBuilder {
            text: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 32),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        LineView {
            text: batch.column(0).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        LineCols {
            text: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Word schema (single Utf8 column) ────────────────────────────────

struct Word {
    word: String,
}

struct WordBuilder {
    word: arrow_array::builder::StringBuilder,
}

#[derive(Debug)]
struct WordView<'a> {
    word: &'a str,
}

struct WordCols<'a> {
    #[allow(dead_code)]
    word: &'a arrow_array::StringArray,
}

impl RheiBuilder for WordBuilder {
    type Item = Word;
    fn append(&mut self, item: Word) {
        self.word.append_value(&item.word);
    }
    fn append_null(&mut self) {
        self.word.append_null();
    }
    fn len(&self) -> usize {
        self.word.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(Word::arrow_schema(), vec![Arc::new(self.word.finish())])
            .expect("Word schema mismatch")
    }
}

impl RheiSchema for Word {
    type Builder = WordBuilder;
    type View<'a> = WordView<'a>;
    type Columns<'a> = WordCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("word", DataType::Utf8, false)]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        WordBuilder {
            word: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        WordView {
            word: batch.column(0).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        WordCols {
            word: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── WordCount output schema ─────────────────────────────────────────

struct WordCount {
    result: String,
}

struct WordCountBuilder {
    result: arrow_array::builder::StringBuilder,
}

#[derive(Debug)]
#[allow(dead_code)]
struct WordCountView<'a> {
    result: &'a str,
}

struct WordCountCols<'a> {
    #[allow(dead_code)]
    result: &'a arrow_array::StringArray,
}

impl RheiBuilder for WordCountBuilder {
    type Item = WordCount;
    fn append(&mut self, item: WordCount) {
        self.result.append_value(&item.result);
    }
    fn append_null(&mut self) {
        self.result.append_null();
    }
    fn len(&self) -> usize {
        self.result.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            WordCount::arrow_schema(),
            vec![Arc::new(self.result.finish())],
        )
        .expect("WordCount schema mismatch")
    }
}

impl RheiSchema for WordCount {
    type Builder = WordCountBuilder;
    type View<'a> = WordCountView<'a>;
    type Columns<'a> = WordCountCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            false,
        )]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        WordCountBuilder {
            result: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 32),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        WordCountView {
            result: batch.column(0).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        WordCountCols {
            result: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Stateful word-count operator ────────────────────────────────────

#[derive(Clone)]
struct WordCounter;

#[async_trait]
impl StreamFunction for WordCounter {
    type Input = Word;
    type Output = WordCount;

    async fn process(
        &mut self,
        input: RheiBuffer<Word>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<WordCount>> {
        let mut outputs = Vec::new();
        let batch = input.as_record_batch();

        for (_phys_idx, _view) in input.iter().enumerate_physical() {
            let view = Word::view(batch, _phys_idx);
            let key = view.word.to_string();
            let count = {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "count");
                let prev = state.get(&key).await.unwrap_or(None).unwrap_or(0);
                let next = prev + 1;
                state.put(&key, &next)?;
                next
            };
            outputs.push(WordCount {
                result: format!("{key}: {count}"),
            });
        }

        if outputs.is_empty() {
            return Ok(BufferOutput::None);
        }
        let mut builder = WordCount::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_word_count_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let lines = vec![
        Line {
            text: "hello world".into(),
        },
        Line {
            text: "hello rhei".into(),
        },
        Line {
            text: "rhei is a stream processor".into(),
        },
        Line {
            text: "hello world again".into(),
        },
    ];

    println!("=== Single-worker ===");
    {
        let graph = DataflowGraph::new();
        graph
            .source(VecSource::new(lines.clone()))
            .flat_map(|view: LineView<'_>| {
                view.text
                    .split_whitespace()
                    .map(|w| Word {
                        word: w.to_string(),
                    })
                    .collect()
            })
            .key_by(|view: &WordView<'_>| view.word.to_string())
            .operator("word_counter", WordCounter)
            .sink(PrintSink::<WordCount>::new());

        let executor = Executor::builder()
            .checkpoint_dir(dir.join("single"))
            .build()?;
        executor.run(graph).await?;
    }

    println!("\n=== Multi-worker (3 workers) ===");
    {
        let graph = DataflowGraph::new();
        graph
            .source(VecSource::new(lines))
            .flat_map(|view: LineView<'_>| {
                view.text
                    .split_whitespace()
                    .map(|w| Word {
                        word: w.to_lowercase(),
                    })
                    .collect()
            })
            .key_by(|view: &WordView<'_>| view.word.to_string())
            .operator("word_counter", WordCounter)
            .sink(PrintSink::<WordCount>::new());

        let executor = Executor::builder()
            .checkpoint_dir(dir.join("multi"))
            .workers(3)
            .build()?;
        executor.run(graph).await?;
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
