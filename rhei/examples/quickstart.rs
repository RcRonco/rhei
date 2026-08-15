//! The README quick-start pipeline, as a compiled and runnable example.
//!
//! Counts occurrences of each word using keyed state. The README embeds this
//! same code; `xtask/check-docs` verifies the two stay in sync.
//!
//! Run with: `cargo run -p rhei --example quickstart`

// ANCHOR: quickstart
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema};
use rhei::{KeyedState, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct WordIn {
    text: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct WordOut {
    text: String,
    count: u64,
}

#[rhei::op]
async fn word_counter(
    input: RheiBuffer<WordIn>,
    ctx: &mut OperatorContext,
) -> anyhow::Result<BufferOutput<WordOut>> {
    let mut builder = WordOut::builder(input.len());
    let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "count");

    for view in &input {
        let word = view.text.to_string();
        let count = state.get(&word).await?.unwrap_or(0) + 1;
        state.put(&word, &count)?;
        builder.append(WordOut { text: word, count });
    }

    Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
}

#[rhei::pipeline]
fn main(graph: &DataflowGraph) {
    let words = ["hello", "world", "hello"]
        .into_iter()
        .map(|text| WordIn { text: text.into() })
        .collect();

    graph
        .source(VecSource::new(words))
        .key_by(|w| w.text.to_string())
        .operator("word_counter", WordCounter)
        .sink(PrintSink::<WordOut>::new());
}
// ANCHOR_END: quickstart
