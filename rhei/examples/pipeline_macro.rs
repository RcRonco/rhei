//! End-to-end example using `#[rhei::op]` and `#[rhei::pipeline]` macros.
//!
//! Run with: `cargo run -p rhei --example pipeline_macro`

use rhei::{KeyedState, PrintSink, StateContext, VecSource};

#[rhei::op]
async fn word_counter(input: String, ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
    let mut outputs = Vec::new();
    for word in input.split_whitespace() {
        let key = word.to_string();
        let count = {
            let mut state = KeyedState::<String, u64>::new(ctx, "count");
            let count = state.get(&key).await.unwrap_or(None).unwrap_or(0) + 1;
            state.put(&key, &count)?;
            count
        };
        outputs.push(format!("{word}: {count}"));
    }
    Ok(outputs)
}

#[rhei::pipeline]
fn main(graph: &DataflowGraph) {
    let lines = vec![
        "hello world".to_string(),
        "hello rhei".to_string(),
        "rhei is a stream processor".to_string(),
        "hello world again".to_string(),
    ];

    graph
        .source(VecSource::new(lines))
        .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
        .key_by(|word: &String| word.clone())
        .operator("word_counter", WordCounter)
        .sink(PrintSink::<String>::new());
}
