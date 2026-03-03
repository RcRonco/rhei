//! Proc macros for the Rhei stream processing engine.
//!
//! Provides `#[op]`, `#[op_batch]`, and `#[pipeline]` attribute macros
//! to reduce boilerplate when defining operators and pipelines.

mod op;
mod op_batch;
mod pipeline;
mod util;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Define a stateful stream operator from an async function.
///
/// # Example
///
/// ```ignore
/// #[rhei::op]
/// async fn word_counter(input: String, ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
///     Ok(vec![input.to_uppercase()])
/// }
/// ```
///
/// Generates a `WordCounter` struct implementing `StreamFunction`.
#[proc_macro_attribute]
pub fn op(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as syn::ItemFn);
    match op::expand(item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Define a batch stream operator from an async function.
///
/// # Example
///
/// ```ignore
/// #[rhei::op_batch]
/// async fn batch_upper(inputs: Vec<String>, ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
///     Ok(inputs.into_iter().map(|s| s.to_uppercase()).collect())
/// }
/// ```
///
/// Generates a `BatchUpper` struct implementing `StreamFunction` with a custom
/// `process_batch`, and `process` delegates to `process_batch(vec![input])`.
#[proc_macro_attribute]
pub fn op_batch(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as syn::ItemFn);
    match op_batch::expand(item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Define a pipeline entry point.
///
/// # Example
///
/// ```ignore
/// #[rhei::pipeline]
/// fn main(graph: &DataflowGraph) {
///     graph.source(VecSource::new(data))
///         .key_by(|w: &String| w.clone())
///         .operator("counter", WordCounter)
///         .sink(PrintSink::new());
/// }
/// ```
///
/// Generates an async `main` with `#[tokio::main]`, checkpoint directory setup,
/// and `PipelineController` execution.
#[proc_macro_attribute]
pub fn pipeline(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as syn::ItemFn);
    match pipeline::expand(item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
