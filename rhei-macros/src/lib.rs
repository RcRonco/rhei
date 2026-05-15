//! Proc macros for the Rhei stream processing engine.
//!
//! Provides `#[op_batch]` and `#[pipeline]` attribute macros and
//! `#[derive(RheiSchema)]` to reduce boilerplate when defining operators
//! and pipelines.

mod op_batch;
mod pipeline;
mod rhei_schema;
mod util;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

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
/// Generates a struct implementing `StreamFunction`.
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
///     graph.batch_source(VecSource::new(data))
///         .map(|e: EventView| Output { ... })
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

/// Derive the `RheiSchema` trait for a struct, generating Arrow schema,
/// columnar builder, zero-copy view, typed column accessors, and column constants.
///
/// # Example
///
/// ```ignore
/// #[derive(RheiSchema)]
/// pub struct WebEvent {
///     pub user_id: i64,
///     pub path: String,
///     pub is_active: bool,
///     pub timestamp: Option<i64>,
///     pub tags: Vec<String>,
/// }
/// ```
///
/// Generates: `WebEventBuilder`, `WebEventView<'a>`, `WebEventColumns<'a>`,
/// and constants like `WebEvent::USER_ID`, `WebEvent::PATH`, etc.
#[proc_macro_derive(RheiSchema)]
pub fn derive_rhei_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match rhei_schema::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
