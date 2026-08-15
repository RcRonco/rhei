//! Proc macros for the Rhei stream processing engine.
//!
//! Provides `#[op]` and `#[pipeline]` attribute macros and
//! `#[derive(RheiSchema)]` to reduce boilerplate when defining operators
//! and pipelines.

mod op;
mod pipeline;
mod rhei_schema;
mod util;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Define a stream operator from an async function.
///
/// # Example
///
/// ```ignore
/// #[rhei::op]
/// async fn upper(input: RheiBuffer<Word>, ctx: &mut OperatorContext) -> anyhow::Result<BufferOutput<Upper>> {
///     let mut builder = Upper::builder(input.len());
///     for view in &input {
///         builder.append(Upper { text: view.text.to_uppercase() });
///     }
///     Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
/// }
/// ```
///
/// Generates a struct (`Upper`) implementing `StreamFunction`.
#[proc_macro_attribute]
pub fn op(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as syn::ItemFn);
    match op::expand(item_fn) {
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
/// columnar builder, zero-copy view, typed column accessors, filter-expression
/// handles, and column constants.
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
/// Generates:
///
/// - `WebEventBuilder` — columnar builder over Arrow array builders.
/// - `WebEventView<'a>` — zero-copy view of one row.
/// - `WebEventColumns<'a>` — typed Arrow array accessors for the whole batch.
/// - `WebEventCol` plus `WebEvent::col()` — a typed `Col<T>` handle per field
///   for building filter expressions, e.g.
///   `WebEvent::col().user_id().gt(42)`. An `Option<T>` field yields `Col<T>`.
/// - Column name constants: `WebEvent::USER_ID`, `WebEvent::PATH`, etc.
#[proc_macro_derive(RheiSchema)]
pub fn derive_rhei_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match rhei_schema::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
