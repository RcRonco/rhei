//! Rhei stream processing engine — facade crate.
//!
//! Add `rhei = "0.1"` and use `#[rhei::op]`, `#[rhei::op_batch]`, and
//! `#[rhei::pipeline]` to define operators and pipelines with minimal
//! boilerplate.

// Re-export macros
pub use rhei_macros::{op, op_batch, pipeline};

// Core traits
pub use rhei_core::traits::{Sink, Source, StreamFunction};

// State
pub use rhei_core::state::context::StateContext;

// Dataflow graph API
pub use rhei_runtime::dataflow::{DataflowGraph, KeyedStream, Stream, TransformContext};

// Pipeline controller
pub use rhei_runtime::controller::{PipelineController, PipelineControllerBuilder};

// Common connectors
pub use rhei_core::connectors::print_sink::PrintSink;
pub use rhei_core::connectors::vec_source::VecSource;

// Operators
pub use rhei_core::operators::keyed_state::KeyedState;

/// Items used by macro-generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use tokio;
}
