//! Rhei stream processing engine — facade crate.
//!
//! Add `rhei = "0.1"` and use `#[rhei::op]`, `#[rhei::pipeline]`, and
//! `#[derive(RheiSchema)]` to define operators and pipelines with minimal
//! boilerplate.

// Re-export macros
pub use rhei_macros::{RheiSchema, op, pipeline};

// The repository's user-facing Markdown documentation is compiled as doctests
// so that documented APIs cannot drift from the real ones. Verified by
// `cargo test --doc -p rhei` in CI. See DOCS-AUDIT.md.
#[cfg(doctest)]
#[doc = include_str!("../../README.md")]
pub struct ReadmeDocs;

#[cfg(doctest)]
#[doc = include_str!("../../API.md")]
pub struct ApiDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/getting-started.md")]
pub struct GettingStartedDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/walkthrough.md")]
pub struct WalkthroughDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/concepts.md")]
pub struct ConceptsDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/operators.md")]
pub struct OperatorsDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/time-and-watermarks.md")]
pub struct TimeDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/exchange-and-partitioning.md")]
pub struct ExchangeDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/state-and-checkpointing.md")]
pub struct StateDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/deployment.md")]
pub struct DeploymentDocs;

#[cfg(doctest)]
#[doc = include_str!("../../docs/internals.md")]
pub struct InternalsDocs;

// Arrow primitives and batch traits
pub use rhei_core::arrow::{
    BufferOutput, OperatorContext, OperatorMetrics, RheiBuffer, RheiBuilder, RheiIter,
    RheiSchema as RheiSchemaT, Sink, Source, StreamFunction,
};

/// Arrow module re-exports for direct access to traits and types.
pub mod arrow {
    pub use rhei_core::arrow::*;
}

// State
pub use rhei_core::state::context::StateContext;

// Dataflow graph API
pub use rhei_runtime::dataflow::{DataflowGraph, Stream};

// Pipeline controller
pub use rhei_runtime::controller::{PipelineController, PipelineControllerBuilder};

// Batch connectors
pub use rhei_core::connectors::batch::{PrintSink, VecSource};

// State types
pub use rhei_core::state::list_state::ListState;
pub use rhei_core::state::map_state::MapState;
pub use rhei_core::state::timer_service::TimerService;
pub use rhei_core::state::value_state::ValueState;

// Kafka connectors (behind `kafka` feature)
#[cfg(feature = "kafka")]
pub use rhei_core::connectors::batch::{KafkaSink, KafkaSource};
#[cfg(feature = "kafka")]
pub use rhei_core::connectors::kafka::types::{KafkaHeader, KafkaMessage, KafkaRecord};

// Batch operators
pub use rhei_core::operators::{
    AfterMatch, Col, CountWindow, Expr, FilterExprOp, MatchCtx, ReduceOp, RollingAggregateOp,
    ScalarLike, ScalarValue, SequenceDetect, SessionWindow, Side, SlidingWindow, TemporalJoin,
    TumblingWindow, col, lit_bool, lit_f64, lit_i64, lit_str, lit_u64,
};

// Window builders
pub use rhei_core::operators::window_builder::{
    CountWindowBuilder, SessionWindowBuilder, SlidingWindowBuilder, TumblingWindowBuilder, Window,
};

// Batch connectors (partitioned)
pub use rhei_core::connectors::batch::PartitionedVecSource;

// KeyedState (used by stateful batch operators)
pub use rhei_core::operators::keyed_state::KeyedState;

/// Items used by macro-generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use clap;
    pub use tokio;
}
