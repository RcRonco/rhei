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

// State types
pub use rhei_core::state::list_state::ListState;
pub use rhei_core::state::map_state::MapState;
pub use rhei_core::state::timer_service::TimerService;
pub use rhei_core::state::value_state::ValueState;

// Kafka connectors (behind `kafka` feature)
#[cfg(feature = "kafka")]
pub use rhei_core::connectors::kafka::sink::KafkaSink;
#[cfg(feature = "kafka")]
pub use rhei_core::connectors::kafka::source::KafkaSource;
#[cfg(feature = "kafka")]
pub use rhei_core::connectors::kafka::types::{KafkaHeader, KafkaMessage, KafkaRecord};

// Operators
pub use rhei_core::operators::count_window::{CountWindow, CountWindowOutput};
pub use rhei_core::operators::enrich::EnrichOp;
pub use rhei_core::operators::keyed_state::KeyedState;
pub use rhei_core::operators::reduce::ReduceOp;
pub use rhei_core::operators::rolling_aggregate::RollingAggregateOp;
pub use rhei_core::operators::with_side::WithSide;

/// Items used by macro-generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use tokio;
}
