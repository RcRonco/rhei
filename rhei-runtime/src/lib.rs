//! Runtime execution engine for Rhei stream processing pipelines.
//!
//! This crate provides the machinery to run logical plans built with `rhei-core`:
//!
//! - [`controller::PipelineController`] — configuration, lifecycle orchestration, and checkpointing
//! - [`executor`] — per-worker Timely DAG compilation and execution
//! - [`task_manager`] — task management, I/O bridging, and checkpoint orchestration
//! - [`dataflow::DataflowGraph`] — type-erased graph builder with
//!   [`Stream<T>`](dataflow::Stream)
//! - [`bridge`] — async-to-sync channel bridges for Timely integration
//! - [`telemetry`] — tracing and Prometheus metrics initialization

#![warn(missing_docs)]

/// Async-to-sync channel bridges for Timely dataflow integration.
pub mod bridge;
/// Cross-process checkpoint coordination via lightweight TCP.
pub mod checkpoint_coord;
/// Graph compilation: logical [`DataflowGraph`](dataflow::DataflowGraph) to executable segments.
pub mod compiler;
/// Pipeline configuration, lifecycle orchestration, and checkpointing.
pub mod controller;
/// Dataflow graph API: [`DataflowGraph`](dataflow::DataflowGraph),
/// [`Stream<T>`](dataflow::Stream).
pub mod dataflow;
/// Type-erased batch operator/source/sink wrappers for Arrow columnar execution.
pub mod erased_batch;
/// Type-erased Arrow buffer for Timely dataflow channels.
pub mod erased_buffer;
/// Pure Timely DAG construction and execution.
pub mod executor;
/// Fan-out recorder delegating to Prometheus and Snapshot recorders.
pub mod fanout_recorder;
/// Pipeline health state for readiness and liveness probes.
pub mod health;
/// HTTP server for health checks and Prometheus metrics.
pub mod http_server;
/// Decoupled metrics snapshot data layer for dashboards and exporters.
pub mod metrics_snapshot;
/// Graceful shutdown coordination.
pub mod shutdown;
/// Task management, I/O bridging, and checkpoint orchestration for Timely execution.
pub(crate) mod task_manager;
/// Tracing and Prometheus metrics initialization.
pub mod telemetry;
/// Timely-aware operator wrappers with capability management.
pub mod timely_operator;
/// Log capture layer for dashboards and log aggregation.
pub mod tracing_capture;

// Backward-compatible re-exports.
#[doc(hidden)]
pub use controller::PipelineController as Executor;
#[doc(hidden)]
pub use controller::PipelineControllerBuilder as ExecutorBuilder;
