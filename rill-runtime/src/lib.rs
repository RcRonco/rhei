//! Runtime execution engine for Rill stream processing pipelines.
//!
//! This crate provides the machinery to run logical plans built with `rill-core`:
//!
//! - [`executor::Executor`] — materializes [`DataflowGraph`](dataflow::DataflowGraph)
//!   pipelines into Timely-backed multi-worker execution
//! - [`dataflow::DataflowGraph`] — type-erased graph builder with
//!   [`Stream<T>`](dataflow::Stream) and [`KeyedStream<T>`](dataflow::KeyedStream)
//! - [`bridge`] — async-to-sync channel bridges for Timely integration
//! - [`telemetry`] — tracing and Prometheus metrics initialization
//!
//! # Example
//!
//! ```ignore
//! let graph = DataflowGraph::new();
//! graph.source(my_source)
//!     .key_by(|item| item.key.clone())
//!     .operator("agg", MyOperator)
//!     .sink(my_sink);
//!
//! let executor = Executor::builder()
//!     .checkpoint_dir("./checkpoints")
//!     .workers(4)
//!     .build();
//! executor.run(graph).await?;
//! ```

#![warn(missing_docs)]

/// Non-blocking async wrapper for [`StreamFunction`](rill_core::traits::StreamFunction).
pub mod async_operator;
/// Async-to-sync channel bridges for Timely dataflow integration.
pub mod bridge;
/// Dataflow graph API: [`DataflowGraph`](dataflow::DataflowGraph),
/// [`Stream<T>`](dataflow::Stream), [`KeyedStream<T>`](dataflow::KeyedStream),
/// and type-erased execution engine.
pub mod dataflow;
/// Pipeline executor with Timely-backed multi-worker support.
pub mod executor;
/// Decoupled metrics snapshot data layer for dashboards and exporters.
pub mod metrics_snapshot;
/// Graceful shutdown coordination.
pub mod shutdown;
/// FIFO event stash for pending state fetches.
pub mod stash;
/// Tracing and Prometheus metrics initialization.
pub mod telemetry;
/// Timely-aware operator wrappers with capability management.
pub mod timely_operator;
/// Log capture layer for dashboards and log aggregation.
pub mod tracing_capture;
