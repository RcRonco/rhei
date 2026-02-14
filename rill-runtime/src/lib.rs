//! Runtime execution engine for Rill stream processing pipelines.
//!
//! This crate provides the machinery to run logical plans built with `rill-core`:
//!
//! - [`executor::Executor`] — materializes pipelines into executable runs
//!   (linear sequential or Timely dataflow)
//! - [`async_operator::AsyncOperator`] — non-blocking wrapper around
//!   [`StreamFunction`](rill_core::traits::StreamFunction)
//! - [`stash::Stash`] — FIFO queue for events awaiting state fetches
//! - [`bridge`] — async-to-sync channel bridges for Timely integration
//! - [`timely_operator::TimelyAsyncOperator`] — capability-aware Timely operator wrapper
//! - [`telemetry`] — tracing and Prometheus metrics initialization

#![warn(missing_docs)]

/// Non-blocking async wrapper for [`StreamFunction`](rill_core::traits::StreamFunction).
pub mod async_operator;
/// Async-to-sync channel bridges for Timely dataflow integration.
pub mod bridge;
/// Pipeline executor (linear and Timely dataflow modes).
pub mod executor;
/// FIFO event stash for pending state fetches.
pub mod stash;
/// Tracing and Prometheus metrics initialization.
pub mod telemetry;
/// Timely-aware async operator with capability management.
pub mod timely_operator;
