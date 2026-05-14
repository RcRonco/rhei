//! Core abstractions for the Rhei stream processing engine.
//!
//! This crate provides the foundational types and traits for building
//! stateful stream processing pipelines using Apache Arrow columnar execution:
//!
//! - [`arrow::BatchStreamFunction`] — stateful operator that transforms Arrow buffers
//! - [`arrow::BatchSource`] — produces columnar batches into a pipeline
//! - [`arrow::BatchSink`] — consumes columnar batches from a pipeline
//! - [`state::context::StateContext`] — operator-scoped key-value state with tiered storage
//! - [`connectors`] — built-in sources and sinks

#![warn(missing_docs)]

/// Apache Arrow columnar execution primitives.
pub mod arrow;
/// Checkpoint manifest for recording pipeline checkpoint metadata.
pub mod checkpoint;
/// TOML-based pipeline configuration with environment variable overrides.
pub mod config;
/// Built-in source and sink connectors.
pub mod connectors;
/// Dead-letter queue types.
pub mod dlq;
/// Classified pipeline errors for structured error handling and DLQ routing.
pub mod error;
/// Event serialization and the [`Event`](event::Event) marker trait.
pub mod event;
/// Logical execution plan builder ([`StreamGraph`](graph::StreamGraph)).
pub mod graph;
/// Reusable stream processing operators.
pub mod operators;
/// Tiered state management (memtable, local, Foyer, `SlateDB`).
pub mod state;
/// Time providers for processing-time windowing and deterministic replay.
pub mod time;
