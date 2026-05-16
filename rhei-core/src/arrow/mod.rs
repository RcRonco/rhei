//! Apache Arrow columnar execution primitives.
//!
//! This module provides the core abstractions for Arrow-based stream processing:
//! - [`RheiSchema`] — trait connecting user structs to their Arrow representation
//! - [`RheiBuilder`] — columnar builder for accumulating rows into Arrow arrays
//! - [`RheiBuffer`] — buffer wrapping a `RecordBatch` with optional selection vector

pub mod aggregator;
mod buffer;
mod builder;
mod context;
mod iter;
mod schema;
mod traits;

pub use aggregator::{ArrowAggregator, AvgAgg, CountAgg, SumAgg};
pub use buffer::{BufferOutput, RheiBuffer};
pub use builder::RheiBuilder;
pub use context::{OperatorContext, OperatorMetrics};
pub use iter::RheiIter;
pub use schema::RheiSchema;
pub use traits::{Sink, Source, StreamFunction};
