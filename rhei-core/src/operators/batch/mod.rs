//! Batch (Arrow columnar) operator implementations.
//!
//! These implement [`BatchStreamFunction`] and operate on `RheiBuffer`
//! batches rather than individual rows.

pub mod filter;
pub mod map;

pub use filter::{BatchFilterFnOp, BatchFilterOp};
pub use map::{BatchFlatMapOp, BatchMapOp};
