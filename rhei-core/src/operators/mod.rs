//! Reusable stream processing operators (Arrow columnar).
//!
//! All operators work on `RheiBuffer<T>` (Arrow `RecordBatch` with selection vector).

pub mod batch;

pub use batch::{
    BatchCountWindow, BatchFilterExprOp, BatchFilterFnOp, BatchFilterOp, BatchFlatMapOp,
    BatchMapOp, BatchReduceOp, BatchRollingAggregateOp, BatchSessionWindow, BatchSlidingWindow,
    BatchTemporalJoin, BatchTumblingWindow, Expr, Side, col, lit_bool, lit_f64, lit_i64, lit_str,
    lit_u64,
};
