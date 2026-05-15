//! Reusable stream processing operators (Arrow columnar).
//!
//! All operators work on `RheiBuffer<T>` (Arrow `RecordBatch` with selection vector).

pub mod batch;

pub use batch::{
    CountWindow, Expr, FilterExprOp, FilterFnOp, FilterOp, FlatMapOp, MapOp, ReduceOp,
    RollingAggregateOp, SessionWindow, Side, SlidingWindow, TemporalJoin, TumblingWindow, col,
    lit_bool, lit_f64, lit_i64, lit_str, lit_u64,
};
