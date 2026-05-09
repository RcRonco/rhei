//! Batch (Arrow columnar) operator implementations.
//!
//! These implement [`BatchStreamFunction`] and operate on `RheiBuffer`
//! batches rather than individual rows.

pub mod count_window;
pub mod filter;
pub mod filter_expr;
pub mod map;
pub mod reduce;
pub mod session_window;
pub mod sliding_window;
pub mod temporal_join;
pub mod tumbling_window;

pub use count_window::BatchCountWindow;
pub use filter::{BatchFilterFnOp, BatchFilterOp};
pub use filter_expr::{BatchFilterExprOp, Expr, col, lit_bool, lit_f64, lit_i64, lit_str, lit_u64};
pub use map::{BatchFlatMapOp, BatchMapOp};
pub use reduce::{BatchReduceOp, BatchRollingAggregateOp};
pub use session_window::BatchSessionWindow;
pub use sliding_window::BatchSlidingWindow;
pub use temporal_join::{BatchTemporalJoin, Side};
pub use tumbling_window::BatchTumblingWindow;
