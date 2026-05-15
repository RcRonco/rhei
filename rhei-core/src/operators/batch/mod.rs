//! Batch (Arrow columnar) operator implementations.
//!
//! These implement [`StreamFunction`] and operate on `RheiBuffer`
//! batches rather than individual rows.

pub mod count_window;
pub mod filter;
pub mod filter_expr;
pub mod keyed_state;
pub mod map;
pub mod reduce;
pub mod session_window;
pub mod sliding_window;
pub mod temporal_join;
pub mod tumbling_window;

pub use count_window::CountWindow;
pub use filter::{FilterFnOp, FilterOp};
pub use filter_expr::{Expr, FilterExprOp, col, lit_bool, lit_f64, lit_i64, lit_str, lit_u64};
pub use map::{FlatMapOp, MapOp};
pub use reduce::{ReduceOp, RollingAggregateOp};
pub use session_window::SessionWindow;
pub use sliding_window::SlidingWindow;
pub use temporal_join::{Side, TemporalJoin};
pub use tumbling_window::TumblingWindow;
