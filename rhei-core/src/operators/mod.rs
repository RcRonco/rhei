//! Reusable stream processing operators.
//!
//! This module provides high-level [`StreamFunction`](crate::traits::StreamFunction)
//! implementations so users don't have to write boilerplate for common patterns:
//!
//! - **Stateless combinators:** [`MapOp`], [`FlatMapOp`], [`FilterOp`]
//! - **Typed state:** [`KeyedState`] — ergonomic wrapper over [`StateContext`](crate::state::context::StateContext)
//! - **Aggregation:** [`Aggregator`] trait with built-in [`Count`], [`Sum`], [`Avg`]
//! - **Rolling aggregation:** [`ReduceOp`], [`RollingAggregateOp`] — per-key stateful emit-on-every-input
//! - **Windowing:** [`TumblingWindow`], [`SlidingWindow`], [`SessionWindow`], [`CountWindow`]
//! - **Joins:** [`TemporalJoin`] with [`JoinSide`]
//! - **Side outputs:** [`WithSide`] — split operator output into main and side channels
//! - **Async enrichment:** [`EnrichOp`] — bounded-concurrency async lookup

pub mod aggregator;
pub mod count_window;
pub mod enrich;
pub mod filter;
pub mod keyed_state;
pub mod map;
pub mod reduce;
pub mod rolling_aggregate;
pub mod session_window;
pub mod sliding_window;
pub mod temporal_join;
pub mod tumbling_window;
pub mod with_side;

pub use aggregator::{Aggregator, Avg, Count, Sum};
pub use count_window::{CountWindow, CountWindowOutput};
pub use enrich::EnrichOp;
pub use filter::FilterOp;
pub use keyed_state::{BincodeEncoder, JsonEncoder, KeyEncoder, KeyedState};
pub use map::{FlatMapOp, MapOp};
pub use reduce::ReduceOp;
pub use rolling_aggregate::RollingAggregateOp;
pub use session_window::SessionWindow;
pub use sliding_window::SlidingWindow;
pub use temporal_join::{JoinSide, TemporalJoin};
pub use tumbling_window::{TumblingWindow, WindowOutput};
pub use with_side::WithSide;
