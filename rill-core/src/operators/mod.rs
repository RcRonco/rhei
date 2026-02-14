//! Reusable stream processing operators.
//!
//! This module provides high-level [`StreamFunction`](crate::traits::StreamFunction)
//! implementations so users don't have to write boilerplate for common patterns:
//!
//! - **Stateless combinators:** [`MapOp`], [`FlatMapOp`], [`FilterOp`]
//! - **Typed state:** [`KeyedState`] — ergonomic wrapper over [`StateContext`](crate::state::context::StateContext)
//! - **Aggregation:** [`Aggregator`] trait with built-in [`Count`], [`Sum`], [`Avg`]
//! - **Windowing:** [`TumblingWindow`], [`SessionWindow`]
//! - **Joins:** [`TemporalJoin`] with [`JoinSide`]

pub mod aggregator;
pub mod filter;
pub mod keyed_state;
pub mod map;
pub mod session_window;
pub mod temporal_join;
pub mod tumbling_window;

pub use aggregator::{Aggregator, Avg, Count, Sum};
pub use filter::FilterOp;
pub use keyed_state::KeyedState;
pub use map::{FlatMapOp, MapOp};
pub use session_window::SessionWindow;
pub use temporal_join::{JoinSide, TemporalJoin};
pub use tumbling_window::{TumblingWindow, WindowOutput};
