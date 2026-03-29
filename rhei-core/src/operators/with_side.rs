//! Side output enum for splitting operator output into main and side channels.
//!
//! Operators that need to emit to multiple logical outputs can use
//! `WithSide<M, S>` as their `Output` type, then downstream users call
//! `.split_side()` to fan out into separate streams.

use serde::{Deserialize, Serialize};

/// Tagged output: either a main-stream value or a side-stream value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WithSide<M, S> {
    /// Main-stream value.
    Main(M),
    /// Side-stream value (e.g. late events, errors, debug samples).
    Side(S),
}

impl<M, S> WithSide<M, S> {
    /// Wrap a value for the main stream.
    pub fn main(value: M) -> Self {
        Self::Main(value)
    }

    /// Wrap a value for the side stream.
    pub fn side(value: S) -> Self {
        Self::Side(value)
    }

    /// Returns `true` if this is a main-stream value.
    pub fn is_main(&self) -> bool {
        matches!(self, Self::Main(_))
    }

    /// Returns `true` if this is a side-stream value.
    pub fn is_side(&self) -> bool {
        matches!(self, Self::Side(_))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn constructors_and_predicates() {
        let m: WithSide<i32, String> = WithSide::main(42);
        assert!(m.is_main());
        assert!(!m.is_side());

        let s: WithSide<i32, String> = WithSide::side("late".into());
        assert!(s.is_side());
        assert!(!s.is_main());
    }

    #[test]
    fn serde_roundtrip() {
        let original: WithSide<i32, String> = WithSide::Main(42);
        let bytes = bincode::serialize(&original).unwrap();
        let restored: WithSide<i32, String> = bincode::deserialize(&bytes).unwrap();
        assert_eq!(original, restored);
    }
}
