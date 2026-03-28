//! Time providers for processing-time windowing.
//!
//! [`TimeProvider`] abstracts the clock so that operators using processing time
//! can be tested deterministically. Two built-in implementations are provided:
//!
//! - [`WallClockProvider`] — returns the real wall-clock time (Unix millis).
//! - [`FixedClockProvider`] — an externally controlled clock for testing and replay.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A clock that returns the current time as Unix milliseconds.
///
/// Implement this trait to supply a custom clock to processing-time window
/// operators via [`proc_time_fn_with`](crate::operators::TumblingWindow).
pub trait TimeProvider: Send + Sync {
    /// Returns the current time in Unix milliseconds.
    fn current_time(&self) -> u64;
}

/// Wall-clock provider that delegates to [`SystemTime::now()`].
///
/// Zero-size and `Copy`, so closures capturing it are auto-`Clone`.
#[derive(Debug, Clone, Copy)]
pub struct WallClockProvider;

impl TimeProvider for WallClockProvider {
    fn current_time(&self) -> u64 {
        #[allow(clippy::cast_possible_truncation)] // millis won't overflow u64 until year 584M+
        {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|e| panic!("system clock before UNIX epoch: {e}"))
                .as_millis() as u64
        }
    }
}

/// A clock whose value is controlled externally via an [`AtomicU64`].
///
/// `Clone` shares state through the inner `Arc`, so all clones (and any
/// closures that captured this provider) see the same time.
#[derive(Debug, Clone)]
pub struct FixedClockProvider {
    millis: Arc<AtomicU64>,
}

impl FixedClockProvider {
    /// Creates a new `FixedClockProvider` starting at `initial_millis`.
    pub fn new(initial_millis: u64) -> Self {
        Self {
            millis: Arc::new(AtomicU64::new(initial_millis)),
        }
    }

    /// Sets the clock to an absolute value.
    pub fn set(&self, millis: u64) {
        self.millis.store(millis, Ordering::Release);
    }

    /// Advances the clock by `delta` milliseconds and returns the new value.
    pub fn advance(&self, delta: u64) -> u64 {
        self.millis.fetch_add(delta, Ordering::AcqRel) + delta
    }
}

impl TimeProvider for FixedClockProvider {
    fn current_time(&self) -> u64 {
        self.millis.load(Ordering::Acquire)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_returns_reasonable_value() {
        let provider = WallClockProvider;
        let t = provider.current_time();
        // Should be after 2024-01-01 (1_704_067_200_000 ms)
        assert!(t > 1_704_067_200_000, "wall clock too low: {t}");
    }

    #[test]
    fn fixed_clock_set_and_read() {
        let clock = FixedClockProvider::new(1000);
        assert_eq!(clock.current_time(), 1000);

        clock.set(5000);
        assert_eq!(clock.current_time(), 5000);
    }

    #[test]
    fn fixed_clock_advance() {
        let clock = FixedClockProvider::new(1000);
        let new_val = clock.advance(250);
        assert_eq!(new_val, 1250);
        assert_eq!(clock.current_time(), 1250);
    }

    #[test]
    fn fixed_clock_clone_shares_state() {
        let clock = FixedClockProvider::new(100);
        let clone = clock.clone();

        clock.set(999);
        assert_eq!(clone.current_time(), 999);

        clone.advance(1);
        assert_eq!(clock.current_time(), 1000);
    }
}
