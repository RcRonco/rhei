//! Timer service for event-time callbacks.
//!
//! [`TimerService`] manages a sorted set of `(timestamp, key)` pairs.
//! When the watermark advances, timers at or below it fire.

use std::collections::BTreeSet;

/// Sorted timer registry.
///
/// Register timers at specific timestamps; drain those that have been
/// passed by the watermark.
#[derive(Debug, Default)]
pub struct TimerService {
    timers: BTreeSet<(u64, String)>,
    dirty: bool,
}

impl TimerService {
    /// Creates a new empty timer service.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a timer that fires when watermark >= `timestamp`.
    pub fn register(&mut self, timestamp: u64, key: String) {
        self.timers.insert((timestamp, key));
        self.dirty = true;
    }

    /// Drain all timers with timestamp <= `watermark`.
    /// Returns them in ascending order.
    pub fn drain_fired(&mut self, watermark: u64) -> Vec<(u64, String)> {
        let mut fired = Vec::new();
        while let Some(entry) = self.timers.first().cloned() {
            if entry.0 <= watermark {
                self.timers.pop_first();
                fired.push(entry);
                self.dirty = true;
            } else {
                break;
            }
        }
        fired
    }

    /// Returns `true` if there are no registered timers.
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }

    /// Returns `true` if the timer set has been modified since last serialize.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Serialize the timer set to bytes.
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let entries: Vec<(u64, String)> = self.timers.iter().cloned().collect();
        Ok(bincode::serialize(&entries)?)
    }

    /// Restore the timer set from bytes.
    pub fn restore(bytes: &[u8]) -> anyhow::Result<Self> {
        let entries: Vec<(u64, String)> = bincode::deserialize(bytes)?;
        Ok(Self {
            timers: entries.into_iter().collect(),
            dirty: false,
        })
    }

    /// Clear the dirty flag (called after persisting).
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }
}

/// Stable key used to persist timer state in [`StateContext`].
pub const TIMER_STATE_KEY: &[u8] = b"__timers__";

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn register_and_drain() {
        let mut ts = TimerService::new();
        ts.register(100, "a".into());
        ts.register(50, "b".into());
        ts.register(200, "c".into());

        // Watermark at 50: fires (50, "b")
        let fired = ts.drain_fired(50);
        assert_eq!(fired, vec![(50, "b".into())]);

        // Watermark at 100: fires (100, "a")
        let fired = ts.drain_fired(100);
        assert_eq!(fired, vec![(100, "a".into())]);

        // Watermark at 150: nothing
        let fired = ts.drain_fired(150);
        assert!(fired.is_empty());

        // Watermark at 200: fires (200, "c")
        let fired = ts.drain_fired(200);
        assert_eq!(fired, vec![(200, "c".into())]);

        assert!(ts.is_empty());
    }

    #[test]
    fn no_fire_below_watermark() {
        let mut ts = TimerService::new();
        ts.register(100, "a".into());

        let fired = ts.drain_fired(50);
        assert!(fired.is_empty());
        assert!(!ts.is_empty());
    }

    #[test]
    fn multiple_timers_ordered() {
        let mut ts = TimerService::new();
        ts.register(30, "c".into());
        ts.register(10, "a".into());
        ts.register(20, "b".into());

        let fired = ts.drain_fired(25);
        assert_eq!(fired, vec![(10, "a".into()), (20, "b".into())]);
    }

    #[test]
    fn serialize_restore_roundtrip() {
        let mut ts = TimerService::new();
        ts.register(100, "a".into());
        ts.register(200, "b".into());

        let bytes = ts.serialize().unwrap();
        let restored = TimerService::restore(&bytes).unwrap();

        assert_eq!(restored.timers.len(), 2);
        let fired = {
            let mut r = restored;
            r.drain_fired(200)
        };
        assert_eq!(fired, vec![(100, "a".into()), (200, "b".into())]);
    }

    #[test]
    fn dirty_tracking() {
        let mut ts = TimerService::new();
        assert!(!ts.is_dirty());

        ts.register(100, "a".into());
        assert!(ts.is_dirty());

        ts.clear_dirty();
        assert!(!ts.is_dirty());

        ts.drain_fired(100);
        assert!(ts.is_dirty());
    }
}
