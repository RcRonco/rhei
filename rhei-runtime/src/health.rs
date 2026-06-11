//! Pipeline health state for readiness and liveness probes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Pipeline lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PipelineStatus {
    /// Pipeline is being constructed but not yet running.
    Starting = 0,
    /// Pipeline is running and processing data.
    Running = 1,
    /// Pipeline is draining after a shutdown signal.
    Draining = 2,
    /// Pipeline has stopped (completed or errored).
    Stopped = 3,
}

impl PipelineStatus {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::Draining,
            _ => Self::Stopped,
        }
    }
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Draining => write!(f, "draining"),
            Self::Stopped => write!(f, "stopped"),
        }
    }
}

/// Shared, atomic health state. Cloneable across tasks and threads.
#[derive(Debug, Clone)]
pub struct HealthState {
    status: Arc<AtomicU8>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Create a new health state in [`PipelineStatus::Starting`].
    pub fn new() -> Self {
        Self {
            status: Arc::new(AtomicU8::new(PipelineStatus::Starting as u8)),
        }
    }

    /// Get the current pipeline status.
    pub fn status(&self) -> PipelineStatus {
        PipelineStatus::from_u8(self.status.load(Ordering::Relaxed))
    }

    /// Set the pipeline status.
    pub fn set_status(&self, status: PipelineStatus) {
        self.status.store(status as u8, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_u8_maps_known_variants() {
        assert_eq!(PipelineStatus::from_u8(0), PipelineStatus::Starting);
        assert_eq!(PipelineStatus::from_u8(1), PipelineStatus::Running);
        assert_eq!(PipelineStatus::from_u8(2), PipelineStatus::Draining);
        assert_eq!(PipelineStatus::from_u8(3), PipelineStatus::Stopped);
    }

    #[test]
    fn from_u8_falls_back_to_stopped_for_unknown() {
        // Any out-of-range byte is treated as Stopped (the safe terminal state).
        assert_eq!(PipelineStatus::from_u8(4), PipelineStatus::Stopped);
        assert_eq!(PipelineStatus::from_u8(255), PipelineStatus::Stopped);
    }

    #[test]
    fn status_round_trips_through_u8() {
        for status in [
            PipelineStatus::Starting,
            PipelineStatus::Running,
            PipelineStatus::Draining,
            PipelineStatus::Stopped,
        ] {
            assert_eq!(PipelineStatus::from_u8(status as u8), status);
        }
    }

    #[test]
    fn display_strings_are_stable() {
        // These strings are consumed by readiness/liveness probes, so pin them.
        assert_eq!(PipelineStatus::Starting.to_string(), "starting");
        assert_eq!(PipelineStatus::Running.to_string(), "running");
        assert_eq!(PipelineStatus::Draining.to_string(), "draining");
        assert_eq!(PipelineStatus::Stopped.to_string(), "stopped");
    }

    #[test]
    fn new_state_starts_in_starting() {
        assert_eq!(HealthState::new().status(), PipelineStatus::Starting);
        assert_eq!(HealthState::default().status(), PipelineStatus::Starting);
    }

    #[test]
    fn set_status_is_observed_through_clones() {
        let state = HealthState::new();
        let clone = state.clone();

        state.set_status(PipelineStatus::Running);
        // Clones share the same atomic, so the update is visible everywhere.
        assert_eq!(clone.status(), PipelineStatus::Running);

        clone.set_status(PipelineStatus::Stopped);
        assert_eq!(state.status(), PipelineStatus::Stopped);
    }
}
