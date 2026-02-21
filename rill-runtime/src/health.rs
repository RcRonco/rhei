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
