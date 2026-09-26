//! Pipeline health state for readiness and liveness probes.
//!
//! Two different questions, two different signals:
//!
//! - **Readiness** ([`PipelineStatus`]) — "should traffic/scrapes come here?"
//!   Answered from the lifecycle status.
//! - **Liveness** ([`WorkerHeartbeats`]) — "is this process still doing work,
//!   or is it wedged?" Answered from per-worker heartbeats. A process whose
//!   Timely worker loop has stopped turning still has a live TCP socket and
//!   would pass any probe that only checks "is the process up"; it needs to be
//!   restarted, and only a heartbeat can tell you that.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

/// How long a worker may go without a heartbeat before liveness fails.
///
/// Generous by default: a checkpoint flush to object storage can occupy a
/// worker for a while, and restarting a healthy pipeline mid-flush is worse
/// than noticing a wedge a minute later. Tune with
/// [`HealthState::with_liveness_timeout`].
pub const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(60);

/// Milliseconds since the Unix epoch, saturating at 0 if the clock is before it.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Per-worker liveness heartbeats for one process.
///
/// Each worker owns one slot and stamps it every time its dataflow loop turns.
/// The pipeline is live when *every* worker has beaten recently: in a
/// shared-nothing dataflow a single wedged worker stalls the whole pipeline, so
/// the minimum across slots is the honest signal.
///
/// Heartbeats mean "this worker's loop is turning", not "data flowed". An idle
/// source produces no data but the loop still steps, so a correctly-idle
/// pipeline stays live.
///
/// Slots are allocated once at full capacity and the *active* count is adjusted
/// later, rather than reallocating when the worker count becomes known. Clones
/// are handed out before then — to the HTTP server, to the CLI — and a
/// reallocation would leave them watching slots nobody beats, failing liveness
/// on a perfectly healthy pipeline.
#[derive(Debug, Clone)]
pub struct WorkerHeartbeats {
    /// Timestamp slots in epoch milliseconds, one per potential local worker.
    slots: Arc<Vec<AtomicU64>>,
    /// How many leading slots correspond to real workers.
    active: Arc<std::sync::atomic::AtomicUsize>,
}

/// Upper bound on locally tracked workers.
///
/// Slots are 8 bytes each, so the whole table costs 8 KiB — cheap enough to
/// allocate once and never resize. Worker counts above this are still run; they
/// just share the last slot for liveness purposes.
const MAX_TRACKED_WORKERS: usize = 1024;

impl WorkerHeartbeats {
    /// Allocate heartbeat slots with `workers` of them active, all beating now.
    ///
    /// Slots start stamped rather than zeroed so that a pipeline is not
    /// reported as wedged during the window between allocation and the first
    /// loop iteration.
    pub fn new(workers: usize) -> Self {
        let now = now_millis();
        let this = Self {
            slots: Arc::new(
                (0..MAX_TRACKED_WORKERS)
                    .map(|_| AtomicU64::new(now))
                    .collect(),
            ),
            active: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        };
        this.set_worker_count(workers);
        this
    }

    /// Set how many workers are expected to beat.
    ///
    /// Visible through every existing clone, so it is safe to call after
    /// handing heartbeat handles to the health endpoint. Newly activated slots
    /// are re-stamped so a count increase does not instantly read as a stall.
    pub fn set_worker_count(&self, workers: usize) {
        let clamped = workers.clamp(1, MAX_TRACKED_WORKERS);
        if workers > MAX_TRACKED_WORKERS {
            tracing::warn!(
                workers,
                tracked = MAX_TRACKED_WORKERS,
                "more workers than heartbeat slots; liveness tracks the first {MAX_TRACKED_WORKERS}"
            );
        }
        let now = now_millis();
        for slot in self.slots.iter().take(clamped) {
            slot.store(now, Ordering::Relaxed);
        }
        self.active.store(clamped, Ordering::Relaxed);
    }

    /// Number of workers currently tracked.
    pub fn len(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Always false — at least one slot is always active.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Record that `worker` is still turning.
    ///
    /// Out-of-range indices are ignored: a heartbeat is diagnostic, and
    /// panicking in the hot loop would turn a bookkeeping mistake into an
    /// outage.
    pub fn beat(&self, worker: usize) {
        if let Some(slot) = self.slots.get(worker) {
            slot.store(now_millis(), Ordering::Relaxed);
        }
    }

    /// Milliseconds since the *least recently* beating worker last beat.
    ///
    /// This is the staleness of the pipeline as a whole.
    pub fn max_staleness_ms(&self) -> u64 {
        self.stalest_worker().1
    }

    /// Index and staleness of the active worker that has been silent longest.
    pub fn stalest_worker(&self) -> (usize, u64) {
        let now = now_millis();
        self.slots
            .iter()
            .take(self.len())
            .enumerate()
            .map(|(i, slot)| (i, now.saturating_sub(slot.load(Ordering::Relaxed))))
            .max_by_key(|&(_, staleness)| staleness)
            .unwrap_or((0, 0))
    }
}

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

/// Why a liveness probe failed, or that it passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Liveness {
    /// The process is healthy and should not be restarted.
    Live,
    /// The pipeline claims to be running but a worker has stopped beating.
    ///
    /// The process needs restarting: something in the dataflow loop is stuck
    /// (a blocked sink, a deadlocked operator, a wedged state backend) and it
    /// will not recover on its own.
    Stalled {
        /// The worker that has been silent longest.
        worker: usize,
        /// How long that worker has been silent.
        stalled_for: Duration,
        /// The configured tolerance it exceeded.
        timeout: Duration,
    },
}

impl Liveness {
    /// Whether the process should be considered alive.
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }
}

impl std::fmt::Display for Liveness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Stalled {
                worker,
                stalled_for,
                timeout,
            } => write!(
                f,
                "worker {worker} has not made progress for {:.1}s (liveness timeout {:.1}s)",
                stalled_for.as_secs_f64(),
                timeout.as_secs_f64()
            ),
        }
    }
}

/// Shared, atomic health state. Cloneable across tasks and threads.
#[derive(Debug, Clone)]
pub struct HealthState {
    status: Arc<AtomicU8>,
    heartbeats: WorkerHeartbeats,
    liveness_timeout: Duration,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Create a new health state in [`PipelineStatus::Starting`] with a single
    /// heartbeat slot and the default liveness timeout.
    pub fn new() -> Self {
        Self {
            status: Arc::new(AtomicU8::new(PipelineStatus::Starting as u8)),
            heartbeats: WorkerHeartbeats::new(1),
            liveness_timeout: DEFAULT_LIVENESS_TIMEOUT,
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

    /// Set how many local workers must beat for the pipeline to be live.
    ///
    /// Updates the shared slot table in place, so clones handed out earlier —
    /// to the HTTP server, to the CLI — observe the change too.
    #[must_use]
    pub fn with_workers(self, workers: usize) -> Self {
        self.heartbeats.set_worker_count(workers);
        self
    }

    /// Override how long a worker may go silent before liveness fails.
    #[must_use]
    pub fn with_liveness_timeout(mut self, timeout: Duration) -> Self {
        self.liveness_timeout = timeout;
        self
    }

    /// The configured liveness timeout.
    pub fn liveness_timeout(&self) -> Duration {
        self.liveness_timeout
    }

    /// Handle for workers to stamp their heartbeats.
    pub fn heartbeats(&self) -> WorkerHeartbeats {
        self.heartbeats.clone()
    }

    /// Evaluate liveness for a `/healthz` probe.
    ///
    /// Heartbeats are only meaningful while the pipeline is
    /// [`PipelineStatus::Running`]. Before that the workers do not exist yet,
    /// and during draining or after stopping they are expected to fall silent —
    /// restarting the process then would interrupt an orderly shutdown, so
    /// those states report [`Liveness::Live`] and let readiness carry the
    /// signal instead.
    pub fn liveness(&self) -> Liveness {
        if self.status() != PipelineStatus::Running {
            return Liveness::Live;
        }

        let (worker, staleness_ms) = self.heartbeats.stalest_worker();
        let stalled_for = Duration::from_millis(staleness_ms);
        if stalled_for > self.liveness_timeout {
            Liveness::Stalled {
                worker,
                stalled_for,
                timeout: self.liveness_timeout,
            }
        } else {
            Liveness::Live
        }
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

    // ── Liveness ─────────────────────────────────────────────────────────

    #[test]
    fn heartbeats_start_fresh() {
        let hb = WorkerHeartbeats::new(4);
        assert_eq!(hb.len(), 4);
        // Slots are stamped at construction so the window before the first
        // loop iteration is not reported as a stall.
        assert!(hb.max_staleness_ms() < 1_000);
    }

    #[test]
    fn zero_workers_still_gets_a_slot() {
        // Guards against a divide-by-nothing: `stalest_worker` must always
        // have something to report.
        assert_eq!(WorkerHeartbeats::new(0).len(), 1);
    }

    #[test]
    fn beat_on_unknown_worker_is_ignored() {
        let hb = WorkerHeartbeats::new(2);
        hb.beat(99); // must not panic — a heartbeat is diagnostic, not critical
        assert_eq!(hb.len(), 2);
    }

    #[test]
    fn stalest_worker_reports_the_quietest_slot() {
        let hb = WorkerHeartbeats::new(3);
        // Force worker 1 to look old by rewinding its stamp.
        hb.slots[1].store(1, Ordering::Relaxed);
        let (worker, staleness) = hb.stalest_worker();
        assert_eq!(worker, 1);
        assert!(staleness > 1_000_000);
    }

    #[test]
    fn liveness_is_true_before_running() {
        let state = HealthState::new().with_liveness_timeout(Duration::from_millis(1));
        // Starting: workers do not exist yet, so there is nothing to stall.
        assert_eq!(state.liveness(), Liveness::Live);
    }

    #[test]
    fn liveness_is_true_while_workers_beat() {
        let state = HealthState::new().with_workers(2);
        state.set_status(PipelineStatus::Running);
        state.heartbeats().beat(0);
        state.heartbeats().beat(1);
        assert!(state.liveness().is_live());
    }

    #[test]
    fn liveness_fails_when_a_worker_stops_beating() {
        let state = HealthState::new()
            .with_workers(2)
            .with_liveness_timeout(Duration::from_millis(10));
        state.set_status(PipelineStatus::Running);

        // Rewind worker 1's stamp to simulate a wedged dataflow loop.
        state.heartbeats.slots[1].store(1, Ordering::Relaxed);

        match state.liveness() {
            Liveness::Stalled { worker, .. } => assert_eq!(worker, 1),
            Liveness::Live => panic!("a wedged worker must fail liveness"),
        }
    }

    #[test]
    fn draining_pipeline_stays_live() {
        let state = HealthState::new()
            .with_workers(1)
            .with_liveness_timeout(Duration::from_millis(1));
        state.set_status(PipelineStatus::Draining);
        state.heartbeats.slots[0].store(1, Ordering::Relaxed);

        // Workers falling silent during shutdown is expected — restarting the
        // process here would interrupt an orderly drain.
        assert!(state.liveness().is_live());
    }

    #[test]
    fn stopped_pipeline_stays_live() {
        let state = HealthState::new()
            .with_workers(1)
            .with_liveness_timeout(Duration::from_millis(1));
        state.set_status(PipelineStatus::Stopped);
        state.heartbeats.slots[0].store(1, Ordering::Relaxed);
        assert!(state.liveness().is_live());
    }

    #[test]
    fn heartbeats_are_shared_with_clones() {
        let state = HealthState::new()
            .with_workers(1)
            .with_liveness_timeout(Duration::from_millis(10));
        state.set_status(PipelineStatus::Running);
        state.heartbeats.slots[0].store(1, Ordering::Relaxed);
        assert!(!state.liveness().is_live());

        // The worker's own handle must write through to the health endpoint's
        // view, or the probe reads a stamp nobody updates.
        let worker_view = state.clone().heartbeats();
        worker_view.beat(0);
        assert!(state.liveness().is_live());
    }

    #[test]
    fn stall_reason_names_the_worker_and_timeout() {
        let reason = Liveness::Stalled {
            worker: 3,
            stalled_for: Duration::from_secs(90),
            timeout: Duration::from_secs(60),
        };
        let text = reason.to_string();
        assert!(text.contains("worker 3"), "got: {text}");
        assert!(text.contains("90.0s"), "got: {text}");
        assert!(!reason.is_live());
    }

    #[test]
    fn worker_count_change_is_visible_through_earlier_clones() {
        // The CLI hands a HealthState clone to the HTTP server *before* the
        // executor learns the worker count. If resizing reallocated the slots,
        // the server would watch a table nobody beats and fail liveness on a
        // healthy pipeline.
        let state = HealthState::new().with_liveness_timeout(Duration::from_millis(10));
        let server_view = state.clone();
        state.set_status(PipelineStatus::Running);

        let running = state.with_workers(4);
        assert_eq!(server_view.heartbeats().len(), 4);

        // Rewind worker 3 — a slot that did not exist when the clone was made.
        running.heartbeats().slots[3].store(1, Ordering::Relaxed);
        match server_view.liveness() {
            Liveness::Stalled { worker, .. } => assert_eq!(worker, 3),
            Liveness::Live => panic!("clone must observe the resized slot table"),
        }
    }

    #[test]
    fn raising_the_worker_count_restamps_new_slots() {
        let hb = WorkerHeartbeats::new(1);
        hb.slots[2].store(1, Ordering::Relaxed);
        // Activating slot 2 must re-stamp it, or a count increase would read as
        // an instant stall.
        hb.set_worker_count(4);
        assert!(hb.max_staleness_ms() < 1_000);
    }

    #[test]
    fn worker_count_is_clamped_to_the_slot_table() {
        let hb = WorkerHeartbeats::new(1);
        hb.set_worker_count(MAX_TRACKED_WORKERS * 2);
        assert_eq!(hb.len(), MAX_TRACKED_WORKERS);
        hb.set_worker_count(0);
        assert_eq!(hb.len(), 1);
    }
}
