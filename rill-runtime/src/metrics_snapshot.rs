//! Decoupled metrics data layer for dashboards, web UI, and Prometheus export.
//!
//! [`SnapshotRecorder`] implements `metrics::Recorder`, storing counters and
//! gauges in shared atomic state. [`start_snapshot_publisher`] periodically
//! reads this state and publishes [`MetricsSnapshot`] via a `tokio::sync::watch`
//! channel — consumers never touch `metrics` internals directly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use serde::{Deserialize, Serialize};

/// Serde helper to serialize `Duration` as `f64` seconds and back.
mod duration_as_secs_f64 {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(duration.as_secs_f64())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Point-in-time snapshot of all pipeline metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Total elements processed.
    pub elements_total: u64,
    /// Total batches processed.
    pub batches_total: u64,
    /// Derived elements per second (computed from delta).
    pub elements_per_second: f64,
    /// State get operations.
    pub state_get_total: u64,
    /// State put operations.
    pub state_put_total: u64,
    /// L1 cache hits.
    pub l1_hits: u64,
    /// L2 cache hits.
    pub l2_hits: u64,
    /// L3 cache hits.
    pub l3_hits: u64,
    /// L1 hit rate percentage (0.0–100.0).
    pub l1_hit_rate: f64,
    /// L2 hit rate percentage (0.0–100.0).
    pub l2_hit_rate: f64,
    /// L3 hit rate percentage (0.0–100.0).
    pub l3_hit_rate: f64,
    /// Latest checkpoint duration in seconds.
    pub checkpoint_duration_secs: f64,
    /// Latest element processing duration p50 (seconds).
    pub element_duration_p50: f64,
    /// Latest element processing duration p99 (seconds).
    pub element_duration_p99: f64,
    /// Current stash depth (elements waiting for processing).
    pub stash_depth: f64,
    /// Current number of pending async futures.
    pub pending_futures: f64,
    /// Number of parallel workers.
    pub workers: u64,
    /// Time since pipeline started (serialized as seconds f64).
    #[serde(with = "duration_as_secs_f64")]
    pub uptime: Duration,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            elements_total: 0,
            batches_total: 0,
            elements_per_second: 0.0,
            state_get_total: 0,
            state_put_total: 0,
            l1_hits: 0,
            l2_hits: 0,
            l3_hits: 0,
            l1_hit_rate: 0.0,
            l2_hit_rate: 0.0,
            l3_hit_rate: 0.0,
            checkpoint_duration_secs: 0.0,
            element_duration_p50: 0.0,
            element_duration_p99: 0.0,
            stash_depth: 0.0,
            pending_futures: 0.0,
            workers: 0,
            uptime: Duration::ZERO,
        }
    }
}

/// Shared state backing the [`SnapshotRecorder`].
#[derive(Debug)]
struct RecorderInner {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    gauges: Mutex<HashMap<String, Arc<AtomicU64>>>,
    start_time: Instant,
}

/// A `metrics::Recorder` that stores counters and gauges in memory for snapshot
/// extraction. This has zero UI dependencies and can be used with any consumer.
#[derive(Debug, Clone)]
pub struct SnapshotRecorder {
    inner: Arc<RecorderInner>,
}

/// A shareable handle to the snapshot recorder, used to produce [`MetricsSnapshot`]s.
#[derive(Debug, Clone)]
pub struct MetricsHandle {
    inner: Arc<RecorderInner>,
}

impl MetricsHandle {
    /// Produce a current snapshot by reading all counters and gauges.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let counters = self.inner.counters.lock().unwrap();
        let gauges = self.inner.gauges.lock().unwrap();

        let get = |map: &HashMap<String, Arc<AtomicU64>>, key: &str| -> u64 {
            map.get(key).map_or(0, |v| v.load(Ordering::Relaxed))
        };

        let get_f64 = |map: &HashMap<String, Arc<AtomicU64>>, key: &str| -> f64 {
            map.get(key)
                .map_or(0.0, |v| f64::from_bits(v.load(Ordering::Relaxed)))
        };

        let elements_total = get(&counters, "executor_elements_total");
        let batches_total = get(&counters, "executor_batches_total");
        let state_get_total = get(&counters, "state_gets_total");
        let state_put_total = get(&counters, "state_puts_total");
        let l1_hits = get(&counters, "state_l1_hits_total");
        let l2_hits = get(&counters, "state_l2_hits_total");
        let l3_hits = get(&counters, "state_l3_hits_total");

        let total_lookups = l1_hits + l2_hits + l3_hits;
        let (l1_rate, l2_rate, l3_rate) = if total_lookups > 0 {
            #[allow(clippy::cast_precision_loss)]
            let total = total_lookups as f64;
            (
                l1_hits as f64 / total * 100.0,
                l2_hits as f64 / total * 100.0,
                l3_hits as f64 / total * 100.0,
            )
        } else {
            (0.0, 0.0, 0.0)
        };

        let checkpoint_duration_secs = get_f64(&gauges, "executor_checkpoint_duration_seconds");
        let element_duration_p50 = get_f64(&gauges, "executor_element_duration_p50");
        let element_duration_p99 = get_f64(&gauges, "executor_element_duration_p99");
        let stash_depth = get_f64(&gauges, "backpressure_stash_depth");
        let pending_futures = get_f64(&gauges, "backpressure_pending_futures");
        let workers_f64 = get_f64(&gauges, "executor_workers");
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let workers = workers_f64 as u64;

        let uptime = self.inner.start_time.elapsed();

        MetricsSnapshot {
            elements_total,
            batches_total,
            elements_per_second: 0.0, // computed by publisher from deltas
            state_get_total,
            state_put_total,
            l1_hits,
            l2_hits,
            l3_hits,
            l1_hit_rate: l1_rate,
            l2_hit_rate: l2_rate,
            l3_hit_rate: l3_rate,
            checkpoint_duration_secs,
            element_duration_p50,
            element_duration_p99,
            stash_depth,
            pending_futures,
            workers,
            uptime,
        }
    }
}

impl SnapshotRecorder {
    /// Create a new recorder and its corresponding handle.
    pub fn new() -> (Self, MetricsHandle) {
        let inner = Arc::new(RecorderInner {
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            start_time: Instant::now(),
        });
        (
            Self {
                inner: inner.clone(),
            },
            MetricsHandle { inner },
        )
    }

    fn counter_arc(&self, key: &Key) -> Arc<AtomicU64> {
        let mut map = self.inner.counters.lock().unwrap();
        map.entry(key.name().to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    fn gauge_arc(&self, key: &Key) -> Arc<AtomicU64> {
        let mut map = self.inner.gauges.lock().unwrap();
        map.entry(key.name().to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }
}

/// Atomic counter wrapper for `metrics::CounterFn`.
#[derive(Debug)]
struct AtomicCounter(Arc<AtomicU64>);

impl metrics::CounterFn for AtomicCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

/// Atomic gauge wrapper for `metrics::GaugeFn`.
#[derive(Debug)]
struct AtomicGauge(Arc<AtomicU64>);

impl metrics::GaugeFn for AtomicGauge {
    fn increment(&self, value: f64) {
        let mut current = f64::from_bits(self.0.load(Ordering::Relaxed));
        current += value;
        self.0.store(current.to_bits(), Ordering::Relaxed);
    }

    fn decrement(&self, value: f64) {
        let mut current = f64::from_bits(self.0.load(Ordering::Relaxed));
        current -= value;
        self.0.store(current.to_bits(), Ordering::Relaxed);
    }

    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

/// No-op histogram (snapshot recorder doesn't track distributions).
#[derive(Debug)]
struct NoopHistogram;

impl metrics::HistogramFn for NoopHistogram {
    fn record(&self, _value: f64) {}
}

impl Recorder for SnapshotRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(AtomicCounter(self.counter_arc(key))))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(AtomicGauge(self.gauge_arc(key))))
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(NoopHistogram))
    }
}

/// Spawn a background task that publishes [`MetricsSnapshot`] at the given interval.
///
/// Returns a `watch::Receiver` that always holds the latest snapshot.
pub fn start_snapshot_publisher(
    handle: MetricsHandle,
    interval: Duration,
) -> tokio::sync::watch::Receiver<MetricsSnapshot> {
    let (tx, rx) = tokio::sync::watch::channel(MetricsSnapshot::default());
    let mut prev_elements: u64 = 0;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let mut snapshot = handle.snapshot();

            // Compute throughput from delta
            #[allow(clippy::cast_precision_loss)]
            let delta = snapshot.elements_total.saturating_sub(prev_elements) as f64;
            snapshot.elements_per_second = delta / interval.as_secs_f64();
            prev_elements = snapshot.elements_total;

            if tx.send(snapshot).is_err() {
                break; // all receivers dropped
            }
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_tracks_counters() {
        let (recorder, handle) = SnapshotRecorder::new();
        let key = Key::from_name("executor_elements_total");
        let counter =
            recorder.register_counter(&key, &Metadata::new("", metrics::Level::INFO, None));
        counter.increment(42);

        let snap = handle.snapshot();
        assert_eq!(snap.elements_total, 42);
    }

    #[test]
    fn hit_rate_computation() {
        let (recorder, handle) = SnapshotRecorder::new();

        let l1_key = Key::from_name("state_l1_hits_total");
        let l2_key = Key::from_name("state_l2_hits_total");
        let l3_key = Key::from_name("state_l3_hits_total");
        let c1 = recorder.register_counter(&l1_key, &Metadata::new("", metrics::Level::INFO, None));
        let c2 = recorder.register_counter(&l2_key, &Metadata::new("", metrics::Level::INFO, None));
        let c3 = recorder.register_counter(&l3_key, &Metadata::new("", metrics::Level::INFO, None));

        c1.increment(90);
        c2.increment(7);
        c3.increment(3);

        let snap = handle.snapshot();
        assert!((snap.l1_hit_rate - 90.0).abs() < 0.01);
        assert!((snap.l2_hit_rate - 7.0).abs() < 0.01);
        assert!((snap.l3_hit_rate - 3.0).abs() < 0.01);
    }
}
