//! Fan-out recorder that delegates metrics to both Prometheus and Snapshot recorders.
//!
//! [`FanoutRecorder`] wraps a [`PrometheusRecorder`] and a [`SnapshotRecorder`],
//! forwarding every `register_*` / `describe_*` call to both. This lets the HTTP
//! server serve Prometheus text on `/metrics` and structured JSON on `/api/metrics`
//! without parsing Prometheus exposition format.

use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_exporter_prometheus::PrometheusRecorder;

use crate::metrics_snapshot::SnapshotRecorder;

/// A `metrics::Recorder` that fans out to both Prometheus and Snapshot recorders.
#[derive(Debug)]
pub struct FanoutRecorder {
    prometheus: PrometheusRecorder,
    snapshot: SnapshotRecorder,
}

impl FanoutRecorder {
    /// Create a new fanout recorder from its two inner recorders.
    pub fn new(prometheus: PrometheusRecorder, snapshot: SnapshotRecorder) -> Self {
        Self {
            prometheus,
            snapshot,
        }
    }
}

/// Counter that increments both inner counters.
#[derive(Debug)]
struct FanoutCounter {
    prometheus: Counter,
    snapshot: Counter,
}

impl metrics::CounterFn for FanoutCounter {
    fn increment(&self, value: u64) {
        self.prometheus.increment(value);
        self.snapshot.increment(value);
    }

    fn absolute(&self, value: u64) {
        self.prometheus.absolute(value);
        self.snapshot.absolute(value);
    }
}

/// Gauge that sets both inner gauges.
#[derive(Debug)]
struct FanoutGauge {
    prometheus: Gauge,
    snapshot: Gauge,
}

impl metrics::GaugeFn for FanoutGauge {
    fn increment(&self, value: f64) {
        self.prometheus.increment(value);
        self.snapshot.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.prometheus.decrement(value);
        self.snapshot.decrement(value);
    }

    fn set(&self, value: f64) {
        self.prometheus.set(value);
        self.snapshot.set(value);
    }
}

/// Histogram that records to both inner histograms.
#[derive(Debug)]
struct FanoutHistogram {
    prometheus: Histogram,
    snapshot: Histogram,
}

impl metrics::HistogramFn for FanoutHistogram {
    fn record(&self, value: f64) {
        self.prometheus.record(value);
        self.snapshot.record(value);
    }
}

impl Recorder for FanoutRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_counter(key.clone(), unit, description.clone());
        self.snapshot.describe_counter(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_gauge(key.clone(), unit, description.clone());
        self.snapshot.describe_gauge(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_histogram(key.clone(), unit, description.clone());
        self.snapshot.describe_histogram(key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        let prom = self.prometheus.register_counter(key, metadata);
        let snap = self.snapshot.register_counter(key, metadata);
        Counter::from_arc(Arc::new(FanoutCounter {
            prometheus: prom,
            snapshot: snap,
        }))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let prom = self.prometheus.register_gauge(key, metadata);
        let snap = self.snapshot.register_gauge(key, metadata);
        Gauge::from_arc(Arc::new(FanoutGauge {
            prometheus: prom,
            snapshot: snap,
        }))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let prom = self.prometheus.register_histogram(key, metadata);
        let snap = self.snapshot.register_histogram(key, metadata);
        Histogram::from_arc(Arc::new(FanoutHistogram {
            prometheus: prom,
            snapshot: snap,
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use metrics::Recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;

    fn meta() -> Metadata<'static> {
        Metadata::new("", metrics::Level::INFO, None)
    }

    /// Build a `FanoutRecorder` and return read handles for both backends.
    fn fanout() -> (
        FanoutRecorder,
        metrics_exporter_prometheus::PrometheusHandle,
        crate::metrics_snapshot::MetricsHandle,
    ) {
        let prometheus = PrometheusBuilder::new().build_recorder();
        let prom_handle = prometheus.handle();
        let (snapshot, snap_handle) = SnapshotRecorder::new();
        (
            FanoutRecorder::new(prometheus, snapshot),
            prom_handle,
            snap_handle,
        )
    }

    #[test]
    fn counter_increment_reaches_both_backends() {
        let (recorder, prom_handle, snap_handle) = fanout();

        // `executor_elements_total` is one of the keys SnapshotRecorder maps
        // onto a structured field, so we can read it back from the snapshot.
        let key = Key::from_name("executor_elements_total");
        let counter = recorder.register_counter(&key, &meta());
        counter.increment(42);

        // Snapshot (JSON API) side saw it.
        assert_eq!(snap_handle.snapshot().elements_total, 42);

        // Prometheus (text /metrics) side saw it too.
        let rendered = prom_handle.render();
        assert!(
            rendered.contains("executor_elements_total"),
            "prometheus output missing metric: {rendered}"
        );
        assert!(
            rendered.contains("42"),
            "prometheus output missing value: {rendered}"
        );
    }

    #[test]
    fn counter_absolute_reaches_snapshot_backend() {
        let (recorder, _prom_handle, snap_handle) = fanout();
        let key = Key::from_name("executor_elements_total");
        let counter = recorder.register_counter(&key, &meta());
        counter.absolute(100);
        assert_eq!(snap_handle.snapshot().elements_total, 100);
    }

    #[test]
    fn register_gauge_and_histogram_do_not_panic() {
        // Registration + emission must fan out cleanly even for metric types
        // the snapshot side does not surface as named fields.
        let (recorder, _prom_handle, _snap_handle) = fanout();

        let gauge = recorder.register_gauge(&Key::from_name("some_gauge"), &meta());
        gauge.set(1.5);
        gauge.increment(0.5);
        gauge.decrement(0.25);

        let hist = recorder.register_histogram(&Key::from_name("some_hist"), &meta());
        hist.record(3.0);
    }

    #[test]
    fn describe_calls_fan_out_without_panicking() {
        let (recorder, _prom_handle, _snap_handle) = fanout();
        recorder.describe_counter("c".into(), None, "a counter".into());
        recorder.describe_gauge("g".into(), None, "a gauge".into());
        recorder.describe_histogram("h".into(), None, "a histogram".into());
    }
}
