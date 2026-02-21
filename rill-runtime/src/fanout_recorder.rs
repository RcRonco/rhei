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
