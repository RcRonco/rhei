//! Decoupled log capture layer for dashboards, web UI, and log aggregation.
//!
//! [`CapturingLayer`] implements `tracing_subscriber::Layer`, forwarding
//! structured log events to a bounded `mpsc` channel as [`LogEntry`] values.
//! Consumers receive logs without coupling to any specific rendering framework.
//!
//! Worker context is automatically captured from tracing spans. Any span with
//! a `worker` field (e.g. `tracing::info_span!("worker", worker = 0)`) will
//! tag all events within it with the worker index.

use std::time::SystemTime;

use tracing::Level;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// A captured log event with timestamp, level, target, message, and optional worker.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// When the event was recorded.
    pub timestamp: SystemTime,
    /// Severity level (ERROR, WARN, INFO, DEBUG, TRACE).
    pub level: Level,
    /// The module/target that emitted the event.
    pub target: String,
    /// The formatted message.
    pub message: String,
    /// Worker index if the event was emitted within a worker span.
    pub worker: Option<usize>,
}

/// Extension stored on spans to carry worker identity through the span tree.
struct WorkerInfo {
    worker: usize,
}

/// A `tracing_subscriber::Layer` that captures events into a bounded channel.
///
/// Uses non-blocking `try_send` — when the channel is full, the oldest events
/// have already been consumed and new ones are dropped (backpressure).
#[derive(Debug)]
pub struct CapturingLayer {
    tx: tokio::sync::mpsc::Sender<LogEntry>,
}

impl CapturingLayer {
    /// Create a new capturing layer with the given buffer capacity.
    ///
    /// Returns the layer (to be added to a subscriber) and a receiver
    /// for consuming captured log entries.
    pub fn new(buffer: usize) -> (Self, tokio::sync::mpsc::Receiver<LogEntry>) {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        (Self { tx }, rx)
    }
}

impl<S> Layer<S> for CapturingLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut visitor = WorkerVisitor(None);
        attrs.record(&mut visitor);
        if let Some(worker) = visitor.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(WorkerInfo { worker });
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();

        // Extract message from the event's fields
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);

        // Walk the span tree to find worker context
        let worker = ctx.event_span(event).and_then(|span| {
            for s in span.scope() {
                if let Some(info) = s.extensions().get::<WorkerInfo>() {
                    return Some(info.worker);
                }
            }
            None
        });

        let entry = LogEntry {
            timestamp: SystemTime::now(),
            level: *metadata.level(),
            target: metadata.target().to_string(),
            message: visitor.0,
            worker,
        };

        // Non-blocking send — drop if channel full
        let _ = self.tx.try_send(entry);
    }
}

/// Visitor that extracts a `worker` field (u64) from span attributes.
struct WorkerVisitor(Option<usize>);

impl tracing::field::Visit for WorkerVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "worker" {
            #[allow(clippy::cast_possible_truncation)]
            {
                self.0 = Some(value as usize);
            }
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

/// Visitor that extracts the `message` field from a tracing event.
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        } else if self.0.is_empty() {
            self.0 = format!("{}: {value:?}", field.name());
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write;
        if field.name() == "message" {
            self.0 = value.to_string();
        } else if self.0.is_empty() {
            self.0 = format!("{}: {value}", field.name());
        } else {
            let _ = write!(self.0, " {}={value}", field.name());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn layer_creation() {
        let (layer, _rx) = CapturingLayer::new(100);
        // Just verify it creates without panic
        assert!(!format!("{layer:?}").is_empty());
    }
}
