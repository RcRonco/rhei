//! Decoupled log capture layer for dashboards, web UI, and log aggregation.
//!
//! [`CapturingLayer`] implements `tracing_subscriber::Layer`, forwarding
//! structured log events to a bounded `mpsc` channel as [`LogEntry`] values.
//! Consumers receive logs without coupling to any specific rendering framework.

use std::time::SystemTime;

use tracing::Level;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// A captured log event with timestamp, level, target, and message.
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
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();

        // Collect span fields (e.g. worker=0) from the current span scope
        let mut span_prefix = String::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let exts = span.extensions();
                if let Some(fields) = exts.get::<SpanFields>()
                    && !fields.0.is_empty()
                {
                    if !span_prefix.is_empty() {
                        span_prefix.push(' ');
                    }
                    span_prefix.push_str(&fields.0);
                }
            }
        }

        // Extract message from the event's fields
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);

        let message = if span_prefix.is_empty() {
            visitor.0
        } else {
            format!("[{span_prefix}] {}", visitor.0)
        };

        let entry = LogEntry {
            timestamp: SystemTime::now(),
            level: *metadata.level(),
            target: metadata.target().to_string(),
            message,
        };

        // Non-blocking send — drop if channel full
        let _ = self.tx.try_send(entry);
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut visitor = SpanFieldVisitor(String::new());
        attrs.record(&mut visitor);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields(visitor.0));
        }
    }
}

/// Stored on each span to hold its formatted fields.
struct SpanFields(String);

/// Visitor that formats span fields as `key=value` pairs.
struct SpanFieldVisitor(String);

impl tracing::field::Visit for SpanFieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(self.0, "{}={value:?}", field.name());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        use std::fmt::Write;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(self.0, "{}={value}", field.name());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(self.0, "{}={value}", field.name());
    }
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
mod tests {
    use super::*;

    #[test]
    fn layer_creation() {
        let (layer, _rx) = CapturingLayer::new(100);
        // Just verify it creates without panic
        assert!(!format!("{layer:?}").is_empty());
    }
}
