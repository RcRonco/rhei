//! Dead-letter queue types for failed record handling.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A record that failed operator processing and was routed to the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    /// Best-effort debug representation of the input element.
    pub input_repr: String,
    /// The operator name that produced the error.
    pub operator_name: String,
    /// The error message.
    pub error: String,
    /// ISO-8601 timestamp of when the failure occurred.
    pub timestamp: String,
}

/// Trait for DLQ sink implementations.
///
/// Implement this to route dead-letter records to any backend
/// (file, Kafka, HTTP, etc.).
#[async_trait]
pub trait DlqSink: Send + 'static {
    /// Write a single dead-letter record.
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()>;
    /// Flush any buffered records. Called on shutdown.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Policy for handling operator errors.
#[derive(Debug, Default)]
pub enum ErrorPolicy {
    /// Skip the failed element and log a warning (default).
    #[default]
    Skip,
    /// Route failed records to a DLQ sink (set via controller builder).
    SendToDlq,
}

/// A file-based DLQ sink that writes newline-delimited JSON to a file.
#[derive(Debug)]
pub struct FileDlqSink {
    writer: std::io::BufWriter<std::fs::File>,
}

impl FileDlqSink {
    /// Create a new file DLQ sink at the given path.
    ///
    /// Creates or truncates the file.
    pub fn new(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self {
            writer: std::io::BufWriter::new(file),
        })
    }
}

#[async_trait]
impl DlqSink for FileDlqSink {
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()> {
        use std::io::Write;
        let json = serde_json::to_string(&record)?;
        writeln!(self.writer, "{json}")?;
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        use std::io::Write;
        self.writer.flush()?;
        Ok(())
    }
}

/// A logging DLQ sink that writes records via `tracing::error!`.
#[derive(Debug)]
pub struct LogDlqSink;

#[async_trait]
impl DlqSink for LogDlqSink {
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()> {
        tracing::error!(
            operator = %record.operator_name,
            error = %record.error,
            input = %record.input_repr,
            timestamp = %record.timestamp,
            "DLQ record"
        );
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn record(n: u32) -> DeadLetterRecord {
        DeadLetterRecord {
            input_repr: format!("input-{n}"),
            operator_name: "map".to_string(),
            error: format!("boom-{n}"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn error_policy_defaults_to_skip() {
        assert!(matches!(ErrorPolicy::default(), ErrorPolicy::Skip));
    }

    #[test]
    fn dead_letter_record_serde_round_trips() {
        let original = record(7);
        let json = serde_json::to_string(&original).unwrap();
        let parsed: DeadLetterRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.input_repr, original.input_repr);
        assert_eq!(parsed.operator_name, original.operator_name);
        assert_eq!(parsed.error, original.error);
        assert_eq!(parsed.timestamp, original.timestamp);
    }

    #[tokio::test]
    async fn file_sink_writes_newline_delimited_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.ndjson");

        let mut sink = FileDlqSink::new(&path).unwrap();
        sink.write(record(1)).await.unwrap();
        sink.write(record(2)).await.unwrap();
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON object per line");

        // Each line is independently parseable (true NDJSON).
        let first: DeadLetterRecord = serde_json::from_str(lines[0]).unwrap();
        let second: DeadLetterRecord = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(first.input_repr, "input-1");
        assert_eq!(second.error, "boom-2");
    }

    #[tokio::test]
    async fn file_sink_truncates_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.ndjson");
        std::fs::write(&path, "stale pre-existing content\n").unwrap();

        let mut sink = FileDlqSink::new(&path).unwrap();
        sink.write(record(9)).await.unwrap();
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(!contents.contains("stale"), "file must be truncated on open");
        assert_eq!(contents.lines().count(), 1);
    }

    #[tokio::test]
    async fn log_sink_write_succeeds() {
        let mut sink = LogDlqSink;
        sink.write(record(0)).await.unwrap();
        // Default flush is a no-op that must still succeed.
        sink.flush().await.unwrap();
    }
}
