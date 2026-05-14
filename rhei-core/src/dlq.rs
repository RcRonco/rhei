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
