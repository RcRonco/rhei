//! Dead-letter queue types for failed record handling.

use serde::{Deserialize, Serialize};

use crate::traits::Sink;

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

/// Factory that creates per-worker DLQ sink instances.
///
/// Each call to [`create`](DlqSinkFactory::create) returns a fresh sink for the
/// given worker index, enabling lock-free per-worker DLQ writes.
pub trait DlqSinkFactory: Send + Sync + std::fmt::Debug {
    /// Create a new DLQ sink for the given worker index.
    fn create(
        &self,
        worker_index: usize,
    ) -> anyhow::Result<Box<dyn Sink<Input = DeadLetterRecord>>>;
}

/// Factory that creates [`DlqFileSink`](crate::connectors::dlq_file_sink::DlqFileSink)
/// instances per worker.
///
/// Appends `-{worker_index}` to the base path so each worker writes to its own file,
/// avoiding contention.
#[derive(Debug, Clone)]
pub struct DlqFileSinkFactory {
    base_path: std::path::PathBuf,
}

impl DlqFileSinkFactory {
    /// Create a new factory with the given base path.
    ///
    /// Each worker's file will be named `{base_path}-{worker_index}`.
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_path: path.into(),
        }
    }
}

impl DlqSinkFactory for DlqFileSinkFactory {
    fn create(
        &self,
        worker_index: usize,
    ) -> anyhow::Result<Box<dyn Sink<Input = DeadLetterRecord>>> {
        use crate::connectors::dlq_file_sink::DlqFileSink;

        let mut path = self.base_path.as_os_str().to_owned();
        path.push(format!("-{worker_index}"));
        let sink = DlqFileSink::open(std::path::PathBuf::from(path))?;
        Ok(Box::new(sink))
    }
}

/// Policy for handling operator errors.
#[derive(Debug, Default)]
pub enum ErrorPolicy {
    /// Skip the failed element and log a warning (default).
    #[default]
    Skip,
    /// Write failed elements to a DLQ sink and continue.
    ///
    /// The factory creates per-worker sink instances, enabling lock-free writes.
    DeadLetter(Box<dyn DlqSinkFactory>),
}

impl ErrorPolicy {
    /// Convenience constructor for a file-backed DLQ.
    ///
    /// Each worker writes to `{path}-{worker_index}`.
    pub fn dead_letter_file(path: impl Into<std::path::PathBuf>) -> Self {
        Self::DeadLetter(Box::new(DlqFileSinkFactory::new(path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dlq_file_sink_factory_creates_sinks() {
        let dir =
            std::env::temp_dir().join(format!("rhei_dlq_factory_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let base_path = dir.join("dlq.jsonl");
        let factory = DlqFileSinkFactory::new(&base_path);

        let sink0 = factory.create(0);
        assert!(sink0.is_ok(), "factory should create sink for worker 0");

        let sink1 = factory.create(1);
        assert!(sink1.is_ok(), "factory should create sink for worker 1");

        // Verify separate files were created.
        let path0 = format!("{}-0", base_path.display());
        let path1 = format!("{}-1", base_path.display());
        assert!(
            std::path::Path::new(&path0).exists(),
            "worker 0 file should exist"
        );
        assert!(
            std::path::Path::new(&path1).exists(),
            "worker 1 file should exist"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn error_policy_dead_letter_file_convenience() {
        let policy = ErrorPolicy::dead_letter_file("/tmp/test-dlq.jsonl");
        assert!(matches!(policy, ErrorPolicy::DeadLetter(_)));
    }
}
