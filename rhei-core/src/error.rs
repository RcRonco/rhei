//! Classified pipeline errors for structured error handling and DLQ routing.
//!
//! [`PipelineError`] categorizes operator failures into three classes:
//!
//! - [`Retriable`](PipelineError::Retriable) — transient failures that may succeed on retry
//!   (network timeouts, temporary unavailability)
//! - [`BadData`](PipelineError::BadData) — the input data is malformed and will never succeed
//!   (parse errors, schema violations, missing required fields)
//! - [`SystemError`](PipelineError::SystemError) — infrastructure failures that require
//!   operator intervention (disk full, permission denied, configuration error)
//!
//! # Structured JSON format
//!
//! [`StructuredError`] provides a JSON-serializable envelope for DLQ records,
//! including classification, timestamps, and operator context.
//!
//! # Example
//!
//! ```
//! use rhei_core::error::{PipelineError, ErrorClassification};
//!
//! // In an operator's process() method:
//! fn validate(input: &str) -> Result<(), PipelineError> {
//!     if input.is_empty() {
//!         return Err(PipelineError::bad_data("input must not be empty"));
//!     }
//!     Ok(())
//! }
//!
//! let err = validate("").unwrap_err();
//! assert_eq!(err.classification(), ErrorClassification::BadData);
//! assert!(!err.is_retriable());
//! ```

use serde::{Deserialize, Serialize};

/// Classification of pipeline errors for routing and retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClassification {
    /// Transient failure — may succeed on retry.
    Retriable,
    /// The input data is invalid — will never succeed, route to DLQ.
    BadData,
    /// Infrastructure or configuration failure — requires operator intervention.
    SystemError,
}

impl std::fmt::Display for ErrorClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retriable => write!(f, "retriable"),
            Self::BadData => write!(f, "bad_data"),
            Self::SystemError => write!(f, "system_error"),
        }
    }
}

/// A classified pipeline error.
///
/// Wraps an underlying error with a [`classification`](ErrorClassification) so
/// the runtime can make routing and retry decisions:
///
/// - `Retriable` errors are retried (up to a configured limit)
/// - `BadData` errors are routed to the dead-letter queue
/// - `SystemError` errors halt the pipeline (or route to DLQ based on policy)
#[derive(Debug)]
pub struct PipelineError {
    classification: ErrorClassification,
    source: anyhow::Error,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.classification, self.source)
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl PipelineError {
    /// Create a retriable error from a message string.
    ///
    /// Use for transient failures: network timeouts, temporary unavailability,
    /// rate limiting, connection resets.
    pub fn retriable(msg: impl std::fmt::Display) -> Self {
        Self {
            classification: ErrorClassification::Retriable,
            source: anyhow::anyhow!("{msg}"),
        }
    }

    /// Create a bad-data error from a message string.
    ///
    /// Use for permanent data issues: parse errors, schema violations,
    /// missing required fields, invalid ranges.
    pub fn bad_data(msg: impl std::fmt::Display) -> Self {
        Self {
            classification: ErrorClassification::BadData,
            source: anyhow::anyhow!("{msg}"),
        }
    }

    /// Create a system error from a message string.
    ///
    /// Use for infrastructure failures: disk full, permission denied,
    /// configuration errors, state corruption.
    pub fn system_error(msg: impl std::fmt::Display) -> Self {
        Self {
            classification: ErrorClassification::SystemError,
            source: anyhow::anyhow!("{msg}"),
        }
    }

    /// Create a retriable error wrapping an existing error.
    pub fn retriable_err(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            classification: ErrorClassification::Retriable,
            source: anyhow::Error::new(err),
        }
    }

    /// Create a bad-data error wrapping an existing error.
    pub fn bad_data_err(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            classification: ErrorClassification::BadData,
            source: anyhow::Error::new(err),
        }
    }

    /// Create a system error wrapping an existing error.
    pub fn system_error_err(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            classification: ErrorClassification::SystemError,
            source: anyhow::Error::new(err),
        }
    }

    /// Returns the error classification.
    pub fn classification(&self) -> ErrorClassification {
        self.classification
    }

    /// Returns `true` if this error might succeed on retry.
    pub fn is_retriable(&self) -> bool {
        self.classification == ErrorClassification::Retriable
    }

    /// Returns `true` if this error represents invalid input data.
    pub fn is_bad_data(&self) -> bool {
        self.classification == ErrorClassification::BadData
    }

    /// Returns `true` if this error represents a system/infra failure.
    pub fn is_system_error(&self) -> bool {
        self.classification == ErrorClassification::SystemError
    }

    /// Unwrap the inner `anyhow::Error`.
    pub fn into_inner(self) -> anyhow::Error {
        self.source
    }
}

/// Structured error format for DLQ records and logging.
///
/// JSON-serializable envelope containing classification, operator context,
/// and a best-effort representation of the failed input. Suitable for
/// writing to Kafka DLQ topics or structured log aggregation.
///
/// # JSON example
///
/// ```json
/// {
///   "classification": "bad_data",
///   "operator": "json_parser",
///   "error": "expected value at line 1 column 1",
///   "input_repr": "{invalid json",
///   "timestamp": "1710499800",
///   "pipeline": "clickstream-ingest",
///   "worker_index": 2
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredError {
    /// Error classification for routing decisions.
    pub classification: ErrorClassification,
    /// The operator that produced the error.
    pub operator: String,
    /// Human-readable error message.
    pub error: String,
    /// Best-effort debug representation of the failed input.
    pub input_repr: String,
    /// Unix epoch seconds (as a string) of when the error occurred.
    pub timestamp: String,
    /// Optional pipeline name for multi-pipeline environments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Worker index that produced the error, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_index: Option<usize>,
}

impl StructuredError {
    /// Create a new structured error with the required fields.
    ///
    /// Timestamp is set to the current time automatically.
    pub fn new(
        classification: ErrorClassification,
        operator: impl Into<String>,
        error: impl Into<String>,
        input_repr: impl Into<String>,
    ) -> Self {
        let timestamp = {
            let d = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            // Unix epoch seconds as a string (no chrono dependency)
            format!("{}", d.as_secs())
        };
        Self {
            classification,
            operator: operator.into(),
            error: error.into(),
            input_repr: input_repr.into(),
            timestamp,
            pipeline: None,
            worker_index: None,
        }
    }

    /// Set the pipeline name.
    pub fn with_pipeline(mut self, name: impl Into<String>) -> Self {
        self.pipeline = Some(name.into());
        self
    }

    /// Set the worker index.
    pub fn with_worker_index(mut self, idx: usize) -> Self {
        self.worker_index = Some(idx);
        self
    }

    /// Serialize to a JSON string.
    ///
    /// Returns the JSON bytes suitable for writing to a DLQ topic.
    pub fn to_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Serialize to a pretty-printed JSON string (for logging).
    pub fn to_json_pretty(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl std::fmt::Display for StructuredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] operator={} error={}",
            self.classification, self.operator, self.error
        )
    }
}

/// Convert a [`PipelineError`] into a [`StructuredError`] with operator context.
impl StructuredError {
    /// Create from a [`PipelineError`] with operator and input context.
    pub fn from_pipeline_error(
        err: &PipelineError,
        operator: impl Into<String>,
        input_repr: impl Into<String>,
    ) -> Self {
        Self::new(err.classification(), operator, err.to_string(), input_repr)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_error_classification() {
        let retriable = PipelineError::retriable("timeout");
        assert!(retriable.is_retriable());
        assert!(!retriable.is_bad_data());
        assert_eq!(retriable.classification(), ErrorClassification::Retriable);

        let bad = PipelineError::bad_data("invalid JSON");
        assert!(bad.is_bad_data());
        assert!(!bad.is_retriable());

        let sys = PipelineError::system_error("disk full");
        assert!(sys.is_system_error());
    }

    #[test]
    fn pipeline_error_display() {
        let err = PipelineError::bad_data("missing field 'user_id'");
        let msg = err.to_string();
        assert!(msg.contains("bad_data"));
        assert!(msg.contains("missing field"));
    }

    #[test]
    fn structured_error_json_roundtrip() {
        let err = StructuredError::new(
            ErrorClassification::BadData,
            "json_parser",
            "invalid JSON at line 1",
            r#"{"broken"#,
        )
        .with_pipeline("clickstream")
        .with_worker_index(2);

        let json = err.to_json().unwrap();
        let restored: StructuredError = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.classification, ErrorClassification::BadData);
        assert_eq!(restored.operator, "json_parser");
        assert_eq!(restored.pipeline.as_deref(), Some("clickstream"));
        assert_eq!(restored.worker_index, Some(2));
    }

    #[test]
    fn structured_error_from_pipeline_error() {
        let pe = PipelineError::retriable("connection refused");
        let se = StructuredError::from_pipeline_error(&pe, "kafka_sink", "record#42");
        assert_eq!(se.classification, ErrorClassification::Retriable);
        assert_eq!(se.operator, "kafka_sink");
        assert!(se.error.contains("connection refused"));
    }

    #[test]
    fn error_classification_serde() {
        let json = serde_json::to_string(&ErrorClassification::BadData).unwrap();
        assert_eq!(json, r#""bad_data""#);
        let restored: ErrorClassification = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, ErrorClassification::BadData);
    }

    #[test]
    fn pipeline_error_into_anyhow() {
        let pe = PipelineError::bad_data("schema violation");
        let anyhow_err: anyhow::Error = pe.into();
        assert!(anyhow_err.to_string().contains("schema violation"));
    }
}
