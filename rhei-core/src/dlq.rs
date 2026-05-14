//! Dead-letter queue types for failed record handling.

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

/// Policy for handling operator errors.
#[derive(Debug, Default)]
pub enum ErrorPolicy {
    /// Skip the failed element and log a warning (default).
    #[default]
    Skip,
    /// Route failed records to a dead-letter queue file.
    SendToDlq,
}
