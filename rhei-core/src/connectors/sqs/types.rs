use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A message received from an Amazon SQS queue.
///
/// The `receipt_handle` is the token used to delete the message (acknowledge it)
/// or to change its visibility timeout. It is only valid until the message's
/// visibility timeout expires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    /// The URL of the queue this message was received from.
    pub queue_url: String,
    /// The SQS-assigned unique message ID.
    pub message_id: String,
    /// The receipt handle used to delete the message or extend its visibility.
    pub receipt_handle: String,
    /// The message body (SQS bodies are UTF-8 text).
    pub body: String,
    /// The FIFO message group ID, if the source queue is a FIFO queue.
    pub group_id: Option<String>,
    /// `SentTimestamp` system attribute, in milliseconds since the Unix epoch.
    pub sent_timestamp: Option<i64>,
    /// `ApproximateReceiveCount` system attribute (how many times this message
    /// has been received without being deleted).
    pub receive_count: Option<i64>,
    /// User-defined string message attributes.
    pub attributes: BTreeMap<String, String>,
}

/// A record to be sent to an Amazon SQS queue.
///
/// `group_id` and `dedup_id` are only meaningful for FIFO queues; standard
/// queues ignore them. `delay_seconds` is only meaningful for standard queues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsRecord {
    /// The message body.
    pub body: String,
    /// FIFO `MessageGroupId`. Required for FIFO queues; ignored otherwise.
    pub group_id: Option<String>,
    /// FIFO `MessageDeduplicationId`. Used to suppress duplicate sends within
    /// the queue's 5-minute deduplication window. Ignored for standard queues
    /// and for FIFO queues with content-based deduplication enabled.
    pub dedup_id: Option<String>,
    /// Per-message delay in seconds (standard queues only, 0-900).
    pub delay_seconds: Option<i32>,
}

impl SqsRecord {
    /// Create a new record carrying just a body.
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            group_id: None,
            dedup_id: None,
            delay_seconds: None,
        }
    }

    /// Set the FIFO message group ID (builder pattern).
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Set the FIFO deduplication ID (builder pattern).
    pub fn with_dedup_id(mut self, dedup_id: impl Into<String>) -> Self {
        self.dedup_id = Some(dedup_id.into());
        self
    }

    /// Set the per-message delay in seconds (builder pattern).
    pub fn with_delay_seconds(mut self, delay_seconds: i32) -> Self {
        self.delay_seconds = Some(delay_seconds);
        self
    }
}
