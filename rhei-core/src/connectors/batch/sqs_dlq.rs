//! Amazon SQS-backed dead-letter queue sink.
//!
//! Implements [`DlqSink`] by sending [`DeadLetterRecord`]s as JSON message
//! bodies to an SQS queue. This is a rhei-level DLQ for records that fail
//! operator processing; it is independent of SQS's own server-side redrive
//! policy (which moves messages to a DLQ after `maxReceiveCount` failed
//! receives).

use async_trait::async_trait;
use aws_sdk_sqs::Client;

use crate::dlq::{DeadLetterRecord, DlqSink};

/// A DLQ sink that sends dead-letter records to an SQS queue.
pub struct SqsDlqSink {
    client: Client,
    queue_url: String,
    /// FIFO group ID, applied when the target queue is a FIFO queue.
    group_id: Option<String>,
}

impl std::fmt::Debug for SqsDlqSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsDlqSink")
            .field("queue_url", &self.queue_url)
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

impl SqsDlqSink {
    /// Create a DLQ sink from an existing SQS client and queue URL.
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
            group_id: None,
        }
    }

    /// Build a DLQ sink using AWS credentials and region from the environment.
    pub async fn connect(queue_url: impl Into<String>) -> anyhow::Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self::new(Client::new(&config), queue_url))
    }

    /// Set a FIFO message group ID for the DLQ queue.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }
}

#[async_trait]
impl DlqSink for SqsDlqSink {
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()> {
        let body = serde_json::to_string(&record)?;
        let mut req = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body);
        if let Some(g) = &self.group_id {
            req = req
                .message_group_id(g)
                .message_deduplication_id(format!("{}-{}", record.operator_name, record.timestamp));
        }
        req.send()
            .await
            .map_err(|e| anyhow::anyhow!("SQS DLQ send error: {e}"))?;
        Ok(())
    }
}
