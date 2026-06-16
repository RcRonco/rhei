//! Batch Amazon SQS source that produces `RheiBuffer<SqsMessage>`.
//!
//! Uses long polling to receive messages and defers deletion (acknowledgement)
//! until a checkpoint completes, giving at-least-once delivery: a message is
//! only removed from the queue after the checkpoint that consumed it succeeds.
//! If the process fails before that, the message reappears once its visibility
//! timeout elapses and is reprocessed.

use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};

use crate::arrow::{RheiBuffer, RheiBuilder, RheiSchema, Source};
use crate::connectors::sqs::types::SqsMessage;

/// Maximum messages SQS returns from a single `ReceiveMessage` call.
const SQS_MAX_RECEIVE: i32 = 10;
/// Maximum entries SQS accepts in a single `DeleteMessageBatch` call.
const SQS_MAX_DELETE_BATCH: usize = 10;

/// A batch SQS source that produces Arrow buffers of [`SqsMessage`].
pub struct SqsSource {
    client: Client,
    queue_url: String,
    batch_size: usize,
    visibility_timeout: i32,
    wait_time_seconds: i32,
    /// Receipt handles received since the last checkpoint, deleted on commit.
    pending_handles: Vec<String>,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
    max_timestamp: Option<i64>,
    allowed_lateness_ms: u64,
}

impl std::fmt::Debug for SqsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsSource")
            .field("queue_url", &self.queue_url)
            .field("batch_size", &self.batch_size)
            .field("visibility_timeout", &self.visibility_timeout)
            .field("wait_time_seconds", &self.wait_time_seconds)
            .field("pending_handles", &self.pending_handles.len())
            .finish_non_exhaustive()
    }
}

impl SqsSource {
    /// Create a source from an existing SQS client and queue URL.
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
            batch_size: 1024,
            visibility_timeout: 30,
            wait_time_seconds: 20,
            pending_handles: Vec::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
            max_timestamp: None,
            allowed_lateness_ms: 0,
        }
    }

    /// Build a source using AWS credentials and region from the environment.
    pub async fn connect(queue_url: impl Into<String>) -> anyhow::Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self::new(Client::new(&config), queue_url))
    }

    /// Set the maximum number of messages accumulated per batch (default: 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the visibility timeout in seconds applied to received messages
    /// (default: 30). Must exceed the time between checkpoints, otherwise
    /// in-flight messages become visible again and are reprocessed.
    pub fn with_visibility_timeout(mut self, seconds: i32) -> Self {
        self.visibility_timeout = seconds;
        self
    }

    /// Set the long-poll wait time in seconds (default: 20, the SQS maximum).
    pub fn with_wait_time_seconds(mut self, seconds: i32) -> Self {
        self.wait_time_seconds = seconds.clamp(0, 20);
        self
    }

    /// Set the watermark interval in records (default: 1000).
    pub fn with_watermark_interval(mut self, interval: usize) -> Self {
        self.watermark_interval = interval;
        self
    }

    /// Set the allowed lateness for watermark computation in milliseconds.
    pub fn with_allowed_lateness(mut self, ms: u64) -> Self {
        self.allowed_lateness_ms = ms;
        self
    }

    /// Receive messages until `batch_size` is reached or the queue is drained.
    ///
    /// Only the first `ReceiveMessage` call long-polls; subsequent calls use a
    /// zero wait time so a partially-full batch is returned promptly rather than
    /// blocking for the full long-poll window per call.
    async fn poll_messages(&mut self) -> anyhow::Result<Vec<SqsMessage>> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut first = true;

        while batch.len() < self.batch_size {
            let remaining = self.batch_size - batch.len();
            let want = i32::try_from(remaining)
                .unwrap_or(SQS_MAX_RECEIVE)
                .clamp(1, SQS_MAX_RECEIVE);
            let wait_secs = if first { self.wait_time_seconds } else { 0 };
            first = false;

            let resp = self
                .client
                .receive_message()
                .queue_url(&self.queue_url)
                .max_number_of_messages(want)
                .visibility_timeout(self.visibility_timeout)
                .wait_time_seconds(wait_secs)
                .message_system_attribute_names(MessageSystemAttributeName::All)
                .message_attribute_names("All")
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("SQS receive error: {e}"))?;

            let messages = resp.messages.unwrap_or_default();
            let got = messages.len();
            for msg in messages {
                let converted = self.convert(msg);
                if let Some(handle) = converted.1 {
                    self.pending_handles.push(handle);
                }
                batch.push(converted.0);
            }

            if got < usize::try_from(want).unwrap_or(usize::MAX) {
                break;
            }
        }

        Ok(batch)
    }

    /// Convert an SDK message into an [`SqsMessage`], returning the receipt
    /// handle separately so it can be tracked for deletion.
    fn convert(&mut self, msg: aws_sdk_sqs::types::Message) -> (SqsMessage, Option<String>) {
        let receipt_handle = msg.receipt_handle().unwrap_or_default().to_string();

        let mut group_id = None;
        let mut sent_timestamp = None;
        let mut receive_count = None;
        if let Some(attrs) = msg.attributes() {
            if let Some(ts) = attrs
                .get(&MessageSystemAttributeName::SentTimestamp)
                .and_then(|v| v.parse::<i64>().ok())
            {
                sent_timestamp = Some(ts);
                match self.max_timestamp {
                    Some(prev) if ts > prev => self.max_timestamp = Some(ts),
                    None => self.max_timestamp = Some(ts),
                    _ => {}
                }
            }
            receive_count = attrs
                .get(&MessageSystemAttributeName::ApproximateReceiveCount)
                .and_then(|v| v.parse::<i64>().ok());
            group_id = attrs
                .get(&MessageSystemAttributeName::MessageGroupId)
                .cloned();
        }

        let attributes = msg
            .message_attributes()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.string_value().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let message = SqsMessage {
            queue_url: self.queue_url.clone(),
            message_id: msg.message_id().unwrap_or_default().to_string(),
            receipt_handle: receipt_handle.clone(),
            body: msg.body().unwrap_or_default().to_string(),
            group_id,
            sent_timestamp,
            receive_count,
            attributes,
        };

        let handle = if receipt_handle.is_empty() {
            None
        } else {
            Some(receipt_handle)
        };
        (message, handle)
    }
}

#[async_trait]
impl Source for SqsSource {
    type Output = SqsMessage;

    async fn next_batch(&mut self) -> Option<RheiBuffer<SqsMessage>> {
        // SQS is an unbounded source — never return None (which signals
        // exhaustion). Retry empty polls until data arrives; the bridge's
        // shutdown handle terminates the loop externally.
        loop {
            let messages = match self.poll_messages().await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "SQS receive failed; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            if messages.is_empty() {
                continue;
            }

            let count = messages.len();
            let mut builder = SqsMessage::builder(count);
            for msg in messages {
                builder.append(msg);
            }

            self.records_since_watermark += count;
            if self.records_since_watermark >= self.watermark_interval {
                self.watermark_pending = true;
                self.records_since_watermark = 0;
            }

            return Some(RheiBuffer::from_builder(builder));
        }
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
    }

    fn current_watermark(&self) -> Option<u64> {
        self.max_timestamp.map(|ts| {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
            let wm = (ts - self.allowed_lateness_ms as i64).max(0) as u64;
            wm
        })
    }

    /// Delete (acknowledge) all messages received since the last checkpoint.
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        if self.pending_handles.is_empty() {
            return Ok(());
        }

        let handles = std::mem::take(&mut self.pending_handles);
        for chunk in handles.chunks(SQS_MAX_DELETE_BATCH) {
            let mut req = self
                .client
                .delete_message_batch()
                .queue_url(&self.queue_url);
            for (i, handle) in chunk.iter().enumerate() {
                let entry = aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(handle)
                    .build()
                    .map_err(|e| anyhow::anyhow!("SQS delete entry build error: {e}"))?;
                req = req.entries(entry);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("SQS delete error: {e}"))?;
            if !resp.failed().is_empty() {
                let ids: Vec<&str> = resp
                    .failed()
                    .iter()
                    .map(aws_sdk_sqs::types::BatchResultErrorEntry::id)
                    .collect();
                return Err(anyhow::anyhow!("SQS delete failed for entries: {ids:?}"));
            }
        }
        Ok(())
    }
}

impl SqsSource {
    /// Fetch the queue's `ApproximateNumberOfMessages` attribute. Useful for
    /// observability and tests against a live queue.
    pub async fn approximate_depth(&self) -> anyhow::Result<Option<i64>> {
        let resp = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("SQS get_queue_attributes error: {e}"))?;
        Ok(resp
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .and_then(|v| v.parse::<i64>().ok()))
    }
}
