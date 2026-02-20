use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, TopicPartitionList};

use super::kafka_types::KafkaMessage;
use crate::traits::Source;

/// A Kafka consumer that implements the [`Source`] trait.
///
/// Polls messages from one or more Kafka topics and emits [`KafkaMessage`] batches.
/// Offset commits are deferred until [`Source::on_checkpoint_complete`] is called,
/// ensuring at-least-once delivery when combined with Rill's checkpoint mechanism.
pub struct KafkaSource {
    consumer: StreamConsumer,
    batch_size: usize,
    poll_timeout: Duration,
    tracked_offsets: HashMap<(String, i32), i64>,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("batch_size", &self.batch_size)
            .field("poll_timeout", &self.poll_timeout)
            .field("tracked_offsets", &self.tracked_offsets)
            .field("watermark_interval", &self.watermark_interval)
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Create a new `KafkaSource` that consumes from the given topics.
    ///
    /// Uses sensible defaults: `enable.auto.commit = false`,
    /// `auto.offset.reset = earliest`, batch size 100, poll timeout 100ms.
    pub fn new(brokers: &str, group_id: &str, topics: &[&str]) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(topics)?;

        Ok(Self {
            consumer,
            batch_size: 100,
            poll_timeout: Duration::from_millis(100),
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
        })
    }

    /// Create a `KafkaSource` from an already-configured [`StreamConsumer`].
    ///
    /// The caller is responsible for subscribing to topics and setting
    /// `enable.auto.commit = false`.
    pub fn from_consumer(consumer: StreamConsumer) -> Self {
        Self {
            consumer,
            batch_size: 100,
            poll_timeout: Duration::from_millis(100),
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
        }
    }

    /// Set the maximum number of messages per batch (default: 100).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the poll timeout (default: 100ms).
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the watermark interval in records (default: 1000).
    pub fn with_watermark_interval(mut self, interval: usize) -> Self {
        self.watermark_interval = interval;
        self
    }
}

#[async_trait]
impl Source for KafkaSource {
    type Output = KafkaMessage;

    async fn next_batch(&mut self) -> Option<Vec<KafkaMessage>> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let deadline = tokio::time::Instant::now() + self.poll_timeout;

        while batch.len() < self.batch_size {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let topic = msg.topic().to_owned();
                    let partition = msg.partition();
                    let offset = msg.offset();

                    let kafka_msg = KafkaMessage {
                        topic: topic.clone(),
                        partition,
                        offset,
                        key: msg.key().map(<[u8]>::to_vec),
                        payload: msg.payload().map(<[u8]>::to_vec),
                        timestamp: msg.timestamp().to_millis(),
                    };

                    self.tracked_offsets.insert((topic, partition), offset);
                    metrics::counter!("kafka_messages_consumed_total").increment(1);
                    batch.push(kafka_msg);
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "kafka consumer error");
                    break;
                }
                Err(_) => {
                    // Timeout — return whatever we have
                    break;
                }
            }
        }

        self.records_since_watermark += batch.len();
        if self.records_since_watermark >= self.watermark_interval {
            self.watermark_pending = true;
            self.records_since_watermark = 0;
        }

        // Kafka is unbounded — always return Some, even if empty
        Some(batch)
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        if self.tracked_offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.tracked_offsets {
            // Kafka convention: committed offset = next offset to read
            tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(offset + 1))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
        metrics::counter!("kafka_consumer_offsets_committed_total").increment(1);
        tracing::debug!(partitions = self.tracked_offsets.len(), "committed offsets");
        self.tracked_offsets.clear();

        Ok(())
    }
}
