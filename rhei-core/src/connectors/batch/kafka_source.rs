//! Batch Kafka source that produces `RheiBuffer<KafkaMessage>` from Kafka topics.
//!
//! Wraps the row-based `KafkaSource` and builds Arrow buffers from consumed messages.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::{ClientConfig, TopicPartitionList};

use crate::arrow::{BatchSource, RheiBuffer, RheiBuilder, RheiSchema};
use crate::connectors::kafka::types::KafkaMessage;

/// A batch Kafka source that produces Arrow buffers of [`KafkaMessage`].
///
/// Polls messages from Kafka topics and builds `RheiBuffer<KafkaMessage>` batches.
/// Offset commits are deferred until checkpoint completes.
pub struct BatchKafkaSource {
    consumer: StreamConsumer,
    batch_size: usize,
    poll_timeout: Duration,
    tracked_offsets: HashMap<(String, i32), i64>,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
    max_timestamp: Option<i64>,
    allowed_lateness_ms: u64,
    brokers: String,
    group_id: String,
    topics: Vec<String>,
}

impl std::fmt::Debug for BatchKafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchKafkaSource")
            .field("batch_size", &self.batch_size)
            .field("poll_timeout", &self.poll_timeout)
            .field("watermark_interval", &self.watermark_interval)
            .finish_non_exhaustive()
    }
}

impl BatchKafkaSource {
    /// Create a new `BatchKafkaSource` that consumes from the given topics.
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
            batch_size: 1024,
            poll_timeout: Duration::from_millis(100),
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
            max_timestamp: None,
            allowed_lateness_ms: 0,
            brokers: brokers.to_owned(),
            group_id: group_id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
        })
    }

    /// Create from an already-configured consumer.
    pub fn from_consumer(consumer: StreamConsumer) -> Self {
        Self {
            consumer,
            batch_size: 1024,
            poll_timeout: Duration::from_millis(100),
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
            max_timestamp: None,
            allowed_lateness_ms: 0,
            brokers: String::new(),
            group_id: String::new(),
            topics: Vec::new(),
        }
    }

    /// Set the maximum number of messages per batch (default: 1024).
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

    /// Set the allowed lateness for watermark computation in milliseconds.
    pub fn with_allowed_lateness(mut self, ms: u64) -> Self {
        self.allowed_lateness_ms = ms;
        self
    }

    async fn poll_messages(&mut self) -> Vec<KafkaMessage> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let deadline = tokio::time::Instant::now() + self.poll_timeout;

        while batch.len() < self.batch_size {
            let remaining = deadline.duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let topic = msg.topic().to_owned();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    self.tracked_offsets
                        .insert((topic.clone(), partition), offset + 1);

                    let timestamp = msg.timestamp().to_millis();
                    if let Some(ts) = timestamp {
                        match self.max_timestamp {
                            Some(prev) if ts > prev => self.max_timestamp = Some(ts),
                            None => self.max_timestamp = Some(ts),
                            _ => {}
                        }
                    }

                    let headers = msg
                        .headers()
                        .map(|h| {
                            (0..h.count())
                                .map(|i| {
                                    let header = h.get(i);
                                    crate::connectors::kafka::types::KafkaHeader {
                                        key: header.key.to_owned(),
                                        value: header.value.unwrap_or_default().to_vec(),
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    batch.push(KafkaMessage {
                        topic,
                        partition,
                        offset,
                        key: msg.key().map(<[u8]>::to_vec),
                        payload: msg.payload().map(<[u8]>::to_vec),
                        timestamp,
                        headers,
                    });
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        batch
    }
}

#[async_trait]
impl BatchSource for BatchKafkaSource {
    type Output = KafkaMessage;

    async fn next_batch(&mut self) -> Option<RheiBuffer<KafkaMessage>> {
        let messages = self.poll_messages().await;
        if messages.is_empty() {
            return None;
        }

        let count = messages.len();
        let mut builder = KafkaMessage::builder(count);
        for msg in messages {
            builder.append(msg);
        }

        self.records_since_watermark += count;
        if self.records_since_watermark >= self.watermark_interval {
            self.watermark_pending = true;
            self.records_since_watermark = 0;
        }

        Some(RheiBuffer::from_builder(builder))
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
    }

    #[allow(clippy::cast_possible_wrap)]
    fn current_watermark(&self) -> Option<u64> {
        self.max_timestamp.map(|ts| {
            #[allow(clippy::cast_sign_loss)]
            let wm = (ts - self.allowed_lateness_ms as i64).max(0) as u64;
            wm
        })
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        if self.tracked_offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.tracked_offsets {
            tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(*offset))?;
        }
        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)?;
        self.tracked_offsets.clear();
        Ok(())
    }

    fn current_offsets(&self) -> HashMap<String, String> {
        self.tracked_offsets
            .iter()
            .map(|((topic, partition), offset)| {
                (format!("{topic}/{partition}"), offset.to_string())
            })
            .collect()
    }

    async fn restore_offsets(&mut self, offsets: &HashMap<String, String>) -> anyhow::Result<()> {
        let mut tpl = TopicPartitionList::new();
        for (key, val) in offsets {
            let parts: Vec<&str> = key.rsplitn(2, '/').collect();
            if parts.len() == 2 {
                let partition: i32 = parts[0].parse().unwrap_or(0);
                let topic = parts[1];
                let offset: i64 = val.parse().unwrap_or(0);
                tpl.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset))?;
            }
        }
        if tpl.count() > 0 {
            self.consumer.assign(&tpl)?;
        }
        Ok(())
    }

    fn partition_count(&self) -> Option<usize> {
        if self.topics.is_empty() {
            return None;
        }
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topics[0]), Duration::from_secs(5))
            .ok()?;
        metadata.topics().first().map(|t| t.partitions().len())
    }

    fn create_partition_source(
        &self,
        assigned: &[usize],
    ) -> Option<Box<dyn BatchSource<Output = Self::Output>>> {
        if self.brokers.is_empty() || self.topics.is_empty() {
            return None;
        }

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .ok()?;

        let mut tpl = TopicPartitionList::new();
        for topic in &self.topics {
            for &p in assigned {
                #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                tpl.add_partition(topic, p as i32);
            }
        }
        consumer.assign(&tpl).ok()?;

        let mut src = Self::from_consumer(consumer);
        src.batch_size = self.batch_size;
        src.poll_timeout = self.poll_timeout;
        src.watermark_interval = self.watermark_interval;
        src.allowed_lateness_ms = self.allowed_lateness_ms;
        self.brokers.clone_into(&mut src.brokers);
        self.group_id.clone_into(&mut src.group_id);
        self.topics.clone_into(&mut src.topics);
        Some(Box::new(src))
    }
}
