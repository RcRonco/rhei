use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::{ClientConfig, TopicPartitionList};

use super::types::{KafkaHeader, KafkaMessage};
use crate::traits::Source;

/// A Kafka consumer that implements the [`Source`] trait.
///
/// Polls messages from one or more Kafka topics and emits [`KafkaMessage`] batches.
/// Offset commits are deferred until [`Source::on_checkpoint_complete`] is called,
/// ensuring at-least-once delivery when combined with Rhei's checkpoint mechanism.
pub struct KafkaSource {
    consumer: StreamConsumer,
    batch_size: usize,
    poll_timeout: Duration,
    tracked_offsets: HashMap<(String, i32), i64>,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
    /// Maximum message timestamp seen so far (millis since epoch).
    max_timestamp: Option<i64>,
    /// How far behind the max timestamp the watermark sits (millis).
    allowed_lateness_ms: u64,
    /// Factory config for creating partition-specific consumers.
    brokers: String,
    group_id: String,
    topics: Vec<String>,
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
            max_timestamp: None,
            allowed_lateness_ms: 0,
            brokers: brokers.to_owned(),
            group_id: group_id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
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
            max_timestamp: None,
            allowed_lateness_ms: 0,
            brokers: String::new(),
            group_id: String::new(),
            topics: Vec::new(),
        }
    }

    /// Create a new `KafkaSource` that subscribes to topics matching a regex pattern.
    ///
    /// The pattern follows librdkafka conventions: a regex that is matched against
    /// topic names. For example, `"signals\\..*"` matches `signals.org.abc`,
    /// `signals.pov.all`, etc.
    ///
    /// Internally prefixes the pattern with `^` for librdkafka regex subscription.
    ///
    /// # Limitations
    ///
    /// Pattern-based sources do not support `partition_count()` /
    /// `create_partition_source()` and will run as a single-worker source.
    pub fn with_topic_pattern(
        brokers: &str,
        group_id: &str,
        pattern: &str,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        let regex_topic = format!("^{pattern}");
        consumer.subscribe(&[&regex_topic])?;

        Ok(Self {
            consumer,
            batch_size: 100,
            poll_timeout: Duration::from_millis(100),
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: 1000,
            watermark_pending: false,
            max_timestamp: None,
            allowed_lateness_ms: 0,
            brokers: brokers.to_owned(),
            group_id: group_id.to_owned(),
            topics: Vec::new(),
        })
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

    /// Set the allowed lateness for watermark computation in milliseconds (default: 0).
    ///
    /// The watermark is computed as `max_timestamp - allowed_lateness_ms`. This
    /// gives late events a grace period before they are considered past the watermark.
    pub fn with_allowed_lateness(mut self, ms: u64) -> Self {
        self.allowed_lateness_ms = ms;
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

                    let headers = msg
                        .headers()
                        .map(|hs| {
                            (0..hs.count())
                                .map(|i| {
                                    let h = hs.get(i);
                                    KafkaHeader {
                                        key: h.key.to_owned(),
                                        value: h.value.unwrap_or_default().to_vec(),
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let kafka_msg = KafkaMessage {
                        topic: topic.clone(),
                        partition,
                        offset,
                        key: msg.key().map(<[u8]>::to_vec),
                        payload: msg.payload().map(<[u8]>::to_vec),
                        timestamp: msg.timestamp().to_millis(),
                        headers,
                    };

                    // Track max message timestamp for watermark computation.
                    if let Some(ts) = kafka_msg.timestamp {
                        self.max_timestamp =
                            Some(self.max_timestamp.map_or(ts, |prev| prev.max(ts)));
                    }

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

    fn current_watermark(&self) -> Option<u64> {
        self.max_timestamp.map(|ts| {
            u64::try_from(ts.max(0))
                .unwrap_or(0)
                .saturating_sub(self.allowed_lateness_ms)
        })
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
        if offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for (key, value) in offsets {
            // Key format: "topic/partition"
            let (topic, partition_str) = key
                .rsplit_once('/')
                .ok_or_else(|| anyhow::anyhow!("invalid offset key format: {key}"))?;
            let partition: i32 = partition_str.parse()?;
            let offset: i64 = value.parse()?;
            // Seek to offset+1 (next unread message).
            tpl.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset + 1))?;
        }

        // Switch to manual partition assignment at the restored offsets.
        // This overrides consumer group subscription/rebalancing.
        self.consumer.assign(&tpl)?;
        tracing::info!(
            partitions = offsets.len(),
            "restored source offsets via assign()"
        );
        Ok(())
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

    fn partition_count(&self) -> Option<usize> {
        if self.topics.is_empty() {
            return None; // from_consumer() path — no factory config
        }
        let metadata = self
            .consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .ok()?;
        let count: usize = metadata
            .topics()
            .iter()
            .filter(|t| self.topics.contains(&t.name().to_owned()))
            .map(|t| t.partitions().len())
            .sum();
        Some(count)
    }

    fn create_partition_source(
        &self,
        assigned: &[usize],
    ) -> Option<Box<dyn Source<Output = KafkaMessage>>> {
        // Fetch metadata to map global partition indices to (topic, partition) pairs
        let metadata = match self.consumer.fetch_metadata(None, Duration::from_secs(10)) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "metadata fetch failed");
                return None;
            }
        };

        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for t in metadata.topics() {
            if self.topics.contains(&t.name().to_owned()) {
                for p in t.partitions() {
                    all_partitions.push((t.name().to_owned(), p.id()));
                }
            }
        }

        // Create a new consumer with manual assignment (no group rebalance)
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "failed to create partition consumer");
                return None;
            }
        };

        let mut tpl = TopicPartitionList::new();
        let assigned_pairs: Vec<(String, i32)> = assigned
            .iter()
            .map(|&idx| all_partitions[idx].clone())
            .collect();
        for (topic, partition) in &assigned_pairs {
            if let Err(e) = tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Beginning)
            {
                tracing::error!(error = %e, "failed to add partition offset");
                return None;
            }
        }
        if let Err(e) = consumer.assign(&tpl) {
            tracing::error!(error = %e, "failed to assign partitions");
            return None;
        }

        Some(Box::new(KafkaPartitionSource {
            consumer,
            batch_size: self.batch_size,
            poll_timeout: self.poll_timeout,
            tracked_offsets: HashMap::new(),
            records_since_watermark: 0,
            watermark_interval: self.watermark_interval,
            watermark_pending: false,
            max_timestamp: None,
            allowed_lateness_ms: self.allowed_lateness_ms,
            assigned_partitions: assigned_pairs,
        }))
    }
}

/// A partition-specific Kafka consumer for parallel source consumption.
///
/// Created by [`KafkaSource::create_partition_source()`]. Each instance reads
/// from a fixed set of (topic, partition) pairs using manual assignment.
struct KafkaPartitionSource {
    consumer: StreamConsumer,
    batch_size: usize,
    poll_timeout: Duration,
    tracked_offsets: HashMap<(String, i32), i64>,
    records_since_watermark: usize,
    watermark_interval: usize,
    watermark_pending: bool,
    max_timestamp: Option<i64>,
    allowed_lateness_ms: u64,
    assigned_partitions: Vec<(String, i32)>,
}

#[async_trait]
impl Source for KafkaPartitionSource {
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

                    let headers = msg
                        .headers()
                        .map(|hs| {
                            (0..hs.count())
                                .map(|i| {
                                    let h = hs.get(i);
                                    KafkaHeader {
                                        key: h.key.to_owned(),
                                        value: h.value.unwrap_or_default().to_vec(),
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let kafka_msg = KafkaMessage {
                        topic: topic.clone(),
                        partition,
                        offset,
                        key: msg.key().map(<[u8]>::to_vec),
                        payload: msg.payload().map(<[u8]>::to_vec),
                        timestamp: msg.timestamp().to_millis(),
                        headers,
                    };

                    // Track max message timestamp for watermark computation.
                    if let Some(ts) = kafka_msg.timestamp {
                        self.max_timestamp =
                            Some(self.max_timestamp.map_or(ts, |prev| prev.max(ts)));
                    }

                    self.tracked_offsets.insert((topic, partition), offset);
                    metrics::counter!("kafka_messages_consumed_total").increment(1);
                    batch.push(kafka_msg);
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "kafka partition consumer error");
                    break;
                }
                Err(_) => break,
            }
        }

        self.records_since_watermark += batch.len();
        if self.records_since_watermark >= self.watermark_interval {
            self.watermark_pending = true;
            self.records_since_watermark = 0;
        }

        Some(batch)
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark_pending
    }

    fn current_watermark(&self) -> Option<u64> {
        self.max_timestamp.map(|ts| {
            u64::try_from(ts.max(0))
                .unwrap_or(0)
                .saturating_sub(self.allowed_lateness_ms)
        })
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
        if offsets.is_empty() {
            return Ok(());
        }

        // Reassign ALL partitions: restored ones at their offset, others from beginning.
        // assign() replaces the entire assignment, so we must include every partition.
        let mut tpl = TopicPartitionList::new();
        let mut restored = 0usize;
        for (topic, partition) in &self.assigned_partitions {
            let key = format!("{topic}/{partition}");
            if let Some(offset_str) = offsets.get(&key) {
                let offset: i64 = offset_str.parse()?;
                tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(offset + 1))?;
                restored += 1;
            } else {
                tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Beginning)?;
            }
        }

        self.consumer.assign(&tpl)?;
        tracing::info!(
            restored,
            total = self.assigned_partitions.len(),
            "partition source restored offsets via assign()"
        );
        Ok(())
    }

    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        if self.tracked_offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.tracked_offsets {
            tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(offset + 1))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
        metrics::counter!("kafka_consumer_offsets_committed_total").increment(1);
        tracing::debug!(
            partitions = self.tracked_offsets.len(),
            "partition source committed offsets"
        );
        self.tracked_offsets.clear();

        Ok(())
    }
}
