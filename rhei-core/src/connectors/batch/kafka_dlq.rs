//! Kafka-backed dead-letter queue sink.
//!
//! Implements [`DlqSink`] by producing [`DeadLetterRecord`]s as JSON payloads
//! to a Kafka topic, keyed by operator name.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::dlq::{DeadLetterRecord, DlqSink};

/// A DLQ sink that produces dead-letter records to a Kafka topic.
///
/// Each record is serialized as JSON in the payload, with the `operator_name`
/// used as the Kafka message key.
pub struct KafkaDlqSink {
    producer: FutureProducer,
    topic: String,
    produce_timeout: Duration,
}

impl std::fmt::Debug for KafkaDlqSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDlqSink")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaDlqSink {
    /// Create a new Kafka DLQ sink producing to the given topic.
    pub fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;

        Ok(Self {
            producer,
            topic: topic.to_owned(),
            produce_timeout: Duration::from_secs(5),
        })
    }

    /// Set the produce timeout (default: 5s).
    pub fn with_produce_timeout(mut self, timeout: Duration) -> Self {
        self.produce_timeout = timeout;
        self
    }
}

#[async_trait]
impl DlqSink for KafkaDlqSink {
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&record)?;
        let key = record.operator_name.as_bytes().to_vec();

        let fr = FutureRecord::to(&self.topic).payload(&payload).key(&key);

        if let Err((e, _)) = self.producer.send(fr, self.produce_timeout).await {
            return Err(anyhow::anyhow!("Kafka DLQ produce error: {e}"));
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.producer.flush(self.produce_timeout)?;
        Ok(())
    }
}
