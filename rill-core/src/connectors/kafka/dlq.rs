//! Kafka-backed dead-letter queue sink.
//!
//! Wraps a [`KafkaSink`](super::kafka::sink::KafkaSink) to accept
//! [`DeadLetterRecord`](crate::dlq::DeadLetterRecord)s, serializing them as
//! JSON payloads with the operator name as the Kafka key.

use async_trait::async_trait;

use super::kafka::sink::KafkaSink;
use super::kafka::types::KafkaRecord;
use crate::dlq::{DeadLetterRecord, DlqSinkFactory};
use crate::traits::Sink;

/// A DLQ sink that produces [`DeadLetterRecord`]s to a Kafka topic.
///
/// Each record is serialized as JSON in the payload, with the `operator_name`
/// used as the Kafka message key.
pub struct KafkaDlqSink {
    inner: KafkaSink,
}

impl std::fmt::Debug for KafkaDlqSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDlqSink")
            .field("inner", &self.inner)
            .finish()
    }
}

impl KafkaDlqSink {
    /// Create a new Kafka DLQ sink producing to the given topic.
    pub fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
        let inner = KafkaSink::new(brokers, topic)?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl Sink for KafkaDlqSink {
    type Input = DeadLetterRecord;

    async fn write(&mut self, input: DeadLetterRecord) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&input)?;
        let key = input.operator_name.into_bytes();
        let record = KafkaRecord::with_key(key, payload);
        self.inner.write(record).await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush().await
    }
}

/// Factory that creates [`KafkaDlqSink`] instances per worker.
#[derive(Debug, Clone)]
pub struct KafkaDlqFactory {
    brokers: String,
    topic: String,
}

impl KafkaDlqFactory {
    /// Create a new factory with the given Kafka brokers and topic.
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
        }
    }
}

impl DlqSinkFactory for KafkaDlqFactory {
    fn create(
        &self,
        _worker_index: usize,
    ) -> anyhow::Result<Box<dyn Sink<Input = DeadLetterRecord>>> {
        let sink = KafkaDlqSink::new(&self.brokers, &self.topic)?;
        Ok(Box::new(sink))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_record_to_kafka_record_serialization() {
        let record = DeadLetterRecord {
            input_repr: r#"{"user":"alice"}"#.to_string(),
            operator_name: "enricher".to_string(),
            error: "lookup failed".to_string(),
            timestamp: "1700000000".to_string(),
        };

        let payload = serde_json::to_vec(&record).unwrap();
        let key = record.operator_name.clone().into_bytes();
        let kafka_record = KafkaRecord::with_key(key.clone(), payload.clone());

        assert_eq!(kafka_record.key.as_deref(), Some(key.as_slice()));
        let deserialized: DeadLetterRecord = serde_json::from_slice(&kafka_record.payload).unwrap();
        assert_eq!(deserialized.operator_name, "enricher");
        assert_eq!(deserialized.error, "lookup failed");
    }
}
