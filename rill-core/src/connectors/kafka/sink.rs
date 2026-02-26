use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::kafka::types::KafkaRecord;
use crate::traits::Sink;

/// A Kafka producer that implements the [`Sink`] trait.
///
/// Buffers [`KafkaRecord`] values and produces them to a single Kafka topic.
/// When `buffer_capacity` is 0 (default), each `write()` flushes immediately.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    buffer: Vec<KafkaRecord>,
    buffer_capacity: usize,
    produce_timeout: Duration,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("topic", &self.topic)
            .field("buffer_len", &self.buffer.len())
            .field("buffer_capacity", &self.buffer_capacity)
            .field("produce_timeout", &self.produce_timeout)
            .finish_non_exhaustive()
    }
}

impl KafkaSink {
    /// Create a new `KafkaSink` that produces to the given topic.
    pub fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;

        Ok(Self {
            producer,
            topic: topic.to_owned(),
            buffer: Vec::new(),
            buffer_capacity: 0,
            produce_timeout: Duration::from_secs(5),
        })
    }

    /// Create a `KafkaSink` from an already-configured [`FutureProducer`].
    pub fn from_producer(producer: FutureProducer, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
            buffer: Vec::new(),
            buffer_capacity: 0,
            produce_timeout: Duration::from_secs(5),
        }
    }

    /// Set the buffer capacity (default: 0 = flush immediately on each write).
    pub fn with_buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_capacity = capacity;
        self
    }

    /// Set the produce timeout (default: 5s).
    pub fn with_produce_timeout(mut self, timeout: Duration) -> Self {
        self.produce_timeout = timeout;
        self
    }

    async fn flush_buffer(&mut self) -> anyhow::Result<()> {
        let mut first_error: Option<anyhow::Error> = None;

        for record in self.buffer.drain(..) {
            let mut fr = FutureRecord::to(&self.topic).payload(&record.payload);
            if let Some(ref key) = record.key {
                fr = fr.key(key);
            }

            match self.producer.send(fr, self.produce_timeout).await {
                Ok(_) => {
                    metrics::counter!("kafka_messages_produced_total").increment(1);
                }
                Err((e, _)) => {
                    metrics::counter!("kafka_produce_errors_total").increment(1);
                    tracing::error!(error = %e, "kafka produce error");
                    if first_error.is_none() {
                        first_error = Some(anyhow::anyhow!("kafka produce error: {e}"));
                    }
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
    type Input = KafkaRecord;

    async fn write(&mut self, input: KafkaRecord) -> anyhow::Result<()> {
        self.buffer.push(input);
        if self.buffer_capacity == 0 || self.buffer.len() >= self.buffer_capacity {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.flush_buffer().await?;
        }
        Ok(())
    }
}
