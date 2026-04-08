use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use super::types::KafkaRecord;
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
            if !record.headers.is_empty() {
                let mut owned = OwnedHeaders::new_with_capacity(record.headers.len());
                for h in &record.headers {
                    owned = owned.insert(Header {
                        key: &h.key,
                        value: Some(&h.value),
                    });
                }
                fr = fr.headers(owned);
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

// ---------------------------------------------------------------------------
// Builder for idempotent / transactional Kafka sinks
// ---------------------------------------------------------------------------

/// Builder for constructing [`KafkaSink`] or [`TransactionalKafkaSink`] with
/// fine-grained producer configuration.
///
/// # Idempotent mode
///
/// Sets `enable.idempotence=true` and appropriate acks/retries. This guarantees
/// at-least-once delivery without duplicates from the producer's perspective.
///
/// # Transactional mode
///
/// Requires a `transactional_id`. The sink wraps batches in Kafka transactions
/// (begin / commit), providing exactly-once semantics when combined with
/// source-side offset commits within the transaction.
///
/// # Examples
///
/// ```rust,no_run
/// use rhei_core::connectors::kafka::sink::KafkaSinkBuilder;
///
/// // Idempotent (non-transactional) sink
/// let sink = KafkaSinkBuilder::new("localhost:9092", "events")
///     .idempotent(true)
///     .buffer_capacity(500)
///     .build()
///     .unwrap();
///
/// // Transactional sink
/// let tx_sink = KafkaSinkBuilder::new("localhost:9092", "events")
///     .transactional_id("rhei-pipeline-0")
///     .buffer_capacity(1000)
///     .build_transactional()
///     .unwrap();
/// ```
pub struct KafkaSinkBuilder {
    brokers: String,
    topic: String,
    idempotent: bool,
    transactional_id: Option<String>,
    buffer_capacity: usize,
    produce_timeout: Duration,
    extra_config: Vec<(String, String)>,
}

impl std::fmt::Debug for KafkaSinkBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSinkBuilder")
            .field("brokers", &self.brokers)
            .field("topic", &self.topic)
            .field("idempotent", &self.idempotent)
            .field("transactional_id", &self.transactional_id)
            .field("buffer_capacity", &self.buffer_capacity)
            .finish_non_exhaustive()
    }
}

impl KafkaSinkBuilder {
    /// Create a new builder targeting the given brokers and topic.
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_owned(),
            topic: topic.to_owned(),
            idempotent: false,
            transactional_id: None,
            buffer_capacity: 0,
            produce_timeout: Duration::from_secs(5),
            extra_config: Vec::new(),
        }
    }

    /// Enable idempotent production (`enable.idempotence=true`).
    ///
    /// This sets `acks=all` and `max.in.flight.requests.per.connection=5`
    /// automatically (Kafka >= 3.x defaults).
    pub fn idempotent(mut self, enable: bool) -> Self {
        self.idempotent = enable;
        self
    }

    /// Set the transactional ID. When set, [`build_transactional`](Self::build_transactional)
    /// produces a [`TransactionalKafkaSink`] that wraps writes in transactions.
    ///
    /// Each pipeline instance must use a unique transactional ID to avoid
    /// fencing conflicts.
    pub fn transactional_id(mut self, id: &str) -> Self {
        self.transactional_id = Some(id.to_owned());
        self
    }

    /// Set the buffer capacity (default: 0 = flush on every write).
    pub fn buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_capacity = capacity;
        self
    }

    /// Set the produce timeout (default: 5s).
    pub fn produce_timeout(mut self, timeout: Duration) -> Self {
        self.produce_timeout = timeout;
        self
    }

    /// Add an arbitrary librdkafka configuration property.
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.extra_config.push((key.to_owned(), value.to_owned()));
        self
    }

    fn build_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.brokers);

        if self.idempotent || self.transactional_id.is_some() {
            config.set("enable.idempotence", "true");
            config.set("acks", "all");
        }

        if let Some(ref tid) = self.transactional_id {
            config.set("transactional.id", tid);
        }

        for (k, v) in &self.extra_config {
            config.set(k, v);
        }

        config
    }

    /// Build a standard (non-transactional) [`KafkaSink`].
    ///
    /// If [`idempotent`](Self::idempotent) was set, the producer will be
    /// configured with idempotent delivery guarantees.
    pub fn build(self) -> anyhow::Result<KafkaSink> {
        let producer: FutureProducer = self.build_client_config().create()?;
        Ok(KafkaSink {
            producer,
            topic: self.topic,
            buffer: Vec::new(),
            buffer_capacity: self.buffer_capacity,
            produce_timeout: self.produce_timeout,
        })
    }

    /// Build a [`TransactionalKafkaSink`].
    ///
    /// This calls `init_transactions()` on the producer, which is required by
    /// the Kafka transactional protocol before the first `begin_transaction()`.
    /// The call may block while coordinating with the Kafka broker.
    ///
    /// # Errors
    ///
    /// Returns an error if `transactional_id` was not set, if the producer
    /// cannot be created, or if `init_transactions()` fails.
    pub fn build_transactional(self) -> anyhow::Result<TransactionalKafkaSink> {
        anyhow::ensure!(
            self.transactional_id.is_some(),
            "transactional_id is required for TransactionalKafkaSink"
        );

        let producer: FutureProducer = self.build_client_config().create()?;

        // The Kafka transactional protocol requires init_transactions() to be
        // called exactly once before the first begin_transaction().
        producer.init_transactions(self.produce_timeout)?;

        Ok(TransactionalKafkaSink {
            producer,
            topic: self.topic,
            buffer: Vec::new(),
            buffer_capacity: self.buffer_capacity,
            produce_timeout: self.produce_timeout,
            in_transaction: false,
        })
    }
}

// ---------------------------------------------------------------------------
// Transactional Kafka sink
// ---------------------------------------------------------------------------

/// A Kafka sink that wraps writes in transactions for exactly-once semantics.
///
/// Records are buffered up to `buffer_capacity`. When the buffer is full (or
/// `flush()` is called), the sink begins a transaction, produces all buffered
/// records, and commits the transaction.
///
/// # Transaction lifecycle
///
/// ```text
/// begin_transaction()
///   -> produce(record_1)
///   -> produce(record_2)
///   -> ...
/// commit_transaction()
/// ```
///
/// If any produce fails, the transaction is aborted and the error is propagated.
/// Downstream consumers configured with `isolation.level=read_committed` will
/// only see committed records, providing exactly-once delivery.
pub struct TransactionalKafkaSink {
    producer: FutureProducer,
    topic: String,
    buffer: Vec<KafkaRecord>,
    buffer_capacity: usize,
    produce_timeout: Duration,
    in_transaction: bool,
}

impl std::fmt::Debug for TransactionalKafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalKafkaSink")
            .field("topic", &self.topic)
            .field("buffer_len", &self.buffer.len())
            .field("buffer_capacity", &self.buffer_capacity)
            .field("in_transaction", &self.in_transaction)
            .finish_non_exhaustive()
    }
}

impl TransactionalKafkaSink {
    /// Returns `true` if a transaction is currently in progress.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Returns the number of records currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Access the underlying `FutureProducer` for advanced operations
    /// (e.g. sending offsets to transaction).
    pub fn producer(&self) -> &FutureProducer {
        &self.producer
    }

    async fn flush_transactional(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch_size = self.buffer.len();

        // Begin transaction if not already in one.
        if !self.in_transaction {
            self.producer.begin_transaction()?;
            self.in_transaction = true;
        }

        // Produce all buffered records. On the first error, break immediately
        // and abort the transaction. Records remain in self.buffer so they can
        // be retried on the next flush attempt.
        let mut produce_error: Option<anyhow::Error> = None;

        for record in &self.buffer {
            let mut fr = FutureRecord::to(&self.topic).payload(&record.payload);
            if let Some(ref key) = record.key {
                fr = fr.key(key);
            }
            if !record.headers.is_empty() {
                let mut owned = OwnedHeaders::new_with_capacity(record.headers.len());
                for h in &record.headers {
                    owned = owned.insert(Header {
                        key: &h.key,
                        value: Some(&h.value),
                    });
                }
                fr = fr.headers(owned);
            }

            match self.producer.send(fr, self.produce_timeout).await {
                Ok(_) => {
                    metrics::counter!("kafka_tx_messages_produced_total").increment(1);
                }
                Err((e, _)) => {
                    metrics::counter!("kafka_tx_produce_errors_total").increment(1);
                    tracing::error!(error = %e, "transactional kafka produce error");
                    produce_error =
                        Some(anyhow::anyhow!("transactional kafka produce error: {e}"));
                    break;
                }
            }
        }

        if let Some(e) = produce_error {
            // Abort the transaction on failure. Records stay in self.buffer
            // for retry on the next flush attempt.
            if let Err(abort_err) = self.producer.abort_transaction(self.produce_timeout) {
                tracing::error!(error = %abort_err, "failed to abort kafka transaction");
            }
            self.in_transaction = false;
            metrics::counter!("kafka_tx_aborted_total").increment(1);
            return Err(e);
        }

        // Commit the transaction.
        self.producer.commit_transaction(self.produce_timeout)?;
        self.in_transaction = false;

        // Only clear the buffer after a successful commit.
        self.buffer.clear();

        metrics::counter!("kafka_tx_committed_total").increment(1);
        metrics::counter!("kafka_tx_records_committed_total")
            .increment(u64::try_from(batch_size).unwrap_or(u64::MAX));

        Ok(())
    }
}

#[async_trait]
impl Sink for TransactionalKafkaSink {
    type Input = KafkaRecord;

    async fn write(&mut self, input: KafkaRecord) -> anyhow::Result<()> {
        self.buffer.push(input);
        if self.buffer_capacity > 0 && self.buffer.len() >= self.buffer_capacity {
            self.flush_transactional().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.flush_transactional().await
    }
}
