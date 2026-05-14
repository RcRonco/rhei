//! Batch Kafka sink that consumes `RheiBuffer<KafkaRecord>` and produces to Kafka.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::arrow::{BatchSink, RheiBuffer, RheiBuilder, RheiSchema};
use crate::connectors::kafka::types::{KafkaHeader, KafkaRecord};

// ── RheiSchema for KafkaRecord ─────────────────────────────────────

/// Arrow builder for `KafkaRecord`.
#[derive(Debug)]
pub struct KafkaRecordBuilder {
    key: BinaryBuilder,
    payload: BinaryBuilder,
    headers_json: StringBuilder,
}

/// Zero-copy view into a `KafkaRecord` row.
#[derive(Debug)]
pub struct KafkaRecordView<'a> {
    /// Record key (empty slice if null).
    pub key: &'a [u8],
    /// Whether the key is null.
    pub key_is_null: bool,
    /// Record payload.
    pub payload: &'a [u8],
    /// JSON-encoded headers.
    pub headers_json: &'a str,
}

/// Typed column references for `KafkaRecord`.
#[derive(Debug)]
pub struct KafkaRecordColumns<'a> {
    /// Payload column.
    #[allow(dead_code)]
    pub payload: &'a arrow_array::BinaryArray,
}

impl RheiBuilder for KafkaRecordBuilder {
    type Item = KafkaRecord;

    fn append(&mut self, item: KafkaRecord) {
        match &item.key {
            Some(k) => self.key.append_value(k),
            None => self.key.append_null(),
        }
        self.payload.append_value(&item.payload);
        let headers_str = serde_json::to_string(&item.headers).unwrap_or_default();
        self.headers_json.append_value(&headers_str);
    }

    fn append_null(&mut self) {
        self.key.append_null();
        self.payload.append_null();
        self.headers_json.append_null();
    }

    fn len(&self) -> usize {
        self.key.len()
    }

    #[allow(clippy::expect_used)]
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            KafkaRecord::arrow_schema(),
            vec![
                Arc::new(self.key.finish()),
                Arc::new(self.payload.finish()),
                Arc::new(self.headers_json.finish()),
            ],
        )
        .expect("KafkaRecord schema mismatch")
    }
}

impl RheiSchema for KafkaRecord {
    type Builder = KafkaRecordBuilder;
    type View<'a> = KafkaRecordView<'a>;
    type Columns<'a> = KafkaRecordColumns<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, true),
            Field::new("payload", DataType::Binary, false),
            Field::new("headers_json", DataType::Utf8, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        KafkaRecordBuilder {
            key: BinaryBuilder::with_capacity(capacity, capacity * 16),
            payload: BinaryBuilder::with_capacity(capacity, capacity * 256),
            headers_json: StringBuilder::with_capacity(capacity, capacity * 64),
        }
    }

    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::Array;
        let key_arr = batch.column(0).as_binary::<i32>();
        KafkaRecordView {
            key: if key_arr.is_null(index) {
                &[]
            } else {
                key_arr.value(index)
            },
            key_is_null: key_arr.is_null(index),
            payload: batch.column(1).as_binary::<i32>().value(index),
            headers_json: batch.column(2).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        KafkaRecordColumns {
            payload: batch.column(1).as_binary::<i32>(),
        }
    }
}

impl KafkaRecordView<'_> {
    /// Parse headers from JSON.
    pub fn headers(&self) -> Vec<KafkaHeader> {
        serde_json::from_str(self.headers_json).unwrap_or_default()
    }
}

// ── BatchKafkaSink ─────────────────────────────────────────────────

/// A batch Kafka sink that produces messages from Arrow buffers.
pub struct BatchKafkaSink {
    producer: FutureProducer,
    topic: String,
    produce_timeout: Duration,
}

impl std::fmt::Debug for BatchKafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchKafkaSink")
            .field("topic", &self.topic)
            .field("produce_timeout", &self.produce_timeout)
            .finish_non_exhaustive()
    }
}

impl BatchKafkaSink {
    /// Create a new `BatchKafkaSink` that produces to the given topic.
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

    /// Create from an existing producer.
    pub fn from_producer(producer: FutureProducer, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
            produce_timeout: Duration::from_secs(5),
        }
    }

    /// Set the produce timeout (default: 5s).
    pub fn with_produce_timeout(mut self, timeout: Duration) -> Self {
        self.produce_timeout = timeout;
        self
    }
}

#[async_trait]
impl BatchSink for BatchKafkaSink {
    type Input = KafkaRecord;

    async fn write_batch(&mut self, input: RheiBuffer<KafkaRecord>) -> anyhow::Result<()> {
        for view in &input {
            let headers: Vec<KafkaHeader> = view.headers();
            let mut owned_headers = OwnedHeaders::new_with_capacity(headers.len());
            for h in &headers {
                owned_headers = owned_headers.insert(Header {
                    key: &h.key,
                    value: Some(&h.value),
                });
            }

            let mut fr = FutureRecord::to(&self.topic)
                .payload(view.payload)
                .headers(owned_headers);

            if !view.key_is_null {
                fr = fr.key(view.key);
            }

            if let Err((e, _)) = self.producer.send(fr, self.produce_timeout).await {
                return Err(anyhow::anyhow!("Kafka produce error: {e}"));
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.producer.flush(self.produce_timeout)?;
        Ok(())
    }
}
