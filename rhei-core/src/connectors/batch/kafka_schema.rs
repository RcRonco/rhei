//! `RheiSchema` implementation for `KafkaMessage`.
//!
//! Arrow representation:
//! - `topic`: Utf8
//! - `partition`: Int32
//! - `offset`: Int64
//! - `key`: Binary (nullable)
//! - `payload`: Binary (nullable)
//! - `timestamp`: Int64 (nullable)
//! - `headers_json`: Utf8 (JSON-encoded headers)

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::arrow::{RheiBuilder, RheiSchema};
use crate::connectors::kafka::types::{KafkaHeader, KafkaMessage};

/// Arrow builder for `KafkaMessage`.
#[derive(Debug)]
pub struct KafkaMessageBuilder {
    topic: StringBuilder,
    partition: Int32Builder,
    offset: Int64Builder,
    key: BinaryBuilder,
    payload: BinaryBuilder,
    timestamp: Int64Builder,
    headers_json: StringBuilder,
}

/// Zero-copy view into a `KafkaMessage` row.
#[derive(Debug)]
pub struct KafkaMessageView<'a> {
    /// Topic name.
    pub topic: &'a str,
    /// Partition number.
    pub partition: i32,
    /// Offset within partition.
    pub offset: i64,
    /// Message key (empty slice if null).
    pub key: &'a [u8],
    /// Whether the key is null.
    pub key_is_null: bool,
    /// Message payload (empty slice if null).
    pub payload: &'a [u8],
    /// Whether the payload is null.
    pub payload_is_null: bool,
    /// Timestamp in millis (0 if null).
    pub timestamp: i64,
    /// Whether timestamp is null.
    pub timestamp_is_null: bool,
    /// JSON-encoded headers.
    pub headers_json: &'a str,
}

/// Typed column references for `KafkaMessage`.
#[derive(Debug)]
pub struct KafkaMessageColumns<'a> {
    /// Topic column.
    pub topic: &'a arrow_array::StringArray,
    /// Partition column.
    pub partition: &'a arrow_array::Int32Array,
    /// Offset column.
    pub offset: &'a arrow_array::Int64Array,
}

impl RheiBuilder for KafkaMessageBuilder {
    type Item = KafkaMessage;

    fn append(&mut self, item: KafkaMessage) {
        self.topic.append_value(&item.topic);
        self.partition.append_value(item.partition);
        self.offset.append_value(item.offset);

        match &item.key {
            Some(k) => self.key.append_value(k),
            None => self.key.append_null(),
        }

        match &item.payload {
            Some(p) => self.payload.append_value(p),
            None => self.payload.append_null(),
        }

        match item.timestamp {
            Some(ts) => self.timestamp.append_value(ts),
            None => self.timestamp.append_null(),
        }

        let headers_str = serde_json::to_string(&item.headers).unwrap_or_default();
        self.headers_json.append_value(&headers_str);
    }

    fn append_null(&mut self) {
        self.topic.append_null();
        self.partition.append_null();
        self.offset.append_null();
        self.key.append_null();
        self.payload.append_null();
        self.timestamp.append_null();
        self.headers_json.append_null();
    }

    fn len(&self) -> usize {
        self.topic.len()
    }

    #[allow(clippy::expect_used)]
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            KafkaMessage::arrow_schema(),
            vec![
                Arc::new(self.topic.finish()),
                Arc::new(self.partition.finish()),
                Arc::new(self.offset.finish()),
                Arc::new(self.key.finish()),
                Arc::new(self.payload.finish()),
                Arc::new(self.timestamp.finish()),
                Arc::new(self.headers_json.finish()),
            ],
        )
        .expect("KafkaMessage schema mismatch")
    }
}

impl RheiSchema for KafkaMessage {
    type Builder = KafkaMessageBuilder;
    type View<'a> = KafkaMessageView<'a>;
    type Columns<'a> = KafkaMessageColumns<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("topic", DataType::Utf8, false),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new("key", DataType::Binary, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("timestamp", DataType::Int64, true),
            Field::new("headers_json", DataType::Utf8, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        KafkaMessageBuilder {
            topic: StringBuilder::with_capacity(capacity, capacity * 32),
            partition: Int32Builder::with_capacity(capacity),
            offset: Int64Builder::with_capacity(capacity),
            key: BinaryBuilder::with_capacity(capacity, capacity * 16),
            payload: BinaryBuilder::with_capacity(capacity, capacity * 256),
            timestamp: Int64Builder::with_capacity(capacity),
            headers_json: StringBuilder::with_capacity(capacity, capacity * 64),
        }
    }

    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        let key_arr = batch.column(3).as_binary::<i32>();
        let payload_arr = batch.column(4).as_binary::<i32>();
        let ts_arr = batch.column(5).as_primitive::<Int64Type>();

        KafkaMessageView {
            topic: batch.column(0).as_string::<i32>().value(index),
            partition: batch.column(1).as_primitive::<Int32Type>().value(index),
            offset: batch.column(2).as_primitive::<Int64Type>().value(index),
            key: if key_arr.is_null(index) {
                &[]
            } else {
                key_arr.value(index)
            },
            key_is_null: key_arr.is_null(index),
            payload: if payload_arr.is_null(index) {
                &[]
            } else {
                payload_arr.value(index)
            },
            payload_is_null: payload_arr.is_null(index),
            timestamp: if ts_arr.is_null(index) {
                0
            } else {
                ts_arr.value(index)
            },
            timestamp_is_null: ts_arr.is_null(index),
            headers_json: batch.column(6).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        KafkaMessageColumns {
            topic: batch.column(0).as_string::<i32>(),
            partition: batch.column(1).as_primitive::<Int32Type>(),
            offset: batch.column(2).as_primitive::<Int64Type>(),
        }
    }
}

impl KafkaMessageView<'_> {
    /// Parse headers from JSON back into a vec.
    pub fn headers(&self) -> Vec<KafkaHeader> {
        serde_json::from_str(self.headers_json).unwrap_or_default()
    }
}
