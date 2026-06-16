//! `RheiSchema` implementation for `SqsMessage`.
//!
//! Arrow representation:
//! - `queue_url`: Utf8
//! - `message_id`: Utf8
//! - `receipt_handle`: Utf8
//! - `body`: Utf8
//! - `group_id`: Utf8 (nullable)
//! - `sent_timestamp`: Int64 (nullable)
//! - `receive_count`: Int64 (nullable)
//! - `attributes_json`: Utf8 (JSON-encoded user attributes)

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, Int64Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::arrow::{RheiBuilder, RheiSchema};
use crate::connectors::sqs::types::SqsMessage;

/// Arrow builder for `SqsMessage`.
#[derive(Debug)]
pub struct SqsMessageBuilder {
    queue_url: StringBuilder,
    message_id: StringBuilder,
    receipt_handle: StringBuilder,
    body: StringBuilder,
    group_id: StringBuilder,
    sent_timestamp: Int64Builder,
    receive_count: Int64Builder,
    attributes_json: StringBuilder,
}

/// Zero-copy view into an `SqsMessage` row.
#[derive(Debug)]
pub struct SqsMessageView<'a> {
    /// Queue URL.
    pub queue_url: &'a str,
    /// SQS message ID.
    pub message_id: &'a str,
    /// Receipt handle.
    pub receipt_handle: &'a str,
    /// Message body.
    pub body: &'a str,
    /// FIFO group ID (empty string if null).
    pub group_id: &'a str,
    /// Whether the group ID is null.
    pub group_id_is_null: bool,
    /// `SentTimestamp` in millis (0 if null).
    pub sent_timestamp: i64,
    /// Whether the timestamp is null.
    pub sent_timestamp_is_null: bool,
    /// `ApproximateReceiveCount` (0 if null).
    pub receive_count: i64,
    /// Whether the receive count is null.
    pub receive_count_is_null: bool,
    /// JSON-encoded user attributes.
    pub attributes_json: &'a str,
}

/// Typed column references for `SqsMessage`.
#[derive(Debug)]
pub struct SqsMessageColumns<'a> {
    /// Body column.
    pub body: &'a arrow_array::StringArray,
    /// Receipt handle column.
    pub receipt_handle: &'a arrow_array::StringArray,
}

impl RheiBuilder for SqsMessageBuilder {
    type Item = SqsMessage;

    fn append(&mut self, item: SqsMessage) {
        self.queue_url.append_value(&item.queue_url);
        self.message_id.append_value(&item.message_id);
        self.receipt_handle.append_value(&item.receipt_handle);
        self.body.append_value(&item.body);

        match &item.group_id {
            Some(g) => self.group_id.append_value(g),
            None => self.group_id.append_null(),
        }
        match item.sent_timestamp {
            Some(ts) => self.sent_timestamp.append_value(ts),
            None => self.sent_timestamp.append_null(),
        }
        match item.receive_count {
            Some(c) => self.receive_count.append_value(c),
            None => self.receive_count.append_null(),
        }

        let attrs = serde_json::to_string(&item.attributes).unwrap_or_else(|_| "{}".to_string());
        self.attributes_json.append_value(&attrs);
    }

    fn append_null(&mut self) {
        self.queue_url.append_null();
        self.message_id.append_null();
        self.receipt_handle.append_null();
        self.body.append_null();
        self.group_id.append_null();
        self.sent_timestamp.append_null();
        self.receive_count.append_null();
        self.attributes_json.append_null();
    }

    fn len(&self) -> usize {
        self.queue_url.len()
    }

    #[allow(clippy::expect_used)]
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            SqsMessage::arrow_schema(),
            vec![
                Arc::new(self.queue_url.finish()),
                Arc::new(self.message_id.finish()),
                Arc::new(self.receipt_handle.finish()),
                Arc::new(self.body.finish()),
                Arc::new(self.group_id.finish()),
                Arc::new(self.sent_timestamp.finish()),
                Arc::new(self.receive_count.finish()),
                Arc::new(self.attributes_json.finish()),
            ],
        )
        .expect("SqsMessage schema mismatch")
    }
}

impl RheiSchema for SqsMessage {
    type Builder = SqsMessageBuilder;
    type View<'a> = SqsMessageView<'a>;
    type Columns<'a> = SqsMessageColumns<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("queue_url", DataType::Utf8, false),
            Field::new("message_id", DataType::Utf8, false),
            Field::new("receipt_handle", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("group_id", DataType::Utf8, true),
            Field::new("sent_timestamp", DataType::Int64, true),
            Field::new("receive_count", DataType::Int64, true),
            Field::new("attributes_json", DataType::Utf8, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        SqsMessageBuilder {
            queue_url: StringBuilder::with_capacity(capacity, capacity * 64),
            message_id: StringBuilder::with_capacity(capacity, capacity * 36),
            receipt_handle: StringBuilder::with_capacity(capacity, capacity * 128),
            body: StringBuilder::with_capacity(capacity, capacity * 256),
            group_id: StringBuilder::with_capacity(capacity, capacity * 16),
            sent_timestamp: Int64Builder::with_capacity(capacity),
            receive_count: Int64Builder::with_capacity(capacity),
            attributes_json: StringBuilder::with_capacity(capacity, capacity * 32),
        }
    }

    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        let group_arr = batch.column(4).as_string::<i32>();
        let ts_arr = batch.column(5).as_primitive::<Int64Type>();
        let rc_arr = batch.column(6).as_primitive::<Int64Type>();

        SqsMessageView {
            queue_url: batch.column(0).as_string::<i32>().value(index),
            message_id: batch.column(1).as_string::<i32>().value(index),
            receipt_handle: batch.column(2).as_string::<i32>().value(index),
            body: batch.column(3).as_string::<i32>().value(index),
            group_id: if group_arr.is_null(index) {
                ""
            } else {
                group_arr.value(index)
            },
            group_id_is_null: group_arr.is_null(index),
            sent_timestamp: if ts_arr.is_null(index) {
                0
            } else {
                ts_arr.value(index)
            },
            sent_timestamp_is_null: ts_arr.is_null(index),
            receive_count: if rc_arr.is_null(index) {
                0
            } else {
                rc_arr.value(index)
            },
            receive_count_is_null: rc_arr.is_null(index),
            attributes_json: batch.column(7).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        SqsMessageColumns {
            body: batch.column(3).as_string::<i32>(),
            receipt_handle: batch.column(2).as_string::<i32>(),
        }
    }
}

impl SqsMessageView<'_> {
    /// Parse the user attributes from JSON back into a map.
    pub fn attributes(&self) -> BTreeMap<String, String> {
        serde_json::from_str(self.attributes_json).unwrap_or_default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn build(messages: Vec<SqsMessage>) -> RecordBatch {
        let mut builder = SqsMessage::builder(messages.len());
        for m in messages {
            builder.append(m);
        }
        builder.finish()
    }

    fn message() -> SqsMessage {
        let mut attributes = BTreeMap::new();
        attributes.insert("trace".to_string(), "abc".to_string());
        SqsMessage {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123/orders".to_string(),
            message_id: "m-1".to_string(),
            receipt_handle: "rh-1".to_string(),
            body: "hello".to_string(),
            group_id: Some("g-1".to_string()),
            sent_timestamp: Some(1_700_000_000_000),
            receive_count: Some(2),
            attributes,
        }
    }

    #[test]
    fn round_trips_a_fully_populated_message() {
        let batch = build(vec![message()]);
        let view = SqsMessage::view(&batch, 0);

        assert_eq!(
            view.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123/orders"
        );
        assert_eq!(view.message_id, "m-1");
        assert_eq!(view.receipt_handle, "rh-1");
        assert_eq!(view.body, "hello");
        assert_eq!(view.group_id, "g-1");
        assert!(!view.group_id_is_null);
        assert_eq!(view.sent_timestamp, 1_700_000_000_000);
        assert!(!view.sent_timestamp_is_null);
        assert_eq!(view.receive_count, 2);
        assert!(!view.receive_count_is_null);
        assert_eq!(
            view.attributes().get("trace").map(String::as_str),
            Some("abc")
        );
    }

    #[test]
    fn round_trips_null_optionals() {
        let msg = SqsMessage {
            queue_url: "q".to_string(),
            message_id: "m".to_string(),
            receipt_handle: "rh".to_string(),
            body: String::new(),
            group_id: None,
            sent_timestamp: None,
            receive_count: None,
            attributes: BTreeMap::new(),
        };
        let batch = build(vec![msg]);
        let view = SqsMessage::view(&batch, 0);

        assert!(view.group_id_is_null);
        assert_eq!(view.group_id, "");
        assert!(view.sent_timestamp_is_null);
        assert_eq!(view.sent_timestamp, 0);
        assert!(view.receive_count_is_null);
        assert!(view.attributes().is_empty());
    }

    #[test]
    fn columns_expose_receipt_handles_for_all_rows() {
        let mut a = message();
        a.receipt_handle = "rh-a".to_string();
        a.body = "a".to_string();
        let mut b = message();
        b.receipt_handle = "rh-b".to_string();
        b.body = "b".to_string();

        let batch = build(vec![a, b]);
        let cols = SqsMessage::columns(&batch);
        assert_eq!(cols.receipt_handle.value(0), "rh-a");
        assert_eq!(cols.receipt_handle.value(1), "rh-b");
        assert_eq!(cols.body.value(0), "a");
        assert_eq!(cols.body.value(1), "b");
    }
}
