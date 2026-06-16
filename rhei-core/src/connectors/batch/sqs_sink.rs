//! Batch Amazon SQS sink that consumes `RheiBuffer<SqsRecord>` and sends to SQS.
//!
//! Rows are sent in batches of up to 10 via `SendMessageBatch`. For FIFO queues
//! the per-record `group_id` / `dedup_id` are forwarded as `MessageGroupId` /
//! `MessageDeduplicationId`; an optional sink-level default group ID is applied
//! when a record omits its own.

use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, Int32Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

use crate::arrow::{RheiBuffer, RheiBuilder, RheiSchema, Sink};
use crate::connectors::sqs::types::SqsRecord;

/// Maximum entries SQS accepts in a single `SendMessageBatch` call.
const SQS_MAX_SEND_BATCH: usize = 10;

// ── RheiSchema for SqsRecord ─────────────────────────────────────

/// Arrow builder for `SqsRecord`.
#[derive(Debug)]
pub struct SqsRecordBuilder {
    body: StringBuilder,
    group_id: StringBuilder,
    dedup_id: StringBuilder,
    delay_seconds: Int32Builder,
}

/// Zero-copy view into an `SqsRecord` row.
#[derive(Debug)]
pub struct SqsRecordView<'a> {
    /// Message body.
    pub body: &'a str,
    /// FIFO group ID (empty string if null).
    pub group_id: &'a str,
    /// Whether the group ID is null.
    pub group_id_is_null: bool,
    /// FIFO deduplication ID (empty string if null).
    pub dedup_id: &'a str,
    /// Whether the deduplication ID is null.
    pub dedup_id_is_null: bool,
    /// Per-message delay in seconds (0 if null).
    pub delay_seconds: i32,
    /// Whether the delay is null.
    pub delay_seconds_is_null: bool,
}

/// Typed column references for `SqsRecord`.
#[derive(Debug)]
pub struct SqsRecordColumns<'a> {
    /// Body column.
    pub body: &'a arrow_array::StringArray,
}

impl RheiBuilder for SqsRecordBuilder {
    type Item = SqsRecord;

    fn append(&mut self, item: SqsRecord) {
        self.body.append_value(&item.body);
        match &item.group_id {
            Some(g) => self.group_id.append_value(g),
            None => self.group_id.append_null(),
        }
        match &item.dedup_id {
            Some(d) => self.dedup_id.append_value(d),
            None => self.dedup_id.append_null(),
        }
        match item.delay_seconds {
            Some(d) => self.delay_seconds.append_value(d),
            None => self.delay_seconds.append_null(),
        }
    }

    fn append_null(&mut self) {
        self.body.append_null();
        self.group_id.append_null();
        self.dedup_id.append_null();
        self.delay_seconds.append_null();
    }

    fn len(&self) -> usize {
        self.body.len()
    }

    #[allow(clippy::expect_used)]
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            SqsRecord::arrow_schema(),
            vec![
                Arc::new(self.body.finish()),
                Arc::new(self.group_id.finish()),
                Arc::new(self.dedup_id.finish()),
                Arc::new(self.delay_seconds.finish()),
            ],
        )
        .expect("SqsRecord schema mismatch")
    }
}

impl RheiSchema for SqsRecord {
    type Builder = SqsRecordBuilder;
    type View<'a> = SqsRecordView<'a>;
    type Columns<'a> = SqsRecordColumns<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, false),
            Field::new("group_id", DataType::Utf8, true),
            Field::new("dedup_id", DataType::Utf8, true),
            Field::new("delay_seconds", DataType::Int32, true),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        SqsRecordBuilder {
            body: StringBuilder::with_capacity(capacity, capacity * 256),
            group_id: StringBuilder::with_capacity(capacity, capacity * 16),
            dedup_id: StringBuilder::with_capacity(capacity, capacity * 16),
            delay_seconds: Int32Builder::with_capacity(capacity),
        }
    }

    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        let group_arr = batch.column(1).as_string::<i32>();
        let dedup_arr = batch.column(2).as_string::<i32>();
        let delay_arr = batch.column(3).as_primitive::<Int32Type>();

        SqsRecordView {
            body: batch.column(0).as_string::<i32>().value(index),
            group_id: if group_arr.is_null(index) {
                ""
            } else {
                group_arr.value(index)
            },
            group_id_is_null: group_arr.is_null(index),
            dedup_id: if dedup_arr.is_null(index) {
                ""
            } else {
                dedup_arr.value(index)
            },
            dedup_id_is_null: dedup_arr.is_null(index),
            delay_seconds: if delay_arr.is_null(index) {
                0
            } else {
                delay_arr.value(index)
            },
            delay_seconds_is_null: delay_arr.is_null(index),
        }
    }

    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        SqsRecordColumns {
            body: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── SqsSink ─────────────────────────────────────────────────

/// A batch SQS sink that sends messages from Arrow buffers.
pub struct SqsSink {
    client: Client,
    queue_url: String,
    default_group_id: Option<String>,
}

impl std::fmt::Debug for SqsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsSink")
            .field("queue_url", &self.queue_url)
            .field("default_group_id", &self.default_group_id)
            .finish_non_exhaustive()
    }
}

impl SqsSink {
    /// Create a sink from an existing SQS client and queue URL.
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
            default_group_id: None,
        }
    }

    /// Build a sink using AWS credentials and region from the environment.
    pub async fn connect(queue_url: impl Into<String>) -> anyhow::Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self::new(Client::new(&config), queue_url))
    }

    /// Set a default FIFO message group ID applied to records that omit one.
    pub fn with_default_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.default_group_id = Some(group_id.into());
        self
    }
}

/// An owned send entry, decoupled from the borrowed Arrow view so rows can be
/// regrouped into batches of ten.
struct OutEntry {
    body: String,
    group_id: Option<String>,
    dedup_id: Option<String>,
    delay_seconds: Option<i32>,
}

#[async_trait]
impl Sink for SqsSink {
    type Input = SqsRecord;

    async fn write_batch(&mut self, input: RheiBuffer<SqsRecord>) -> anyhow::Result<()> {
        let entries: Vec<OutEntry> = (&input)
            .into_iter()
            .map(|view| OutEntry {
                body: view.body.to_string(),
                group_id: if view.group_id_is_null {
                    self.default_group_id.clone()
                } else {
                    Some(view.group_id.to_string())
                },
                dedup_id: if view.dedup_id_is_null {
                    None
                } else {
                    Some(view.dedup_id.to_string())
                },
                delay_seconds: if view.delay_seconds_is_null {
                    None
                } else {
                    Some(view.delay_seconds)
                },
            })
            .collect();

        for chunk in entries.chunks(SQS_MAX_SEND_BATCH) {
            let mut req = self.client.send_message_batch().queue_url(&self.queue_url);
            for (i, entry) in chunk.iter().enumerate() {
                let mut builder = SendMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .message_body(&entry.body);
                if let Some(g) = &entry.group_id {
                    builder = builder.message_group_id(g);
                }
                if let Some(d) = &entry.dedup_id {
                    builder = builder.message_deduplication_id(d);
                }
                if let Some(delay) = entry.delay_seconds {
                    builder = builder.delay_seconds(delay);
                }
                let built = builder
                    .build()
                    .map_err(|e| anyhow::anyhow!("SQS send entry build error: {e}"))?;
                req = req.entries(built);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("SQS send error: {e}"))?;
            if !resp.failed().is_empty() {
                let detail: Vec<String> = resp
                    .failed()
                    .iter()
                    .map(|f| format!("{}: {}", f.id(), f.message().unwrap_or("<no message>")))
                    .collect();
                return Err(anyhow::anyhow!("SQS send failed for entries: {detail:?}"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn build(records: Vec<SqsRecord>) -> RecordBatch {
        let mut builder = SqsRecord::builder(records.len());
        for r in records {
            builder.append(r);
        }
        builder.finish()
    }

    #[test]
    fn record_round_trips_through_arrow() {
        let batch = build(vec![
            SqsRecord::new("a")
                .with_group_id("g1")
                .with_dedup_id("d1")
                .with_delay_seconds(5),
            SqsRecord::new("b"),
        ]);

        let first = SqsRecord::view(&batch, 0);
        assert_eq!(first.body, "a");
        assert_eq!(first.group_id, "g1");
        assert!(!first.group_id_is_null);
        assert_eq!(first.dedup_id, "d1");
        assert!(!first.dedup_id_is_null);
        assert_eq!(first.delay_seconds, 5);
        assert!(!first.delay_seconds_is_null);

        let second = SqsRecord::view(&batch, 1);
        assert_eq!(second.body, "b");
        assert!(second.group_id_is_null);
        assert!(second.dedup_id_is_null);
        assert!(second.delay_seconds_is_null);
    }

    #[test]
    fn columns_expose_bodies() {
        let batch = build(vec![SqsRecord::new("x"), SqsRecord::new("y")]);
        let cols = SqsRecord::columns(&batch);
        assert_eq!(cols.body.value(0), "x");
        assert_eq!(cols.body.value(1), "y");
    }
}
