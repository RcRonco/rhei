use serde::{Deserialize, Serialize};

/// A message consumed from Kafka, including topic/partition/offset metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    /// The topic this message was consumed from.
    pub topic: String,
    /// The partition this message was consumed from.
    pub partition: i32,
    /// The offset of this message within its partition.
    pub offset: i64,
    /// The message key, if present.
    pub key: Option<Vec<u8>>,
    /// The message payload, if present.
    pub payload: Option<Vec<u8>>,
    /// The message timestamp in milliseconds, if present.
    pub timestamp: Option<i64>,
}

/// A record to be produced to Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaRecord {
    /// The record key, if present.
    pub key: Option<Vec<u8>>,
    /// The record payload.
    pub payload: Vec<u8>,
}

impl KafkaRecord {
    /// Create a new record with only a payload (no key).
    pub fn new(payload: Vec<u8>) -> Self {
        Self { key: None, payload }
    }

    /// Create a new record with both a key and payload.
    pub fn with_key(key: Vec<u8>, payload: Vec<u8>) -> Self {
        Self {
            key: Some(key),
            payload,
        }
    }
}
