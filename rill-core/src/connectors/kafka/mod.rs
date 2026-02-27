/// Kafka-backed dead-letter queue sink.
pub mod dlq;
/// Kafka producer sink.
pub mod sink;
/// Kafka consumer source with partition-level parallel consumption.
pub mod source;
/// Shared Kafka message and record types.
pub mod types;
