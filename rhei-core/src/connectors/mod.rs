/// Batch (Arrow columnar) sources and sinks.
pub mod batch;

/// Kafka source, sink, and dead-letter queue connectors.
#[cfg(feature = "kafka")]
pub mod kafka;
