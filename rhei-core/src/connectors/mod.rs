/// File-backed dead-letter queue sink.
pub mod dlq_file_sink;
/// A sink that writes JSON lines to a file.
pub mod file_sink;

/// A partitioned source backed by an in-memory `Vec` (for testing parallel consumption).
pub mod partitioned_vec_source;
/// A sink that prints each element to stdout.
pub mod print_sink;
/// A source backed by an in-memory `Vec`.
pub mod vec_source;

/// Kafka source, sink, and dead-letter queue connectors.
#[cfg(feature = "kafka")]
pub mod kafka;
