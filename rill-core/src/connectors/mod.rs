/// A sink that writes JSON lines to a file.
pub mod file_sink;
/// Kafka message types (always compiled — no rdkafka dependency).
pub mod kafka_types;
/// A sink that prints each element to stdout.
pub mod print_sink;
/// A source backed by an in-memory `Vec`.
pub mod vec_source;

/// Kafka consumer source (requires the `kafka` feature).
#[cfg(feature = "kafka")]
pub mod kafka_source;

/// Kafka producer sink (requires the `kafka` feature).
#[cfg(feature = "kafka")]
pub mod kafka_sink;
