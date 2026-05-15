//! Batch (Arrow columnar) source and sink implementations.

pub mod partitioned_vec_source;
pub mod print_sink;
pub mod vec_source;

#[cfg(feature = "kafka")]
pub mod kafka_dlq;
#[cfg(feature = "kafka")]
pub mod kafka_schema;
#[cfg(feature = "kafka")]
pub mod kafka_sink;
#[cfg(feature = "kafka")]
pub mod kafka_source;

pub use partitioned_vec_source::PartitionedVecSource;
pub use print_sink::PrintSink;
pub use vec_source::VecSource;

#[cfg(feature = "kafka")]
pub use kafka_dlq::KafkaDlqSink;
#[cfg(feature = "kafka")]
pub use kafka_sink::KafkaSink;
#[cfg(feature = "kafka")]
pub use kafka_source::KafkaSource;
