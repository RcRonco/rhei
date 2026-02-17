//! Kafka transform example: consume from `input-topic`, uppercase payloads,
//! and produce to `output-topic`.
//!
//! Requires a running Kafka broker on `localhost:9092` with both topics created.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:latest
//! kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092
//! kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092
//! echo "hello world" | kafka-console-producer.sh --topic input-topic --bootstrap-server localhost:9092
//! cargo run -p rill-runtime --example kafka_transform --features kafka
//! kafka-console-consumer.sh --topic output-topic --from-beginning --bootstrap-server localhost:9092
//! ```

use async_trait::async_trait;
use rill_core::connectors::kafka_sink::KafkaSink;
use rill_core::connectors::kafka_source::KafkaSource;
use rill_core::connectors::kafka_types::{KafkaMessage, KafkaRecord};
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;
use rill_runtime::dataflow::DataflowGraph;
use rill_runtime::executor::Executor;

/// Uppercases Kafka message payloads.
#[derive(Clone)]
struct Uppercase;

#[async_trait]
impl StreamFunction for Uppercase {
    type Input = KafkaMessage;
    type Output = KafkaRecord;

    async fn process(&mut self, input: KafkaMessage, _ctx: &mut StateContext) -> Vec<KafkaRecord> {
        let Some(payload) = input.payload else {
            return vec![];
        };
        let text = String::from_utf8_lossy(&payload).to_uppercase();
        vec![KafkaRecord::new(text.into_bytes())]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let dir = std::env::temp_dir().join("rill_kafka_transform_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let source =
        KafkaSource::new("localhost:9092", "rill-example", &["input-topic"])?.with_batch_size(100);
    let sink = KafkaSink::new("localhost:9092", "output-topic")?.with_buffer_capacity(100);

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .key_by(|_msg: &KafkaMessage| "all".to_string())
        .operator("uppercase", Uppercase)
        .sink(sink);

    let executor = Executor::builder().checkpoint_dir(dir.clone()).build();

    tracing::info!("starting kafka transform pipeline");
    executor.run(graph).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
