#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Kafka transform example: consume from `input-topic`, uppercase payloads,
//! and produce to `output-topic`.
//!
//! Requires a running Kafka broker on `localhost:9092` with both topics created.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:latest
//! kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092
//! kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092
//! echo "hello world" | kafka-console-producer.sh --topic input-topic \
//!     --bootstrap-server localhost:9092
//! cargo run -p rhei-runtime --example kafka_transform --features kafka
//! kafka-console-consumer.sh --topic output-topic --from-beginning \
//!     --bootstrap-server localhost:9092
//! ```

#[cfg(feature = "kafka")]
mod inner {
    use async_trait::async_trait;
    use rhei_core::arrow::{
        BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
    };
    use rhei_core::connectors::batch::{KafkaSink, KafkaSource};
    use rhei_core::connectors::kafka::types::{KafkaMessage, KafkaRecord};
    use rhei_runtime::Executor;
    use rhei_runtime::dataflow::DataflowGraph;

    #[derive(Clone)]
    struct Uppercase;

    #[async_trait]
    impl StreamFunction for Uppercase {
        type Input = KafkaMessage;
        type Output = KafkaRecord;

        async fn process(
            &mut self,
            input: RheiBuffer<KafkaMessage>,
            _ctx: &mut OperatorContext,
        ) -> anyhow::Result<BufferOutput<KafkaRecord>> {
            let mut outputs = Vec::new();
            let batch = input.as_record_batch();
            for (_phys_idx, _view) in input.iter().enumerate_physical() {
                let view = KafkaMessage::view(batch, _phys_idx);
                if view.payload_is_null {
                    continue;
                }
                let text = String::from_utf8_lossy(view.payload).to_uppercase();
                outputs.push(KafkaRecord::new(text.into_bytes()));
            }
            if outputs.is_empty() {
                return Ok(BufferOutput::None);
            }
            let mut builder = KafkaRecord::builder(outputs.len());
            for item in outputs {
                builder.append(item);
            }
            Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
        }
    }

    pub async fn run() -> anyhow::Result<()> {
        tracing_subscriber::fmt().with_env_filter("info").init();

        let dir = std::env::temp_dir().join("rhei_kafka_transform_example");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;

        let source = KafkaSource::new("localhost:9092", "rhei-example", &["input-topic"])?
            .with_batch_size(100);
        let sink = KafkaSink::new("localhost:9092", "output-topic")?;

        let graph = DataflowGraph::new();
        graph
            .source(source)
            .operator("uppercase", Uppercase)
            .sink(sink);

        let executor = Executor::builder().checkpoint_dir(dir.clone()).build()?;

        tracing::info!("starting kafka transform pipeline");
        executor.run(graph).await?;

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}

#[cfg(feature = "kafka")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    inner::run().await
}

#[cfg(not(feature = "kafka"))]
fn main() {
    eprintln!(
        "This example requires the `kafka` feature. Run with:\n  \
         cargo run -p rhei-runtime --example kafka_transform --features kafka"
    );
}
