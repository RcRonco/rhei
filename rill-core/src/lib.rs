//! Core abstractions for the Rill stream processing engine.
//!
//! This crate provides the foundational types and traits for building
//! stateful stream processing pipelines:
//!
//! - [`traits::StreamFunction`] — stateful operator that transforms input elements
//! - [`traits::Source`] — produces batches of elements into a pipeline
//! - [`traits::Sink`] — consumes elements from a pipeline
//! - [`state::context::StateContext`] — operator-scoped key-value state with tiered storage
//! - [`graph::StreamGraph`] — fluent builder for constructing logical execution plans
//! - [`connectors`] — built-in sources and sinks (`VecSource`, `PrintSink`)

#![warn(missing_docs)]

/// Built-in source and sink connectors.
pub mod connectors;
/// Dead-letter queue types.
pub mod dlq;
/// Event serialization and the [`Event`](event::Event) marker trait.
pub mod event;
/// Logical execution plan builder ([`StreamGraph`](graph::StreamGraph)).
pub mod graph;
/// Reusable stream processing operators ([`operators::MapOp`], [`operators::TumblingWindow`], etc.).
pub mod operators;
/// Tiered state management (memtable, local, Foyer, `SlateDB`).
pub mod state;
/// Core stream processing traits ([`StreamFunction`](traits::StreamFunction),
/// [`Source`](traits::Source), [`Sink`](traits::Sink)).
pub mod traits;

#[cfg(test)]
mod compile_tests {
    use async_trait::async_trait;

    use crate::state::context::StateContext;
    use crate::traits::StreamFunction;

    /// Acceptance criteria from PLAN.md A-1: this code must compile.
    struct MyOp;

    #[async_trait]
    impl StreamFunction for MyOp {
        type Input = String;
        type Output = String;

        async fn process(
            &mut self,
            input: String,
            ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<String>> {
            let _ = ctx.get(b"key").await;
            Ok(vec![input])
        }
    }

    #[tokio::test]
    async fn my_op_compiles_and_runs() {
        use crate::state::local_backend::LocalBackend;
        let path = std::env::temp_dir().join(format!("rill_compile_test_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        let mut op = MyOp;

        let result = op.process("hello".to_string(), &mut ctx).await.unwrap();
        assert_eq!(result, vec!["hello".to_string()]);

        let _ = std::fs::remove_file(&path);
    }
}
