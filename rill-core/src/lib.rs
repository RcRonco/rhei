pub mod connectors;
pub mod event;
pub mod graph;
pub mod state;
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

        async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
            let _ = ctx.get(b"key").await;
            vec![input]
        }
    }

    #[tokio::test]
    async fn my_op_compiles_and_runs() {
        use crate::state::local_backend::LocalBackend;
        let path =
            std::env::temp_dir().join(format!("rill_compile_test_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        let mut op = MyOp;

        let result = op.process("hello".to_string(), &mut ctx).await;
        assert_eq!(result, vec!["hello".to_string()]);

        let _ = std::fs::remove_file(&path);
    }
}
