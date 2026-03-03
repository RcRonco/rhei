use rhei::StateContext;
use rhei::StreamFunction;

#[rhei::op_batch]
async fn batch_upper(inputs: Vec<String>, _ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
    Ok(inputs.into_iter().map(|s| s.to_uppercase()).collect())
}

#[tokio::test]
async fn op_batch_process_batch() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        rhei_core::state::local_backend::LocalBackend::new(dir.path().join("state.json"), None)
            .unwrap();
    let mut ctx = StateContext::new(Box::new(backend));
    let mut op = BatchUpper;

    let result = op
        .process_batch(vec!["hello".to_string(), "world".to_string()], &mut ctx)
        .await
        .unwrap();
    assert_eq!(result, vec!["HELLO".to_string(), "WORLD".to_string()]);
}

#[tokio::test]
async fn op_batch_process_single() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        rhei_core::state::local_backend::LocalBackend::new(dir.path().join("state.json"), None)
            .unwrap();
    let mut ctx = StateContext::new(Box::new(backend));
    let mut op = BatchUpper;

    // process() should delegate to process_batch(vec![input])
    let result = op.process("hello".to_string(), &mut ctx).await.unwrap();
    assert_eq!(result, vec!["HELLO".to_string()]);
}

#[test]
fn op_batch_struct_is_clone_and_debug() {
    let op = BatchUpper;
    let _cloned = op.clone();
    let _debug = format!("{op:?}");
}
