#![allow(clippy::unwrap_used, clippy::expect_used)]

use rhei::StateContext;
use rhei::StreamFunction;

#[rhei::op]
async fn to_upper(input: String, _ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
    Ok(vec![input.to_uppercase()])
}

#[rhei::op]
async fn duplicate(input: u64, _ctx: &mut StateContext) -> anyhow::Result<Vec<u64>> {
    Ok(vec![input, input])
}

#[tokio::test]
async fn op_to_upper() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        rhei_core::state::local_backend::LocalBackend::new(dir.path().join("state.json"), None)
            .unwrap();
    let mut ctx = StateContext::new(Box::new(backend));
    let mut op = ToUpper;

    let result = op.process("hello".to_string(), &mut ctx).await.unwrap();
    assert_eq!(result, vec!["HELLO".to_string()]);
}

#[tokio::test]
async fn op_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        rhei_core::state::local_backend::LocalBackend::new(dir.path().join("state.json"), None)
            .unwrap();
    let mut ctx = StateContext::new(Box::new(backend));
    let mut op = Duplicate;

    let result = op.process(42, &mut ctx).await.unwrap();
    assert_eq!(result, vec![42, 42]);
}

/// Verify the generated struct is Clone + Debug.
#[test]
fn op_struct_is_clone_and_debug() {
    let op = ToUpper;
    let _cloned = op.clone();
    let _debug = format!("{op:?}");
}
