#![allow(clippy::unwrap_used, clippy::expect_used)]

use rhei::RheiSchema as RheiSchemaDerive;
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Word {
    text: String,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Upper {
    text: String,
}

#[rhei::op_batch]
async fn batch_upper(
    input: RheiBuffer<Word>,
    _ctx: &mut OperatorContext,
) -> anyhow::Result<BufferOutput<Upper>> {
    if input.is_empty() {
        return Ok(BufferOutput::None);
    }
    let mut builder = Upper::builder(input.len());
    for view in &input {
        builder.append(Upper {
            text: view.text.to_uppercase(),
        });
    }
    Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
}

fn test_ctx() -> OperatorContext {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
    OperatorContext::new(StateContext::new(Box::new(backend)))
}

fn make_input(words: &[&str]) -> RheiBuffer<Word> {
    let mut builder = Word::builder(words.len());
    for &w in words {
        builder.append(Word {
            text: w.to_string(),
        });
    }
    RheiBuffer::from_builder(builder)
}

#[tokio::test]
async fn op_batch_process_batch() {
    use rhei::StreamFunction;

    let mut ctx = test_ctx();
    let mut op = BatchUpper;

    let input = make_input(&["hello", "world"]);
    let result = op.process(input, &mut ctx).await.unwrap();
    let BufferOutput::Single(buf) = result else {
        panic!("expected Single output");
    };
    assert_eq!(buf.len(), 2);
    let v0 = Upper::view(buf.as_record_batch(), 0);
    let v1 = Upper::view(buf.as_record_batch(), 1);
    assert_eq!(v0.text, "HELLO");
    assert_eq!(v1.text, "WORLD");
}

#[tokio::test]
async fn op_batch_process_single() {
    use rhei::StreamFunction;

    let mut ctx = test_ctx();
    let mut op = BatchUpper;

    let input = make_input(&["hello"]);
    let result = op.process(input, &mut ctx).await.unwrap();
    let BufferOutput::Single(buf) = result else {
        panic!("expected Single output");
    };
    assert_eq!(buf.len(), 1);
    let v = Upper::view(buf.as_record_batch(), 0);
    assert_eq!(v.text, "HELLO");
}

#[test]
fn op_batch_struct_is_clone_and_debug() {
    let op = BatchUpper;
    let _cloned = op.clone();
    let _debug = format!("{op:?}");
}
