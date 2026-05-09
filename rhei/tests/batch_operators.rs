#![allow(clippy::unwrap_used, clippy::expect_used)]

use rhei::RheiSchema;
use rhei::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder,
    RheiSchema as RheiSchemaTrait,
};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

#[derive(Debug, RheiSchema)]
pub struct InputEvent {
    pub id: i64,
    pub name: String,
    pub score: f64,
}

#[derive(Debug, RheiSchema)]
pub struct OutputEvent {
    pub id: i64,
    pub upper_name: String,
}

fn test_ctx(name: &str) -> OperatorContext {
    let path =
        std::env::temp_dir().join(format!("rhei_batch_op_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let backend = LocalBackend::new(path, None).unwrap();
    OperatorContext::new(StateContext::new(Box::new(backend)))
}

fn sample_buffer() -> RheiBuffer<InputEvent> {
    let mut builder = InputEvent::builder(4);
    builder.append(InputEvent {
        id: 1,
        name: "alice".into(),
        score: 9.5,
    });
    builder.append(InputEvent {
        id: 2,
        name: "bob".into(),
        score: 4.0,
    });
    builder.append(InputEvent {
        id: 3,
        name: "carol".into(),
        score: 7.8,
    });
    builder.append(InputEvent {
        id: 4,
        name: "dave".into(),
        score: 2.1,
    });
    RheiBuffer::from_builder(builder)
}

#[tokio::test]
async fn batch_map_transforms_rows() {
    use rhei_core::operators::batch::BatchMapOp;

    let mut op =
        BatchMapOp::new(
            |view: <InputEvent as rhei::arrow::RheiSchema>::View<'_>| OutputEvent {
                id: view.id,
                upper_name: view.name.to_uppercase(),
            },
        );

    let mut ctx = test_ctx("map");
    let input = sample_buffer();
    let result = op.process(input, &mut ctx).await.unwrap();

    let BufferOutput::Single(buf) = result else {
        panic!("expected Single buffer output");
    };
    assert_eq!(buf.len(), 4);

    let v0 = OutputEvent::view(buf.as_record_batch(), 0);
    assert_eq!(v0.id, 1);
    assert_eq!(v0.upper_name, "ALICE");

    let v3 = OutputEvent::view(buf.as_record_batch(), 3);
    assert_eq!(v3.id, 4);
    assert_eq!(v3.upper_name, "DAVE");
}

#[tokio::test]
async fn batch_flat_map_expands_rows() {
    use rhei_core::operators::batch::BatchFlatMapOp;

    let mut op = BatchFlatMapOp::new(|view: <InputEvent as rhei::arrow::RheiSchema>::View<'_>| {
        if view.score > 5.0 {
            vec![
                OutputEvent {
                    id: view.id,
                    upper_name: view.name.to_uppercase(),
                },
                OutputEvent {
                    id: view.id * 10,
                    upper_name: format!("{}_DUP", view.name),
                },
            ]
        } else {
            vec![]
        }
    });

    let mut ctx = test_ctx("flatmap");
    let input = sample_buffer();
    let result = op.process(input, &mut ctx).await.unwrap();

    let BufferOutput::Single(buf) = result else {
        panic!("expected Single buffer output");
    };
    // alice (9.5 > 5) → 2 rows, bob (4.0 <= 5) → 0, carol (7.8 > 5) → 2, dave (2.1 <= 5) → 0
    assert_eq!(buf.len(), 4);
}

#[tokio::test]
async fn batch_filter_fn_zero_copy() {
    use rhei_core::operators::batch::BatchFilterFnOp;

    let mut op =
        BatchFilterFnOp::new(|view: &<InputEvent as rhei::arrow::RheiSchema>::View<'_>| {
            view.score > 5.0
        });

    let mut ctx = test_ctx("filter_fn");
    let input = sample_buffer();
    let result = op.process(input, &mut ctx).await.unwrap();

    let BufferOutput::Single(buf) = result else {
        panic!("expected Single buffer output");
    };
    // alice (9.5) and carol (7.8) pass
    assert_eq!(buf.len(), 2);

    let views: Vec<_> = buf.iter().collect();
    assert_eq!(views[0].id, 1);
    assert_eq!(views[0].name, "alice");
    assert_eq!(views[1].id, 3);
    assert_eq!(views[1].name, "carol");
}

#[tokio::test]
async fn batch_filter_fn_empty_result() {
    use rhei_core::operators::batch::BatchFilterFnOp;

    let mut op =
        BatchFilterFnOp::new(|view: &<InputEvent as rhei::arrow::RheiSchema>::View<'_>| {
            view.score > 100.0
        });

    let mut ctx = test_ctx("filter_empty");
    let input = sample_buffer();
    let result = op.process(input, &mut ctx).await.unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn batch_map_empty_input() {
    use rhei_core::operators::batch::BatchMapOp;

    let mut op =
        BatchMapOp::new(
            |view: <InputEvent as rhei::arrow::RheiSchema>::View<'_>| OutputEvent {
                id: view.id,
                upper_name: view.name.to_string(),
            },
        );

    let mut ctx = test_ctx("map_empty");
    let builder = InputEvent::builder(0);
    let input: RheiBuffer<InputEvent> = RheiBuffer::from_builder(builder);
    let result = op.process(input, &mut ctx).await.unwrap();

    assert!(result.is_empty());
}
