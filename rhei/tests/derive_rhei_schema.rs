#![allow(clippy::unwrap_used, clippy::expect_used, clippy::approx_constant)]

use rhei::RheiSchema;
use rhei_core::arrow::{RheiBuilder, RheiSchema as RheiSchemaTrait};

// Basic struct with primitives + String
#[derive(Debug, RheiSchema)]
pub struct SimpleEvent {
    pub id: i64,
    pub name: String,
    pub score: f64,
    pub active: bool,
}

// Struct with Option and Vec fields
#[derive(Debug, RheiSchema)]
pub struct ComplexEvent {
    pub user_id: u64,
    pub email: Option<String>,
    pub timestamp: Option<i64>,
    pub tags: Vec<String>,
    pub scores: Vec<f32>,
}

#[test]
fn simple_schema_has_correct_fields() {
    let schema = SimpleEvent::arrow_schema();
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "score");
    assert_eq!(schema.field(3).name(), "active");
    assert!(!schema.field(0).is_nullable());
    assert!(!schema.field(3).is_nullable());
}

#[test]
fn complex_schema_nullable_fields() {
    let schema = ComplexEvent::arrow_schema();
    assert_eq!(schema.fields().len(), 5);
    assert!(!schema.field(0).is_nullable()); // user_id
    assert!(schema.field(1).is_nullable()); // email: Option<String>
    assert!(schema.field(2).is_nullable()); // timestamp: Option<i64>
    assert!(!schema.field(3).is_nullable()); // tags: Vec<String>
}

#[test]
fn column_constants_generated() {
    assert_eq!(SimpleEvent::ID, "id");
    assert_eq!(SimpleEvent::NAME, "name");
    assert_eq!(SimpleEvent::SCORE, "score");
    assert_eq!(SimpleEvent::ACTIVE, "active");

    assert_eq!(ComplexEvent::USER_ID, "user_id");
    assert_eq!(ComplexEvent::EMAIL, "email");
    assert_eq!(ComplexEvent::TAGS, "tags");
}

#[test]
fn builder_roundtrip_simple() {
    let mut builder = SimpleEvent::builder(4);
    builder.append(SimpleEvent {
        id: 1,
        name: "alice".into(),
        score: 9.5,
        active: true,
    });
    builder.append(SimpleEvent {
        id: 2,
        name: "bob".into(),
        score: 7.2,
        active: false,
    });
    builder.append(SimpleEvent {
        id: 3,
        name: "carol".into(),
        score: 8.8,
        active: true,
    });

    assert_eq!(builder.len(), 3);
    assert!(!builder.is_empty());

    let batch = builder.finish();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 4);
}

#[test]
fn view_zero_copy_access() {
    let mut builder = SimpleEvent::builder(2);
    builder.append(SimpleEvent {
        id: 42,
        name: "test_user".into(),
        score: 3.14,
        active: true,
    });
    builder.append(SimpleEvent {
        id: 99,
        name: "another".into(),
        score: 2.71,
        active: false,
    });

    let batch = builder.finish();

    let view0 = SimpleEvent::view(&batch, 0);
    assert_eq!(view0.id, 42);
    assert_eq!(view0.name, "test_user");
    assert!((view0.score - 3.14).abs() < f64::EPSILON);
    assert!(view0.active);

    let view1 = SimpleEvent::view(&batch, 1);
    assert_eq!(view1.id, 99);
    assert_eq!(view1.name, "another");
    assert!(!view1.active);
}

#[test]
fn view_nullable_fields() {
    let mut builder = ComplexEvent::builder(3);
    builder.append(ComplexEvent {
        user_id: 1,
        email: Some("a@b.com".into()),
        timestamp: Some(1000),
        tags: vec!["rust".into(), "arrow".into()],
        scores: vec![1.0, 2.0],
    });
    builder.append(ComplexEvent {
        user_id: 2,
        email: None,
        timestamp: None,
        tags: vec![],
        scores: vec![],
    });

    let batch = builder.finish();

    let v0 = ComplexEvent::view(&batch, 0);
    assert_eq!(v0.user_id, 1);
    assert_eq!(v0.email, Some("a@b.com"));
    assert_eq!(v0.timestamp, Some(1000));
    assert_eq!(v0.tags, vec!["rust".to_string(), "arrow".to_string()]);
    assert_eq!(v0.scores, vec![1.0_f32, 2.0]);

    let v1 = ComplexEvent::view(&batch, 1);
    assert_eq!(v1.user_id, 2);
    assert_eq!(v1.email, None);
    assert_eq!(v1.timestamp, None);
    assert!(v1.tags.is_empty());
    assert!(v1.scores.is_empty());
}

#[test]
fn columns_typed_access() {
    let mut builder = SimpleEvent::builder(2);
    builder.append(SimpleEvent {
        id: 10,
        name: "x".into(),
        score: 1.0,
        active: true,
    });
    builder.append(SimpleEvent {
        id: 20,
        name: "y".into(),
        score: 2.0,
        active: false,
    });

    let batch = builder.finish();
    let cols = SimpleEvent::columns(&batch);

    assert_eq!(cols.id.value(0), 10);
    assert_eq!(cols.id.value(1), 20);
    assert_eq!(cols.name.value(0), "x");
    assert_eq!(cols.name.value(1), "y");
    assert!(cols.active.value(0));
    assert!(!cols.active.value(1));
}

#[test]
fn buffer_integration() {
    use rhei::RheiBuffer;

    let mut builder = SimpleEvent::builder(4);
    builder.append(SimpleEvent {
        id: 1,
        name: "a".into(),
        score: 1.0,
        active: true,
    });
    builder.append(SimpleEvent {
        id: 2,
        name: "b".into(),
        score: 2.0,
        active: false,
    });
    builder.append(SimpleEvent {
        id: 3,
        name: "c".into(),
        score: 3.0,
        active: true,
    });

    let buf = RheiBuffer::<SimpleEvent>::from_builder(builder);
    assert_eq!(buf.len(), 3);

    // Iterate
    let views: Vec<_> = buf.iter().collect();
    assert_eq!(views.len(), 3);
    assert_eq!(views[0].id, 1);
    assert_eq!(views[2].name, "c");

    // Apply mask and iterate
    let mask = arrow_array::BooleanArray::from(vec![true, false, true]);
    let masked = buf.with_mask(mask);
    assert_eq!(masked.len(), 2);

    let masked_views: Vec<_> = masked.iter().collect();
    assert_eq!(masked_views.len(), 2);
    assert_eq!(masked_views[0].id, 1);
    assert_eq!(masked_views[1].id, 3);
}

// ── Typed column handles ─────────────────────────────────────────────

#[test]
fn typed_col_handles_carry_field_names() {
    assert_eq!(SimpleEvent::col().id().name(), "id");
    assert_eq!(SimpleEvent::col().name().name(), "name");
    assert_eq!(SimpleEvent::col().score().name(), "score");
    assert_eq!(SimpleEvent::col().active().name(), "active");

    assert_eq!(ComplexEvent::col().user_id().name(), "user_id");
    assert_eq!(ComplexEvent::col().email().name(), "email");
    assert_eq!(ComplexEvent::col().tags().name(), "tags");
}

#[test]
fn typed_col_handles_are_typed_by_field() {
    // Each accessor pins the literal type: these compile only because the
    // literal matches the field's Rust type.
    let _: rhei::Col<i64> = SimpleEvent::col().id();
    let _: rhei::Col<String> = SimpleEvent::col().name();
    let _: rhei::Col<f64> = SimpleEvent::col().score();
    let _: rhei::Col<bool> = SimpleEvent::col().active();

    // Option<T> fields compare against plain T.
    let _: rhei::Col<String> = ComplexEvent::col().email();
    let _: rhei::Col<i64> = ComplexEvent::col().timestamp();
    let _: rhei::Col<u64> = ComplexEvent::col().user_id();
}

#[test]
fn typed_col_predicates_filter_a_batch() {
    use rhei::RheiBuffer;

    let mut builder = SimpleEvent::builder(3);
    builder.append(SimpleEvent {
        id: 1,
        name: "a".into(),
        score: 1.0,
        active: true,
    });
    builder.append(SimpleEvent {
        id: 2,
        name: "b".into(),
        score: 5.0,
        active: false,
    });
    builder.append(SimpleEvent {
        id: 3,
        name: "c".into(),
        score: 9.0,
        active: true,
    });
    let buf = RheiBuffer::<SimpleEvent>::from_builder(builder);
    let batch = buf.as_record_batch();

    let expr = SimpleEvent::col()
        .score()
        .gt(2.0)
        .and(SimpleEvent::col().name().is_in(["b", "z"]));
    let mask = rhei_core::operators::filter_expr::eval_predicate(&expr, batch).unwrap();
    assert_eq!(
        (0..batch.num_rows())
            .map(|i| mask.value(i))
            .collect::<Vec<_>>(),
        vec![false, true, false]
    );
}

#[test]
fn typed_col_null_predicate_on_optional_field() {
    use rhei::RheiBuffer;

    let mut builder = ComplexEvent::builder(2);
    builder.append(ComplexEvent {
        user_id: 1,
        email: Some("a@example.com".into()),
        timestamp: Some(10),
        tags: vec!["x".into()],
        scores: vec![1.0],
    });
    builder.append(ComplexEvent {
        user_id: 2,
        email: None,
        timestamp: None,
        tags: vec![],
        scores: vec![],
    });
    let buf = RheiBuffer::<ComplexEvent>::from_builder(builder);
    let batch = buf.as_record_batch();

    let expr = ComplexEvent::col().email().is_null();
    let mask = rhei_core::operators::filter_expr::eval_predicate(&expr, batch).unwrap();
    assert!(!mask.value(0));
    assert!(mask.value(1));
}
