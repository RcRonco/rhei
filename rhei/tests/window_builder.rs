#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Tests for the fluent [`Window`] builders.
//!
//! Each test builds an operator through the builder and drives it directly,
//! asserting that the result matches what the equivalent positional
//! constructor produces.

use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema as RheiSchemaTrait,
    StreamFunction,
};
use rhei::{RheiSchema, Window};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

#[derive(Debug, RheiSchema)]
pub struct Reading {
    pub sensor: String,
    pub ts: u64,
    pub value: f64,
}

#[derive(Debug, RheiSchema)]
pub struct WindowOut {
    pub sensor: String,
    pub start: u64,
    pub end: u64,
    pub count: u64,
}

fn test_ctx(name: &str) -> OperatorContext {
    let path =
        std::env::temp_dir().join(format!("rhei_window_builder_{name}_{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let backend = LocalBackend::new(path, None).unwrap();
    OperatorContext::new(StateContext::new(Box::new(backend)))
}

fn readings(rows: &[(&str, u64)]) -> RheiBuffer<Reading> {
    let mut builder = Reading::builder(rows.len());
    for &(sensor, ts) in rows {
        builder.append(Reading {
            sensor: sensor.to_string(),
            ts,
            value: 1.0,
        });
    }
    RheiBuffer::from_builder(builder)
}

fn rows_of(output: BufferOutput<WindowOut>) -> Vec<(String, u64, u64, u64)> {
    let mut rows: Vec<(String, u64, u64, u64)> = match output {
        BufferOutput::None => Vec::new(),
        BufferOutput::Single(buf) => buf
            .iter()
            .map(|v| (v.sensor.to_string(), v.start, v.end, v.count))
            .collect(),
        BufferOutput::Multi(bufs) => bufs
            .iter()
            .flat_map(|buf| {
                buf.iter()
                    .map(|v| (v.sensor.to_string(), v.start, v.end, v.count))
                    .collect::<Vec<_>>()
            })
            .collect(),
    };
    rows.sort();
    rows
}

#[tokio::test]
async fn tumbling_builder_matches_positional_constructor() {
    let mut built = Window::tumbling(10)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .time(|v: ReadingView<'_>| v.ts)
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, start: u64, end: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end,
            count: *acc,
        })
        .build();

    let mut direct = rhei::TumblingWindow::new(
        10,
        |v: ReadingView<'_>| v.sensor.to_string(),
        |v: ReadingView<'_>| v.ts,
        |acc: &mut u64, _v: ReadingView<'_>| *acc += 1,
        |sensor: &str, start: u64, end: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end,
            count: *acc,
        },
    );

    let rows = [("a", 1), ("a", 5), ("b", 3), ("a", 12)];

    // A key moving to a later window closes the previous one during `process`,
    // so both outputs count.
    let mut built_ctx = test_ctx("tumbling_built");
    let mut built_out = rows_of(
        built
            .process(readings(&rows), &mut built_ctx)
            .await
            .unwrap(),
    );
    built_out.extend(rows_of(
        built.on_watermark(20, &mut built_ctx).await.unwrap(),
    ));
    built_out.sort();

    let mut direct_ctx = test_ctx("tumbling_direct");
    let mut direct_out = rows_of(
        direct
            .process(readings(&rows), &mut direct_ctx)
            .await
            .unwrap(),
    );
    direct_out.extend(rows_of(
        direct.on_watermark(20, &mut direct_ctx).await.unwrap(),
    ));
    direct_out.sort();

    assert_eq!(built_out, direct_out);
    assert_eq!(
        built_out,
        vec![
            ("a".to_string(), 0, 10, 2),
            ("a".to_string(), 10, 20, 1),
            ("b".to_string(), 0, 10, 1),
        ]
    );
}

#[tokio::test]
async fn slots_may_be_filled_in_any_order() {
    // Same window as above, declared finish-first.
    let mut win = Window::tumbling(10)
        .finish(|sensor: &str, start: u64, end: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end,
            count: *acc,
        })
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .time(|v: ReadingView<'_>| v.ts)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .build();

    let mut ctx = test_ctx("any_order");
    win.process(readings(&[("a", 1), ("a", 5)]), &mut ctx)
        .await
        .unwrap();
    let out = rows_of(win.on_watermark(20, &mut ctx).await.unwrap());
    assert_eq!(out, vec![("a".to_string(), 0, 10, 2)]);
}

#[tokio::test]
async fn allowed_lateness_is_applied() {
    let mut win = Window::tumbling(10)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .time(|v: ReadingView<'_>| v.ts)
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, start: u64, end: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end,
            count: *acc,
        })
        .allowed_lateness(50)
        .build();

    let mut ctx = test_ctx("lateness");
    win.process(readings(&[("a", 1)]), &mut ctx).await.unwrap();

    // Without the grace period this watermark would close window [0, 10).
    let out = rows_of(win.on_watermark(20, &mut ctx).await.unwrap());
    assert!(out.is_empty(), "window closed despite allowed lateness");

    let out = rows_of(win.on_watermark(60, &mut ctx).await.unwrap());
    assert_eq!(out, vec![("a".to_string(), 0, 10, 1)]);
}

#[tokio::test]
async fn sliding_builder_emits_overlapping_windows() {
    let mut win = Window::sliding(10, 5)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .time(|v: ReadingView<'_>| v.ts)
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, start: u64, end: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end,
            count: *acc,
        })
        .build();

    let mut ctx = test_ctx("sliding");
    win.process(readings(&[("a", 7)]), &mut ctx).await.unwrap();
    let out = rows_of(win.on_watermark(30, &mut ctx).await.unwrap());

    // ts=7 falls into both [0, 10) and [5, 15).
    assert_eq!(
        out,
        vec![("a".to_string(), 0, 10, 1), ("a".to_string(), 5, 15, 1)]
    );
}

#[tokio::test]
async fn session_builder_closes_after_the_gap() {
    let mut win = Window::session(10)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .time(|v: ReadingView<'_>| v.ts)
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, start: u64, last: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start,
            end: last,
            count: *acc,
        })
        .build();

    let mut ctx = test_ctx("session");
    win.process(readings(&[("a", 1), ("a", 4)]), &mut ctx)
        .await
        .unwrap();

    // Gap not yet elapsed.
    assert!(rows_of(win.on_watermark(10, &mut ctx).await.unwrap()).is_empty());

    let out = rows_of(win.on_watermark(30, &mut ctx).await.unwrap());
    assert_eq!(out, vec![("a".to_string(), 1, 4, 2)]);
}

#[tokio::test]
async fn count_builder_fires_on_threshold() {
    let mut win = Window::count(3)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, count: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start: 0,
            end: count,
            count: *acc,
        })
        .build();

    let mut ctx = test_ctx("count");

    // Two rows: below the threshold, nothing fires.
    let out = rows_of(
        win.process(readings(&[("a", 1), ("a", 2)]), &mut ctx)
            .await
            .unwrap(),
    );
    assert!(out.is_empty());

    // Third row trips the window.
    let out = rows_of(win.process(readings(&[("a", 3)]), &mut ctx).await.unwrap());
    assert_eq!(out, vec![("a".to_string(), 0, 3, 3)]);
}

fn assert_stream_fn<T: StreamFunction<Input = Reading, Output = WindowOut>>(_: &T) {}

#[tokio::test]
async fn built_operator_is_an_ordinary_stream_function() {
    // The builder is consumed at construction: what reaches the graph is the
    // same operator the positional constructor produces, with the same
    // `StreamFunction` input and output types.
    let built = Window::count(2)
        .key(|v: ReadingView<'_>| v.sensor.to_string())
        .accumulate(|acc: &mut u64, _v: ReadingView<'_>| *acc += 1)
        .finish(|sensor: &str, count: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start: 0,
            end: count,
            count: *acc,
        })
        .build::<Reading, WindowOut, u64>();

    let direct: rhei::CountWindow<Reading, WindowOut, u64, _, _, _> = rhei::CountWindow::new(
        2,
        |v: ReadingView<'_>| v.sensor.to_string(),
        |acc: &mut u64, _v: ReadingView<'_>| *acc += 1,
        |sensor: &str, count: u64, acc: &u64| WindowOut {
            sensor: sensor.to_string(),
            start: 0,
            end: count,
            count: *acc,
        },
    );

    assert_stream_fn(&built);
    assert_stream_fn(&direct);
}
