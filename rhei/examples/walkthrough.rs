//! The finished pipeline from `docs/walkthrough.md`.
//!
//! Clickstream analytics for a web property. Page views are filtered, keyed by
//! user, and fanned out to two branches: a stateful funnel tracker that emits a
//! conversion the first time a user reaches checkout having already visited the
//! cart, and a session window that summarises each user's visit. Both branches
//! are collected in memory and asserted at the end, so the example doubles as
//! an end-to-end check.
//!
//! Run with: `cargo run -p rhei --example walkthrough`

use std::sync::{Arc, Mutex};

use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei::{DataflowGraph, KeyedState, PipelineController, SessionWindow, VecSource};

// ── Schemas ─────────────────────────────────────────────────────────

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    path: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Conversion {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct SessionSummary {
    user_id: String,
    started_at: u64,
    ended_at: u64,
    views: u64,
}

// ── Stateful operator ───────────────────────────────────────────────

/// The checkout funnel: `/product` → `/cart` → `/checkout`.
///
/// Emits a `Conversion` the first time a user reaches the final step having
/// already passed through the previous ones. The furthest step reached lives in
/// keyed state, so a user who reloads `/checkout` converts once, not twice.
#[derive(Clone)]
struct FunnelTracker;

impl FunnelTracker {
    /// Position of a path in the funnel, or `None` if it is not a funnel step.
    fn step(path: &str) -> Option<u8> {
        match path {
            "/product" => Some(1),
            "/cart" => Some(2),
            "/checkout" => Some(3),
            _ => None,
        }
    }

    const FINAL_STEP: u8 = 3;
}

#[async_trait::async_trait]
impl StreamFunction for FunnelTracker {
    type Input = PageView;
    type Output = Conversion;

    async fn process(
        &mut self,
        input: RheiBuffer<PageView>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Conversion>> {
        let mut builder = Conversion::builder(input.len());
        let mut furthest = KeyedState::<String, u8>::new(&mut ctx.state, "funnel_step");

        for view in &input {
            let Some(step) = Self::step(view.path) else {
                continue;
            };

            let user_id = view.user_id.to_string();
            let reached = furthest.get(&user_id).await?.unwrap_or(0);

            // Only advance one step at a time: jumping straight to /checkout
            // without a cart is not a funnel completion.
            if step == reached + 1 {
                furthest.put(&user_id, &step)?;

                if step == Self::FINAL_STEP {
                    builder.append(Conversion {
                        user_id,
                        ts: view.ts,
                    });
                }
            }
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Collecting sink ─────────────────────────────────────────────────

/// The pattern used throughout `rhei-runtime/tests/`: pair `VecSource` with a
/// sink that collects into a shared `Vec` and assert on it afterwards.
#[derive(Clone)]
struct CollectSink<T> {
    rows: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectSink<T> {
    fn new() -> (Self, Arc<Mutex<Vec<T>>>) {
        let rows = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                rows: Arc::clone(&rows),
            },
            rows,
        )
    }
}

#[async_trait::async_trait]
impl Sink for CollectSink<String> {
    type Input = Conversion;

    async fn write_batch(&mut self, batch: RheiBuffer<Conversion>) -> anyhow::Result<()> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|_| anyhow::anyhow!("conversion sink lock poisoned"))?;
        for view in &batch {
            rows.push(view.user_id.to_string());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for CollectSink<(String, u64)> {
    type Input = SessionSummary;

    async fn write_batch(&mut self, batch: RheiBuffer<SessionSummary>) -> anyhow::Result<()> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|_| anyhow::anyhow!("session sink lock poisoned"))?;
        for view in &batch {
            rows.push((view.user_id.to_string(), view.views));
        }
        Ok(())
    }
}

// ── Pipeline ────────────────────────────────────────────────────────

const MINUTE: u64 = 60_000;

fn page_views() -> Vec<PageView> {
    // alice walks the whole funnel and then reloads checkout.
    // bob browses products but never converts.
    // The health check is noise the pipeline filters out.
    vec![
        PageView {
            user_id: "alice".into(),
            path: "/product".into(),
            ts: MINUTE,
        },
        PageView {
            user_id: "bob".into(),
            path: "/product".into(),
            ts: 2 * MINUTE,
        },
        PageView {
            user_id: "monitoring".into(),
            path: "/health".into(),
            ts: 3 * MINUTE,
        },
        PageView {
            user_id: "alice".into(),
            path: "/cart".into(),
            ts: 4 * MINUTE,
        },
        PageView {
            user_id: "bob".into(),
            path: "/product".into(),
            ts: 5 * MINUTE,
        },
        PageView {
            user_id: "alice".into(),
            path: "/checkout".into(),
            ts: 6 * MINUTE,
        },
        // A reload: same page, already converted, must not convert again.
        PageView {
            user_id: "alice".into(),
            path: "/checkout".into(),
            ts: 7 * MINUTE,
        },
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_walkthrough_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let (conversion_sink, conversions) = CollectSink::<String>::new();
    let (session_sink, sessions) = CollectSink::<(String, u64)>::new();

    // Sessions close after 30 minutes of inactivity for that user.
    let session_window = SessionWindow::new(
        30 * MINUTE,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |v: PageViewView<'_>| v.ts,
        |acc: &mut u64, _v: PageViewView<'_>| *acc += 1,
        |user_id: &str, started_at: u64, ended_at: u64, views: &u64| SessionSummary {
            user_id: user_id.to_string(),
            started_at,
            ended_at,
            views: *views,
        },
    );

    let graph = DataflowGraph::new();

    // `Stream` is `Copy`, so reusing the keyed handle fans out to two branches.
    let keyed = graph
        .source(VecSource::new(page_views()))
        .filter_fn(|v| !v.path.starts_with("/health"))
        .key_by(|v| v.user_id.to_string());

    keyed
        .operator("funnel_tracker", FunnelTracker)
        .name("conversions")
        .sink(conversion_sink);

    keyed
        .operator("session_window", session_window)
        .name("sessions")
        .sink(session_sink);

    PipelineController::builder()
        .checkpoint_dir(&dir)
        .workers(1)
        .build()?
        .run(graph)
        .await?;

    // ── Results ─────────────────────────────────────────────────────

    let conversions = conversions
        .lock()
        .map_err(|_| anyhow::anyhow!("conversion lock poisoned"))?
        .clone();
    let mut sessions = sessions
        .lock()
        .map_err(|_| anyhow::anyhow!("session lock poisoned"))?
        .clone();
    sessions.sort_by(|a, b| a.0.cmp(&b.0));

    println!("conversions:");
    for user_id in &conversions {
        println!("  {user_id} completed the funnel");
    }

    println!("sessions:");
    for (user_id, views) in &sessions {
        println!("  {user_id}: {views} page views");
    }

    // alice converts exactly once, despite reloading /checkout.
    assert_eq!(conversions, vec!["alice".to_string()]);

    // The /health view was filtered out, so "monitoring" has no session at all.
    assert_eq!(sessions.len(), 2, "expected one session per real user");
    assert_eq!(sessions[0], ("alice".to_string(), 4));
    assert_eq!(sessions[1], ("bob".to_string(), 2));

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
