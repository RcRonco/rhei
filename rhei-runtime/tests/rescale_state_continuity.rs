#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: keyed state survives a change in worker count.
//!
//! This is the property that makes dynamic rescaling possible, and the one that
//! was impossible before key groups. Previously two things were tied to the
//! worker count:
//!
//! 1. routing was `hash(key) % num_workers`, so changing the count rehashed
//!    every key onto a different worker; and
//! 2. state was namespaced `p{process_id}/w{worker_index}/{operator}`, so the
//!    persisted bytes stayed under a prefix nobody would look up again.
//!
//! Together those meant a rescale silently reset all keyed state. Now keys map
//! to a fixed set of key groups, groups are assigned to workers in contiguous
//! ranges, and state is addressed by group — so ownership can move without the
//! data moving.
//!
//! The test runs a counting pipeline over the same checkpoint directory at
//! several worker counts and asserts the counters keep accumulating across each
//! change. Under the old scheme the second run would start from zero.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::cluster::{DEFAULT_MAX_PARALLELISM, KeyGroupAssignment, key_group_for};
use rhei_core::connectors::batch::VecSource;
use rhei_core::operators::keyed_state::KeyedState;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Input schema ──────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct KeyedEvent {
    key: String,
}

struct KeyedEventBuilder {
    key: arrow_array::builder::StringBuilder,
}

impl RheiBuilder for KeyedEventBuilder {
    type Item = KeyedEvent;

    fn append(&mut self, item: KeyedEvent) {
        self.key.append_value(&item.key);
    }
    fn append_null(&mut self) {
        self.key.append_null();
    }
    fn finish(mut self) -> arrow_array::RecordBatch {
        arrow_array::RecordBatch::try_new(
            KeyedEvent::arrow_schema(),
            vec![std::sync::Arc::new(self.key.finish())],
        )
        .unwrap()
    }
    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }
}

struct KeyedEventView<'a> {
    key: &'a str,
}

struct KeyedEventColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
}

impl RheiSchema for KeyedEvent {
    type Builder = KeyedEventBuilder;
    type View<'a> = KeyedEventView<'a>;
    type Columns<'a> = KeyedEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "key",
            arrow_schema::DataType::Utf8,
            false,
        )]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        KeyedEventBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }
    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        KeyedEventView {
            key: batch.column(0).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        KeyedEventColumns {
            key: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Output schema ─────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct KeyCount {
    key: String,
    count: u64,
}

struct KeyCountBuilder {
    key: arrow_array::builder::StringBuilder,
    count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

impl RheiBuilder for KeyCountBuilder {
    type Item = KeyCount;

    fn append(&mut self, item: KeyCount) {
        self.key.append_value(&item.key);
        self.count.append_value(item.count);
    }
    fn append_null(&mut self) {
        self.key.append_null();
        self.count.append_null();
    }
    fn finish(mut self) -> arrow_array::RecordBatch {
        arrow_array::RecordBatch::try_new(
            KeyCount::arrow_schema(),
            vec![
                std::sync::Arc::new(self.key.finish()),
                std::sync::Arc::new(self.count.finish()),
            ],
        )
        .unwrap()
    }
    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }
}

struct KeyCountView<'a> {
    key: &'a str,
    count: u64,
}

struct KeyCountColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
}

impl RheiSchema for KeyCount {
    type Builder = KeyCountBuilder;
    type View<'a> = KeyCountView<'a>;
    type Columns<'a> = KeyCountColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
        ]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        KeyCountBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }
    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        KeyCountView {
            key: batch.column(0).as_string::<i32>().value(index),
            count: batch.column(1).as_primitive::<UInt64Type>().value(index),
        }
    }
    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        KeyCountColumns {
            key: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Operator and sink ─────────────────────────────────────────────────

#[derive(Clone)]
struct KeyCounter;

#[async_trait]
impl StreamFunction for KeyCounter {
    type Input = KeyedEvent;
    type Output = KeyCount;

    async fn process(
        &mut self,
        input: RheiBuffer<KeyedEvent>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<KeyCount>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }
        let keys: Vec<String> = input.iter().map(|v| v.key.to_string()).collect();
        let mut outputs = Vec::with_capacity(keys.len());
        for key in keys {
            let count: u64 = {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.get(&key).await?.unwrap_or(0)
            };
            let new_count = count + 1;
            {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.put(&key, &new_count)?;
            }
            outputs.push(KeyCount {
                key,
                count: new_count,
            });
        }
        let mut builder = KeyCount::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

struct CollectSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = KeyCount;
    async fn write_batch(&mut self, input: RheiBuffer<KeyCount>) -> anyhow::Result<()> {
        let mut guard = self
            .collected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for view in &input {
            guard.push((view.key.to_string(), view.count));
        }
        Ok(())
    }
}

// ── Harness ───────────────────────────────────────────────────────────

const KEYS: [&str; 8] = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
];
/// Events per key, per run.
const PER_KEY: usize = 5;

/// Run the counting pipeline once at `workers`, reusing `checkpoint_dir`.
///
/// Returns the highest count observed per key during this run.
async fn run_once(checkpoint_dir: &std::path::Path, workers: usize) -> HashMap<String, u64> {
    let events: Vec<KeyedEvent> = KEYS
        .iter()
        .flat_map(|k| {
            (0..PER_KEY).map(move |_| KeyedEvent {
                key: (*k).to_string(),
            })
        })
        .collect();

    let collected = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
    let graph = DataflowGraph::new();
    graph
        .source(VecSource::new(events).with_batch_size(4))
        .key_by(|view: &KeyedEventView<'_>| view.key.to_string())
        .operator("key_counter", KeyCounter)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::builder()
        .checkpoint_dir(checkpoint_dir.to_path_buf())
        .workers(workers)
        .build()
        .unwrap();
    ctrl.run(graph).await.unwrap();

    let results = collected
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();

    let mut max_per_key: HashMap<String, u64> = HashMap::new();
    for (key, count) in results {
        let entry = max_per_key.entry(key).or_insert(0);
        *entry = (*entry).max(count);
    }
    max_per_key
}

#[tokio::test]
async fn keyed_state_survives_rescaling_the_worker_count() {
    let dir = tempfile::tempdir().unwrap();

    // Scale up, then down, then up again — each run reuses the state the
    // previous one checkpointed, at a different parallelism.
    let schedule = [1usize, 2, 4, 3, 8, 2];

    for (run_idx, &workers) in schedule.iter().enumerate() {
        let counts = run_once(dir.path(), workers).await;
        let expected = ((run_idx + 1) * PER_KEY) as u64;

        for key in KEYS {
            let actual = counts.get(key).copied().unwrap_or(0);
            assert_eq!(
                actual, expected,
                "after run {run_idx} at {workers} worker(s), key {key:?} should have \
                 accumulated to {expected} but was {actual}. A count equal to {PER_KEY} \
                 means state was lost across the worker-count change."
            );
        }
    }
}

/// Read every physical key the pipeline persisted for the `key_counter`
/// operator out of its `LocalBackend` checkpoint file.
fn persisted_physical_keys(checkpoint_dir: &std::path::Path) -> Vec<String> {
    let path = checkpoint_dir.join("key_counter.checkpoint.json");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let entries: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_str(&contents).unwrap();
    entries
        .into_iter()
        .map(|(k, _)| String::from_utf8(k).expect("physical keys are UTF-8 here"))
        .collect()
}

#[tokio::test]
async fn state_is_filed_in_a_key_group_the_routing_worker_owns() {
    // The invariant the whole key-group design rests on, checked end to end
    // against what the runtime actually wrote to disk: for every key, the
    // worker the exchange routes the row to must be the worker that owns the
    // key group the row's state landed in.
    //
    // This fails if the key group is derived from the *storage* key rather than
    // the record's partition key. `KeyedState` stores under `counts:"alpha"`,
    // so hashing that puts the entry in a different group than routing put
    // `alpha` in — a mismatch that is invisible to correctness tests (every
    // worker still derives the same physical key, so reads and writes agree)
    // but silently breaks every per-key-group operation built on top.
    for workers in [1usize, 2, 3, 5] {
        let dir = tempfile::tempdir().unwrap();
        run_once(dir.path(), workers).await;

        let assignment = KeyGroupAssignment::new(DEFAULT_MAX_PARALLELISM, workers).unwrap();
        let keys = persisted_physical_keys(dir.path());
        assert!(!keys.is_empty(), "pipeline persisted no state at all");

        let mut checked = 0;
        for physical in &keys {
            // Layout: kg{group:05}/{operator}/{namespace}:{json_key}
            let (group_part, rest) = physical.split_once('/').expect("group component");
            let group: u16 = group_part
                .strip_prefix("kg")
                .expect("kg prefix")
                .parse()
                .expect("numeric group");
            let (_operator, state_key) = rest.split_once('/').expect("operator component");
            let json_key = state_key
                .strip_prefix("counts:")
                .expect("KeyedState namespace");
            let record_key: String = serde_json::from_str(json_key).expect("json-encoded key");

            let routed_to = assignment.worker_for_key(record_key.as_bytes());
            let owner_of_state =
                rhei_core::cluster::worker_for_key_group(DEFAULT_MAX_PARALLELISM, workers, group);

            assert_eq!(
                owner_of_state, routed_to,
                "at {workers} worker(s), rows for {record_key:?} route to worker \
                 {routed_to}, but its state sits in key group {group}, which worker \
                 {owner_of_state} owns. State must be grouped by the partition key, \
                 not by the storage key."
            );

            // And the group must be exactly the one the partition key hashes to.
            assert_eq!(
                group,
                key_group_for(record_key.as_bytes(), DEFAULT_MAX_PARALLELISM),
                "key {record_key:?} filed in group {group}, not its own key group"
            );
            checked += 1;
        }
        assert_eq!(
            checked,
            KEYS.len(),
            "expected one persisted entry per key at {workers} worker(s)"
        );
    }
}

#[tokio::test]
async fn every_key_stays_in_its_key_group_across_rescales() {
    // The routing-side invariant behind the test above: a key's group is a
    // function of max_parallelism alone, and each rescale only reassigns whole
    // groups to workers.
    let keys: Vec<String> = (0..500).map(|i| format!("user-{i}")).collect();

    let baseline: Vec<u16> = keys
        .iter()
        .map(|k| key_group_for(k.as_bytes(), DEFAULT_MAX_PARALLELISM))
        .collect();

    for workers in [1usize, 2, 3, 7, 16, 64] {
        let assignment = KeyGroupAssignment::new(DEFAULT_MAX_PARALLELISM, workers).unwrap();
        for (key, &expected_group) in keys.iter().zip(&baseline) {
            let group = key_group_for(key.as_bytes(), DEFAULT_MAX_PARALLELISM);
            assert_eq!(
                group, expected_group,
                "key {key:?} changed key group at {workers} workers"
            );
            // And the worker it routes to is one that owns that group.
            let worker = assignment.worker_for_key(key.as_bytes());
            assert!(
                assignment.range(worker).contains(group),
                "key {key:?} routed to worker {worker} which does not own group {group} \
                 at {workers} workers"
            );
        }
    }
}

#[tokio::test]
async fn rescaling_moves_only_a_minority_of_key_groups() {
    // Contiguous ranges mean a small scale change relocates a proportionate
    // slice of the key space, not all of it. This is what keeps the
    // cache-warming cost of a rescale bounded.
    let before = KeyGroupAssignment::new(DEFAULT_MAX_PARALLELISM, 8).unwrap();
    let after = KeyGroupAssignment::new(DEFAULT_MAX_PARALLELISM, 9).unwrap();

    let moved = after.moved_key_groups(&before);
    assert!(moved > 0, "scaling 8 -> 9 must move some key groups");
    assert!(
        moved <= DEFAULT_MAX_PARALLELISM / 2,
        "scaling 8 -> 9 workers moved {moved}/{DEFAULT_MAX_PARALLELISM} key groups; \
         contiguous-range assignment should relocate far fewer than half"
    );
}

/// A source that emits one batch at a time with a delay, so a generation stays
/// alive long enough for a membership change to land mid-run.
struct SlowSource {
    remaining: Vec<KeyedEvent>,
    delay: std::time::Duration,
}

#[async_trait]
impl rhei_core::arrow::Source for SlowSource {
    type Output = KeyedEvent;

    async fn next_batch(&mut self) -> Option<RheiBuffer<KeyedEvent>> {
        let item = self.remaining.pop()?;
        tokio::time::sleep(self.delay).await;
        let mut builder = KeyedEvent::builder(1);
        builder.append(item);
        Some(RheiBuffer::from_builder(builder))
    }
}

// ── Live rescale through run_dynamic ──────────────────────────────────

/// A membership provider driven from the test.
#[derive(Debug)]
struct FakeMembership {
    node_id: String,
    view: Mutex<rhei_core::cluster::ClusterView>,
    notify: tokio::sync::Notify,
}

impl FakeMembership {
    fn new(node_id: &str, workers: usize) -> Arc<Self> {
        Arc::new(Self {
            node_id: node_id.to_string(),
            view: Mutex::new(Self::view_of(node_id, workers)),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn view_of(node_id: &str, workers: usize) -> rhei_core::cluster::ClusterView {
        rhei_core::cluster::ClusterView::new(vec![rhei_core::cluster::Member::new(
            node_id,
            "127.0.0.1:2101",
            workers,
        )])
    }

    /// Publish a new advertised worker count, as a node resize would.
    fn set_workers(&self, workers: usize) {
        *self
            .view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Self::view_of(&self.node_id, workers);
        self.notify.notify_waiters();
    }
}

#[async_trait]
impl rhei_core::cluster::MembershipProvider for FakeMembership {
    async fn current_view(&self) -> anyhow::Result<rhei_core::cluster::ClusterView> {
        Ok(self
            .view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }

    async fn changed(&self) -> Option<rhei_core::cluster::ClusterView> {
        self.notify.notified().await;
        Some(
            self.view
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }
}

#[tokio::test]
async fn run_dynamic_rescales_the_pipeline_on_a_membership_change() {
    // End-to-end: a membership change while the pipeline is running must drain
    // the current generation to a checkpoint and start a fresh one at the new
    // worker count, with keyed state carried across.
    let dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
    let generations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let provider = FakeMembership::new("node-a", 2);

    let ctrl = PipelineController::builder()
        .checkpoint_dir(dir.path().to_path_buf())
        .workers(2)
        .build()
        .unwrap();

    let build_collected = collected.clone();
    let build_generations = generations.clone();
    let build_graph = move || {
        build_generations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let events: Vec<KeyedEvent> = KEYS
            .iter()
            .flat_map(|k| {
                (0..PER_KEY).map(move |_| KeyedEvent {
                    key: (*k).to_string(),
                })
            })
            .collect();
        let graph = DataflowGraph::new();
        graph
            .source(SlowSource {
                remaining: events,
                delay: std::time::Duration::from_millis(20),
            })
            .key_by(|view: &KeyedEventView<'_>| view.key.to_string())
            .operator("key_counter", KeyCounter)
            .sink(CollectSink {
                collected: build_collected.clone(),
            });
        graph
    };

    // Short debounce so the test does not wait out the 5s production default.
    let policy = rhei_runtime::cluster::RescalePolicy::new(ctrl.max_parallelism())
        .with_debounce(std::time::Duration::from_millis(50));

    // Resize the node while the first generation is running.
    let resizer = provider.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        resizer.set_workers(4);
    });

    let (shutdown, trigger) = rhei_runtime::shutdown::ShutdownHandle::new();

    // The source is finite, so a generation ends on its own; stop the loop
    // shortly after the rescale so the test terminates deterministically.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        trigger.shutdown();
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        ctrl.run_dynamic(build_graph, provider.clone(), policy, Some(shutdown)),
    )
    .await
    .expect("run_dynamic did not settle within 30s")
    .expect("run_dynamic returned an error");

    let gens = generations.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        gens >= 2,
        "expected the membership change to start a second pipeline generation, \
         but only {gens} generation(s) were built — the rescale did not happen"
    );

    // Whatever the interleaving, the counters must be monotonic per key: a
    // rescale that lost state would show a count restarting below one already
    // emitted for that key.
    let results = collected
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(!results.is_empty(), "pipeline produced no output");

    let mut highest: HashMap<String, u64> = HashMap::new();
    for (key, count) in &results {
        let entry = highest.entry(key.clone()).or_insert(0);
        assert!(
            *count > *entry || *entry == 0,
            "key {key:?} emitted count {count} after already reaching {entry} — \
             state was lost or double-counted across a rescale"
        );
        *entry = (*entry).max(*count);
    }

    // Every key was seen and counted at least once per generation that ran.
    for key in KEYS {
        assert!(
            highest.get(key).copied().unwrap_or(0) >= PER_KEY as u64,
            "key {key:?} did not reach the expected count"
        );
    }
}
