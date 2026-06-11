#![allow(clippy::unwrap_used, clippy::expect_used, clippy::option_option)]
//! Model-based fuzz tests for `rhei-core` state primitives.
//!
//! Each test drives a primitive with a randomized op sequence while a tiny
//! reference *model* mirrors the same ops; after every step the real
//! implementation must agree with the model. This catches subtle ordering /
//! tombstone / dirty-tracking bugs that example-based tests miss.
//!
//! Covered:
//! * [`MemTable`] — dirty/clean separation, tombstones, flush, prefix scans.
//! * [`TimerService`] — watermark-ordered draining, no double-fire.
//! * [`PrefixedBackend`] — cross-operator key isolation.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use rstest::rstest;

use rhei_core::state::backend::StateBackend;
use rhei_core::state::memtable::MemTable;
use rhei_core::state::prefixed_backend::PrefixedBackend;
use rhei_core::state::timer_service::TimerService;

// ══════════════════════════════════════════════════════════════════
// MemTable: model-based fuzzing
// ══════════════════════════════════════════════════════════════════

/// A single memtable operation, over a deliberately small key universe so the
/// op sequence revisits the same keys (exercising overwrite / tombstone paths).
#[derive(Debug, Clone)]
enum MemOp {
    Put(u8, u8),
    Delete(u8),
    Merge(u8, u8),
    Flush,
}

fn key_of(k: u8) -> Vec<u8> {
    // Two prefix families ("a:" / "b:") so prefix scans have something to filter.
    let family = if k.is_multiple_of(2) { "a:" } else { "b:" };
    format!("{family}{k}").into_bytes()
}

fn val_of(v: u8) -> Bytes {
    Bytes::from(vec![v; 1 + (v as usize % 3)])
}

fn memop_strategy() -> impl Strategy<Value = MemOp> {
    prop_oneof![
        (0u8..10, any::<u8>()).prop_map(|(k, v)| MemOp::Put(k, v)),
        (0u8..10).prop_map(MemOp::Delete),
        (0u8..10, any::<u8>()).prop_map(|(k, v)| MemOp::Merge(k, v)),
        Just(MemOp::Flush),
    ]
}

/// Reference model mirroring `MemTable`'s exact semantics. Valid only while no
/// eviction occurs — the test stays far under the default capacity bounds.
#[derive(Default)]
struct MemModel {
    dirty: BTreeMap<Vec<u8>, Option<Bytes>>,
    clean: BTreeMap<Vec<u8>, Bytes>,
}

impl MemModel {
    fn put(&mut self, k: Vec<u8>, v: Bytes) {
        self.clean.remove(&k);
        self.dirty.insert(k, Some(v));
    }
    fn delete(&mut self, k: Vec<u8>) {
        self.clean.remove(&k);
        self.dirty.insert(k, None);
    }
    fn merge(&mut self, k: Vec<u8>, v: Bytes) {
        if self.dirty.contains_key(&k) || self.clean.contains_key(&k) {
            return;
        }
        self.clean.insert(k, v);
    }
    fn flush(&mut self) -> BTreeMap<Vec<u8>, Option<Bytes>> {
        let drained = std::mem::take(&mut self.dirty);
        for (k, v) in &drained {
            if let Some(v) = v {
                self.clean.insert(k.clone(), v.clone());
            }
        }
        drained
    }
    fn get(&self, k: &[u8]) -> Option<Option<Bytes>> {
        if let Some(v) = self.dirty.get(k) {
            return Some(v.clone());
        }
        self.clean.get(k).map(|v| Some(v.clone()))
    }
    fn keys_with_prefix(&self, prefix: &[u8]) -> BTreeSet<Vec<u8>> {
        let mut out = BTreeSet::new();
        for (k, v) in &self.dirty {
            if k.starts_with(prefix) && v.is_some() {
                out.insert(k.clone());
            }
        }
        for k in self.clean.keys() {
            if k.starts_with(prefix) && !self.dirty.contains_key(k) {
                out.insert(k.clone());
            }
        }
        out
    }
}

proptest! {
    /// Drive `MemTable` and the reference model in lock-step; after every op
    /// they must agree on `get`, `keys_with_prefix`, and flushed contents.
    #[test]
    fn memtable_matches_model(ops in prop::collection::vec(memop_strategy(), 0..120)) {
        let mut mt = MemTable::new();
        let mut model = MemModel::default();

        for op in ops {
            match op {
                MemOp::Put(k, v) => {
                    mt.put(key_of(k), val_of(v));
                    model.put(key_of(k), val_of(v));
                }
                MemOp::Delete(k) => {
                    mt.delete(key_of(k));
                    model.delete(key_of(k));
                }
                MemOp::Merge(k, v) => {
                    mt.merge(key_of(k), val_of(v));
                    model.merge(key_of(k), val_of(v));
                }
                MemOp::Flush => {
                    let mut got = mt.flush();
                    got.sort_by(|a, b| a.0.cmp(&b.0));
                    let expected: Vec<(Vec<u8>, Option<Bytes>)> =
                        model.flush().into_iter().collect();
                    prop_assert_eq!(got, expected);
                }
            }

            // `get` agrees for every key in the universe.
            for k in 0u8..10 {
                let key = key_of(k);
                prop_assert_eq!(mt.get(&key), model.get(&key), "get mismatch for key {}", k);
            }

            // Prefix scans agree (order-independent).
            for prefix in [b"a:".as_slice(), b"b:".as_slice()] {
                let mut got = mt.keys_with_prefix(prefix);
                got.sort();
                let expected: Vec<Vec<u8>> =
                    model.keys_with_prefix(prefix).into_iter().collect();
                prop_assert_eq!(got, expected);
            }
        }
    }

    /// Read-your-own-writes: a `put` is always immediately visible, regardless
    /// of preceding history, because dirty entries are never evicted.
    #[test]
    fn memtable_read_your_writes(
        preamble in prop::collection::vec(memop_strategy(), 0..40),
        k in 0u8..10,
        v in any::<u8>(),
    ) {
        let mut mt = MemTable::new();
        for op in preamble {
            match op {
                MemOp::Put(k, v) => mt.put(key_of(k), val_of(v)),
                MemOp::Delete(k) => mt.delete(key_of(k)),
                MemOp::Merge(k, v) => mt.merge(key_of(k), val_of(v)),
                MemOp::Flush => { mt.flush(); }
            }
        }
        mt.put(key_of(k), val_of(v));
        prop_assert_eq!(mt.get(&key_of(k)), Some(Some(val_of(v))));

        // A second flush right after a flush is always empty.
        mt.flush();
        prop_assert!(mt.flush().is_empty());
    }
}

// ══════════════════════════════════════════════════════════════════
// TimerService: model-based fuzzing
// ══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
enum TimerOp {
    Register(u64, String),
    Drain(u64),
}

fn timerop_strategy() -> impl Strategy<Value = TimerOp> {
    prop_oneof![
        (0u64..1000, "[a-c]").prop_map(|(ts, k)| TimerOp::Register(ts, k)),
        (0u64..1000).prop_map(TimerOp::Drain),
    ]
}

proptest! {
    /// `TimerService` vs a `BTreeSet` model: drains are watermark-bounded,
    /// ascending, and exactly the set of timers at or below the watermark.
    #[test]
    fn timer_service_matches_model(ops in prop::collection::vec(timerop_strategy(), 0..80)) {
        let mut ts = TimerService::new();
        let mut model: BTreeSet<(u64, String)> = BTreeSet::new();

        for op in ops {
            match op {
                TimerOp::Register(t, k) => {
                    ts.register(t, k.clone());
                    model.insert((t, k));
                }
                TimerOp::Drain(wm) => {
                    let fired = ts.drain_fired(wm);

                    // Expected: model entries <= wm, in sorted order.
                    let expected: Vec<(u64, String)> =
                        model.iter().filter(|(t, _)| *t <= wm).cloned().collect();
                    prop_assert_eq!(&fired, &expected);

                    // Output is strictly ascending and all <= watermark.
                    for w in fired.windows(2) {
                        prop_assert!(w[0] < w[1], "drain output not ascending");
                    }
                    for (t, _) in &fired {
                        prop_assert!(*t <= wm);
                    }

                    // Remove the fired entries from the model.
                    for e in &expected {
                        model.remove(e);
                    }
                    // Nothing at-or-below the watermark may remain.
                    prop_assert!(model.iter().all(|(t, _)| *t > wm));
                }
            }

            prop_assert_eq!(ts.is_empty(), model.is_empty());
        }
    }

    /// Across two monotonically increasing watermarks, no timer fires twice and
    /// every fired timer respects its watermark bound.
    #[test]
    fn timer_no_double_fire(
        timers in prop::collection::vec((0u64..1000, "[a-e]"), 0..40),
        wm1 in 0u64..500,
        delta in 0u64..500,
    ) {
        let wm2 = wm1 + delta;
        let mut ts = TimerService::new();
        for (t, k) in &timers {
            ts.register(*t, k.clone());
        }

        let first = ts.drain_fired(wm1);
        let second = ts.drain_fired(wm2);

        let mut seen = BTreeSet::new();
        for e in first.iter().chain(second.iter()) {
            prop_assert!(seen.insert(e.clone()), "timer {:?} fired twice", e);
        }
        for e in &first {
            prop_assert!(e.0 <= wm1);
        }
        for e in &second {
            prop_assert!(e.0 <= wm2 && e.0 > wm1);
        }
    }
}

// ══════════════════════════════════════════════════════════════════
// PrefixedBackend: key-isolation fuzzing
// ══════════════════════════════════════════════════════════════════

/// A trivial in-memory `StateBackend` for fuzzing the prefix wrapper without
/// touching the filesystem.
#[derive(Default)]
struct MemBackend {
    map: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl StateBackend for MemBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.map
            .lock()
            .unwrap()
            .insert(key.to_vec(), Bytes::copy_from_slice(value));
        Ok(())
    }
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
    async fn checkpoint(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Two operators sharing one backend never observe each other's writes, even
/// when they use the same user key. Also verifies the raw `{prefix}/{key}`
/// layout and that deletes stay scoped. Inputs are fuzzed via a manually-driven
/// proptest runner so the async backend can be awaited per case.
#[tokio::test]
async fn prefixed_backend_isolation_fuzz() {
    let strategy = (
        "[a-z]{1,8}",
        "[a-z]{1,8}",
        prop::collection::vec(any::<u8>(), 0..16),
        prop::collection::vec(any::<u8>(), 0..16),
        prop::collection::vec(any::<u8>(), 0..16),
    )
        .prop_filter("distinct prefixes", |(a, b, ..)| a != b);

    let mut runner = TestRunner::deterministic();

    for _ in 0..256 {
        let (pa, pb, key, va, vb) = strategy.new_tree(&mut runner).unwrap().current();

        let shared = std::sync::Arc::new(MemBackend::default());
        let op_a = PrefixedBackend::new(
            pa.clone(),
            Box::new(shared.clone()) as Box<dyn StateBackend>,
        )
        .unwrap();
        let op_b = PrefixedBackend::new(
            pb.clone(),
            Box::new(shared.clone()) as Box<dyn StateBackend>,
        )
        .unwrap();

        op_a.put(&key, &va).await.unwrap();
        op_b.put(&key, &vb).await.unwrap();

        // Each operator reads back exactly its own value.
        assert_eq!(
            op_a.get(&key).await.unwrap(),
            Some(Bytes::copy_from_slice(&va))
        );
        assert_eq!(
            op_b.get(&key).await.unwrap(),
            Some(Bytes::copy_from_slice(&vb))
        );

        // The raw backend stores both under distinct `{prefix}/{key}` entries.
        let raw_a = [pa.as_bytes(), b"/", &key].concat();
        let raw_b = [pb.as_bytes(), b"/", &key].concat();
        assert_eq!(
            shared.get(&raw_a).await.unwrap(),
            Some(Bytes::copy_from_slice(&va))
        );
        assert_eq!(
            shared.get(&raw_b).await.unwrap(),
            Some(Bytes::copy_from_slice(&vb))
        );

        // Deleting from A leaves B untouched.
        op_a.delete(&key).await.unwrap();
        assert_eq!(op_a.get(&key).await.unwrap(), None);
        assert_eq!(
            op_b.get(&key).await.unwrap(),
            Some(Bytes::copy_from_slice(&vb))
        );
    }
}

/// Prefix validation: any `/` in the operator name is rejected; clean names
/// are accepted.
#[rstest]
#[case::simple("operator", true)]
#[case::underscores("my_op_1", true)]
#[case::empty("", true)]
#[case::leading_slash("/op", false)]
#[case::embedded_slash("a/b", false)]
#[case::trailing_slash("op/", false)]
fn prefix_validation(#[case] name: &str, #[case] ok: bool) {
    let backend = Box::new(MemBackend::default());
    let result = PrefixedBackend::new(name, backend);
    assert_eq!(result.is_ok(), ok, "prefix {name:?} acceptance mismatch");
}
