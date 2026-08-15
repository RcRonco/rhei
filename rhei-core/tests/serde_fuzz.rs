#![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
//! Fuzz / property tests for the serialization & encoding surface of
//! `rhei-core`.
//!
//! These combine [`rstest`] table-driven cases (for the deterministic,
//! shape-defining edge cases) with [`proptest`] randomized fuzzing (for the
//! "throw thousands of inputs at it" coverage). Together they guard the
//! round-trip invariants the checkpoint / state machinery depends on:
//!
//! * `event::{to,from}_{json,bincode}` — the wire formats for user events.
//! * [`JsonEncoder`] / [`BincodeEncoder`] — the `KeyedState` value codecs.
//! * [`TimerService::serialize`] / [`restore`] — event-time timer persistence.
//! * [`CheckpointManifest`] — JSON manifest round-trips & partial merges.

use std::collections::HashMap;

use proptest::prelude::*;
use rstest::rstest;
use serde::{Deserialize, Serialize};

use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::event::{from_bincode, from_json, to_bincode, to_json};
use rhei_core::operators::keyed_state::{BincodeEncoder, JsonEncoder, KeyEncoder};
use rhei_core::state::timer_service::TimerService;

// ── A representative, non-trivial user event ────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Sample {
    word: String,
    count: u64,
    signed: i64,
    tags: Vec<String>,
    maybe: Option<i64>,
    nested: Vec<(u32, String)>,
}

/// Strategy for an integer/string `Sample` that round-trips *bit-exactly*
/// through both JSON and bincode. Floats are fuzzed separately below because
/// JSON's shortest-decimal printing makes exact `f64` equality the wrong
/// invariant (see `float_*` tests).
fn sample_strategy() -> impl Strategy<Value = Sample> {
    (
        ".{0,24}",
        any::<u64>(),
        any::<i64>(),
        prop::collection::vec(".{0,12}", 0..6),
        prop::option::of(any::<i64>()),
        prop::collection::vec((any::<u32>(), ".{0,8}"), 0..4),
    )
        .prop_map(|(word, count, signed, tags, maybe, nested)| Sample {
            word,
            count,
            signed,
            tags,
            maybe,
            nested,
        })
}

// ── Event wire-format round-trips ───────────────────────────────────

proptest! {
    /// JSON encode → decode is the identity for arbitrary events.
    #[test]
    fn event_json_roundtrip(sample in sample_strategy()) {
        let bytes = to_json(&sample).unwrap();
        let restored: Sample = from_json(&bytes).unwrap();
        prop_assert_eq!(sample, restored);
    }

    /// Bincode encode → decode is the identity for arbitrary events.
    #[test]
    fn event_bincode_roundtrip(sample in sample_strategy()) {
        let bytes = to_bincode(&sample).unwrap();
        let restored: Sample = from_bincode(&bytes).unwrap();
        prop_assert_eq!(sample, restored);
    }

    /// Encoding is a pure function: same input → byte-identical output.
    #[test]
    fn event_encoding_is_deterministic(sample in sample_strategy()) {
        prop_assert_eq!(to_json(&sample).unwrap(), to_json(&sample).unwrap());
        prop_assert_eq!(to_bincode(&sample).unwrap(), to_bincode(&sample).unwrap());
    }
}

proptest! {
    /// Bincode is bit-exact for `f64`, including NaN/Inf — compare raw bits so
    /// `NaN != NaN` doesn't spuriously fail.
    #[test]
    fn float_bincode_is_bit_exact(bits in any::<u64>()) {
        let value = f64::from_bits(bits);
        let restored: f64 = from_bincode(&to_bincode(&value).unwrap()).unwrap();
        prop_assert_eq!(value.to_bits(), restored.to_bits());
    }

    /// JSON is a *lossy text* format for `f64`, so the right invariant is
    /// bounded relative error (not bit-exactness). This pins down that the
    /// event JSON path never drifts beyond a tight tolerance — the contract
    /// operators relying on JSON-encoded values can count on.
    #[test]
    fn float_json_roundtrip_within_tolerance(value in -1.0e12f64..1.0e12f64) {
        let restored: f64 = from_json(&to_json(&value).unwrap()).unwrap();
        let denom = value.abs().max(1.0);
        prop_assert!(
            (restored - value).abs() / denom < 1e-9,
            "json f64 drift too large: {value} -> {restored}"
        );
    }
}

/// Primitive event types survive both wire formats. The `#[case]`s pin down
/// the concrete monomorphizations that matter in practice.
#[rstest]
#[case::unit(0u64)]
#[case::small(7u64)]
#[case::max(u64::MAX)]
fn primitive_u64_roundtrips(#[case] value: u64) {
    let j: u64 = from_json(&to_json(&value).unwrap()).unwrap();
    let b: u64 = from_bincode(&to_bincode(&value).unwrap()).unwrap();
    assert_eq!(j, value);
    assert_eq!(b, value);
}

#[rstest]
#[case::empty("")]
#[case::ascii("hello")]
#[case::unicode("héllo · 世界 · 🦀")]
#[case::control("a\tb\nc\0d")]
fn primitive_string_roundtrips(#[case] value: &str) {
    let owned = value.to_string();
    let j: String = from_json(&to_json(&owned).unwrap()).unwrap();
    let b: String = from_bincode(&to_bincode(&owned).unwrap()).unwrap();
    assert_eq!(j, owned);
    assert_eq!(b, owned);
}

// ── KeyedState encoder round-trips ──────────────────────────────────

/// A value type exercised through both `KeyEncoder` implementations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredValue {
    sum: i64,
    items: Vec<String>,
    flag: bool,
}

fn stored_value_strategy() -> impl Strategy<Value = StoredValue> {
    (
        any::<i64>(),
        prop::collection::vec(".{0,10}", 0..5),
        any::<bool>(),
    )
        .prop_map(|(sum, items, flag)| StoredValue { sum, items, flag })
}

proptest! {
    /// `JsonEncoder`: value encode → decode is the identity.
    #[test]
    fn json_encoder_value_roundtrip(value in stored_value_strategy()) {
        let enc = JsonEncoder;
        let bytes = enc.encode_value(&value).unwrap();
        let decoded: StoredValue = enc.decode_value(&bytes).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// `BincodeEncoder`: value encode → decode is the identity.
    #[test]
    fn bincode_encoder_value_roundtrip(value in stored_value_strategy()) {
        let enc = BincodeEncoder;
        let bytes = enc.encode_value(&value).unwrap();
        let decoded: StoredValue = enc.decode_value(&bytes).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// `BincodeEncoder::encode_key` always yields valid, even-length lowercase
    /// hex — the property `KeyedState` relies on for collision-free key suffixes.
    #[test]
    fn bincode_key_is_valid_hex(key in any::<u64>()) {
        let enc = BincodeEncoder;
        let encoded = enc.encode_key(&key).unwrap();
        prop_assert!(encoded.len().is_multiple_of(2));
        prop_assert!(encoded.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        // Hex must round-trip back to the original bincode bytes.
        let raw = hex::decode(&encoded).unwrap();
        prop_assert_eq!(raw, bincode::serialize(&key).unwrap());
    }

    /// Key encoding is deterministic and injective-on-equality: equal keys
    /// encode identically.
    #[test]
    fn key_encoding_is_deterministic(a in ".{0,16}", b in ".{0,16}") {
        let json = JsonEncoder;
        let binc = BincodeEncoder;
        prop_assert_eq!(json.encode_key(&a).unwrap(), json.encode_key(&a).unwrap());
        prop_assert_eq!(binc.encode_key(&a).unwrap(), binc.encode_key(&a).unwrap());
        if a == b {
            prop_assert_eq!(json.encode_key(&a).unwrap(), json.encode_key(&b).unwrap());
            prop_assert_eq!(binc.encode_key(&a).unwrap(), binc.encode_key(&b).unwrap());
        }
    }
}

// ── TimerService serialize / restore ────────────────────────────────

proptest! {
    /// A serialized-then-restored timer service drains exactly the same timers
    /// in the same order as the original.
    #[test]
    fn timer_service_serde_roundtrip(
        timers in prop::collection::vec((any::<u64>(), ".{0,8}"), 0..32),
    ) {
        let mut original = TimerService::new();
        for (ts, key) in &timers {
            original.register(*ts, key.clone());
        }
        let bytes = original.serialize().unwrap();
        let mut restored = TimerService::restore(&bytes).unwrap();

        // Drain everything (watermark = u64::MAX) from both and compare.
        let from_orig = original.drain_fired(u64::MAX);
        let from_restored = restored.drain_fired(u64::MAX);
        prop_assert_eq!(from_orig, from_restored);
    }
}

// ── CheckpointManifest JSON round-trips ─────────────────────────────

fn manifest_strategy() -> impl Strategy<Value = CheckpointManifest> {
    (
        any::<u64>(),
        any::<u64>(),
        prop::collection::vec("[a-z_]{1,12}", 0..6),
        prop::collection::hash_map("[a-z]/[0-9]", "[0-9]{1,8}", 0..8),
        prop::option::of(1usize..16),
        prop::option::of(1usize..16),
    )
        .prop_map(
            |(checkpoint_id, timestamp_ms, operators, source_offsets, n_processes, wpp)| {
                CheckpointManifest {
                    version: 1,
                    checkpoint_id,
                    timestamp_ms,
                    operators,
                    source_offsets,
                    n_processes,
                    workers_per_process: wpp,
                    max_parallelism: None,
                    total_workers: None,
                    cluster_members: Vec::new(),
                }
            },
        )
}

/// Compare two manifests field-by-field (it has no `PartialEq`).
fn manifest_eq(a: &CheckpointManifest, b: &CheckpointManifest) -> bool {
    a.version == b.version
        && a.checkpoint_id == b.checkpoint_id
        && a.timestamp_ms == b.timestamp_ms
        && a.operators == b.operators
        && a.source_offsets == b.source_offsets
        && a.n_processes == b.n_processes
        && a.workers_per_process == b.workers_per_process
}

proptest! {
    /// Manifest → JSON → manifest preserves every field.
    #[test]
    fn manifest_json_roundtrip(manifest in manifest_strategy()) {
        let json = serde_json::to_string(&manifest).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();
        prop_assert!(manifest_eq(&manifest, &restored));
    }

    /// Manifest → disk (atomic save) → load preserves every field.
    #[test]
    fn manifest_disk_roundtrip(manifest in manifest_strategy()) {
        let dir = tempfile::tempdir().unwrap();
        manifest.save(dir.path()).unwrap();
        let loaded = CheckpointManifest::load(dir.path()).expect("manifest exists");
        prop_assert!(manifest_eq(&manifest, &loaded));
    }

    /// `merge_partials` unions every partial's source offsets and takes the max
    /// checkpoint id / timestamp.
    #[test]
    fn merge_partials_unions_offsets(
        partials in prop::collection::vec(manifest_strategy(), 1..5),
    ) {
        let dir = tempfile::tempdir().unwrap();
        for (pid, p) in partials.iter().enumerate() {
            p.save_partial(dir.path(), pid).unwrap();
        }
        let merged = CheckpointManifest::merge_partials(dir.path(), partials.len())
            .expect("merge should not error")
            .expect("all partials present");

        let expected_max_ckpt = partials.iter().map(|p| p.checkpoint_id).max().unwrap();
        let expected_max_ts = partials.iter().map(|p| p.timestamp_ms).max().unwrap();
        prop_assert_eq!(merged.checkpoint_id, expected_max_ckpt);
        prop_assert_eq!(merged.timestamp_ms, expected_max_ts);

        // Every key from every partial must appear in the merged map.
        let mut all_keys: Vec<&String> =
            partials.iter().flat_map(|p| p.source_offsets.keys()).collect();
        all_keys.sort();
        all_keys.dedup();
        for k in all_keys {
            prop_assert!(merged.source_offsets.contains_key(k));
        }
    }
}

/// `merge_partials` returns `None` when any partial is missing — driven across
/// several "present vs expected" shapes.
#[rstest]
#[case::one_of_two(1, 2)]
#[case::two_of_five(2, 5)]
#[case::none_of_three(0, 3)]
fn merge_partials_incomplete_is_none(#[case] present: usize, #[case] expected: usize) {
    let dir = tempfile::tempdir().unwrap();
    let manifest = CheckpointManifest {
        version: 1,
        checkpoint_id: 1,
        timestamp_ms: 1,
        operators: vec![],
        source_offsets: HashMap::new(),
        n_processes: None,
        workers_per_process: None,
        max_parallelism: None,
        total_workers: None,
        cluster_members: Vec::new(),
    };
    for pid in 0..present {
        manifest.save_partial(dir.path(), pid).unwrap();
    }
    assert!(
        CheckpointManifest::merge_partials(dir.path(), expected)
            .expect("missing partials are not an error")
            .is_none()
    );
}
