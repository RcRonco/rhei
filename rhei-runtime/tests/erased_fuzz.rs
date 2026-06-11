#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Fuzz / property tests for [`ErasedBuffer`] — the type-erased Arrow batch
//! that every record takes across a Timely exchange.
//!
//! The in-crate unit tests already fuzz single-column partitioning & IPC; these
//! integration tests focus on the **type-erasure boundary** over richer,
//! multi-column schemas (built with `#[derive(RheiSchema)]`):
//!
//! * `from_typed` → `downcast` is a lossless identity round-trip.
//! * Arrow-IPC serde (`serde_json`) preserves rows, schema id, and mask.
//! * Masked buffers survive erasure and `compact` to the manually-filtered batch.
//! * `downcast` to the wrong schema is rejected (no silent type confusion).

use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use proptest::prelude::*;
use rstest::rstest;

use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema as RheiSchemaTrait};
use rhei_macros::RheiSchema;
use rhei_runtime::erased_buffer::ErasedBuffer;

// ── Two distinct multi-column schemas ───────────────────────────────

#[derive(Debug, Clone, RheiSchema)]
struct EventA {
    id: i64,
    name: String,
    score: f64,
}

#[derive(Debug, Clone, RheiSchema)]
struct EventB {
    a: u64,
    b: i64,
}

// ── Strategies ──────────────────────────────────────────────────────

fn event_a_rows() -> impl Strategy<Value = Vec<EventA>> {
    prop::collection::vec(
        // Finite scores so Arrow batch equality (bitwise) is well-defined.
        (any::<i64>(), ".{0,8}", -1.0e9f64..1.0e9f64),
        0..40,
    )
    .prop_map(|v| {
        v.into_iter()
            .map(|(id, name, score)| EventA { id, name, score })
            .collect()
    })
}

fn build_a(rows: &[EventA]) -> RheiBuffer<EventA> {
    let mut b = EventA::builder(rows.len());
    for r in rows {
        b.append(r.clone());
    }
    RheiBuffer::from_builder(b)
}

// ── Properties ──────────────────────────────────────────────────────

proptest! {
    /// `from_typed` → `downcast` reconstructs the identical Arrow batch.
    #[test]
    fn typed_erase_downcast_roundtrip(rows in event_a_rows()) {
        let buf = build_a(&rows);
        let original = buf.as_record_batch().clone();

        let erased = ErasedBuffer::from_typed(buf);
        prop_assert_eq!(erased.num_rows(), rows.len());

        let restored: RheiBuffer<EventA> = erased.downcast().unwrap();
        prop_assert_eq!(restored.as_record_batch(), &original);
    }

    /// Arrow-IPC serde (the Timely exchange path) preserves rows, schema id,
    /// and the underlying batch.
    #[test]
    fn ipc_serde_roundtrip_preserves_batch(rows in event_a_rows()) {
        let original = ErasedBuffer::from_typed(build_a(&rows));

        let json = serde_json::to_string(&original).unwrap();
        let restored: ErasedBuffer = serde_json::from_str(&json).unwrap();

        prop_assert_eq!(restored.num_rows(), original.num_rows());
        prop_assert_eq!(restored.schema_id(), original.schema_id());
        prop_assert_eq!(restored.as_record_batch(), original.as_record_batch());
    }

    /// A masked buffer survives erasure: after `downcast` + `compact` it equals
    /// the manually mask-filtered batch, and the schema id is unchanged.
    #[test]
    fn masked_buffer_survives_erasure(
        rows in event_a_rows(),
        seed in any::<u64>(),
    ) {
        let mask_vec: Vec<bool> =
            (0..rows.len()).map(|i| ((seed >> (i % 64)) & 1) == 1).collect();
        let mask = BooleanArray::from(mask_vec);

        let raw_batch = build_a(&rows).as_record_batch().clone();
        let expected: RecordBatch = filter_record_batch(&raw_batch, &mask).unwrap();

        let buf = build_a(&rows).with_mask(mask);
        let erased = ErasedBuffer::from_typed(buf);
        let restored: RheiBuffer<EventA> = erased.downcast().unwrap();

        let compacted = restored.compact();
        prop_assert_eq!(compacted.as_record_batch(), &expected);
    }
}

// ── Type-confusion guard (rstest matrix) ────────────────────────────

/// Erasing as one schema and downcasting to another must fail. The matrix
/// covers both directions plus the matching (success) baselines.
#[rstest]
#[case::a_as_a("A", "A", true)]
#[case::b_as_b("B", "B", true)]
#[case::a_as_b("A", "B", false)]
#[case::b_as_a("B", "A", false)]
fn downcast_respects_schema(#[case] erase: &str, #[case] target: &str, #[case] ok: bool) {
    // Build an erased buffer of the `erase` schema.
    let erased = if erase == "A" {
        let mut b = EventA::builder(1);
        b.append(EventA {
            id: 1,
            name: "x".into(),
            score: 1.0,
        });
        ErasedBuffer::from_typed(RheiBuffer::<EventA>::from_builder(b))
    } else {
        let mut b = EventB::builder(1);
        b.append(EventB { a: 1, b: 2 });
        ErasedBuffer::from_typed(RheiBuffer::<EventB>::from_builder(b))
    };

    let result_ok = if target == "A" {
        erased.downcast::<EventA>().is_ok()
    } else {
        erased.downcast::<EventB>().is_ok()
    };
    assert_eq!(result_ok, ok, "erase {erase} → downcast {target}");
}
