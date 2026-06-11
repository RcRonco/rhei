#![allow(clippy::unwrap_used, clippy::cast_possible_wrap)]
//! Property-based tests for `RheiBuffer` selection-vector invariants.
//!
//! These exercise the zero-copy masking machinery (`with_mask`, `and_mask`,
//! `compact`) and key partitioning (`partition_by_key`) across thousands of
//! randomized row/mask combinations, guarding invariants the operator library
//! depends on.

use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use proptest::prelude::*;

use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema};

// ── A minimal single-i64-column schema (`{ v: Int64 }`) ──────────────

struct N {
    v: i64,
}

struct NBuilder {
    v: arrow_array::builder::Int64Builder,
}

struct NView {
    v: i64,
}

#[allow(dead_code)]
struct NCols<'a> {
    v: &'a Int64Array,
}

impl RheiBuilder for NBuilder {
    type Item = N;
    fn append(&mut self, item: N) {
        self.v.append_value(item.v);
    }
    fn append_null(&mut self) {
        self.v.append_null();
    }
    fn len(&self) -> usize {
        self.v.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(N::arrow_schema(), vec![Arc::new(self.v.finish())]).unwrap()
    }
}

impl RheiSchema for N {
    type Builder = NBuilder;
    type View<'a> = NView;
    type Columns<'a> = NCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        NBuilder {
            v: arrow_array::builder::Int64Builder::with_capacity(capacity),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        NView {
            v: batch.column(0).as_primitive::<Int64Type>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        NCols {
            v: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

fn buffer(values: &[i64]) -> RheiBuffer<N> {
    let mut builder = N::builder(values.len());
    for &v in values {
        builder.append(N { v });
    }
    RheiBuffer::from_builder(builder)
}

fn logical_values(buf: &RheiBuffer<N>) -> Vec<i64> {
    buf.iter().map(|view| view.v).collect()
}

/// Generates a vec of values together with a same-length boolean mask.
fn values_and_mask() -> impl Strategy<Value = (Vec<i64>, Vec<bool>)> {
    prop::collection::vec(any::<i64>(), 0..64).prop_flat_map(|values| {
        let len = values.len();
        (
            Just(values),
            prop::collection::vec(any::<bool>(), len..=len),
        )
    })
}

proptest! {
    /// `with_mask` yields exactly the rows whose mask bit is true, in order.
    #[test]
    fn mask_selects_exactly_the_true_rows((values, mask) in values_and_mask()) {
        let buf = buffer(&values).with_mask(BooleanArray::from(mask.clone()));

        let expected: Vec<i64> = values
            .iter()
            .zip(&mask)
            .filter_map(|(v, &keep)| keep.then_some(*v))
            .collect();

        prop_assert_eq!(logical_values(&buf), expected.clone());
        prop_assert_eq!(buf.len(), expected.len());
        // Logical length can never exceed the physical row count.
        prop_assert!(buf.len() <= buf.physical_len());
    }

    /// `and_mask` composes as a logical AND of the two selection vectors.
    #[test]
    fn and_mask_is_logical_and(
        (values, mask_a) in values_and_mask(),
        seed in any::<u64>(),
    ) {
        // Derive a second, deterministic mask of the same length.
        let mask_b: Vec<bool> = (0..values.len())
            .map(|i| ((seed >> (i % 64)) & 1) == 1)
            .collect();

        let buf = buffer(&values)
            .with_mask(BooleanArray::from(mask_a.clone()))
            .and_mask(BooleanArray::from(mask_b.clone()));

        let expected: Vec<i64> = (0..values.len())
            .filter(|&i| mask_a[i] && mask_b[i])
            .map(|i| values[i])
            .collect();

        prop_assert_eq!(logical_values(&buf), expected);
    }

    /// `compact` physically removes masked rows but preserves the logical view:
    /// same values in the same order, and afterward physical_len == len.
    #[test]
    fn compact_preserves_logical_sequence((values, mask) in values_and_mask()) {
        let masked = buffer(&values).with_mask(BooleanArray::from(mask));
        let before = logical_values(&masked);

        let compacted = masked.compact();
        prop_assert_eq!(logical_values(&compacted), before.clone());
        // Fully materialized: no masked-out physical rows remain.
        prop_assert_eq!(compacted.len(), compacted.physical_len());
        prop_assert_eq!(compacted.len(), before.len());
    }

    /// Partitioning by key routes every logical row to exactly one partition:
    /// the multiset of values across all partitions equals the input.
    #[test]
    fn partition_by_key_conserves_every_row(
        values in prop::collection::vec(any::<i64>(), 0..64),
        num_partitions in 1usize..8,
    ) {
        let buf = buffer(&values);
        let partitions = buf.partition_by_key(|view| view.v, num_partitions);

        let mut seen: Vec<i64> = partitions
            .iter()
            .flat_map(logical_values)
            .collect();
        let mut expected = values.clone();
        seen.sort_unstable();
        expected.sort_unstable();

        prop_assert_eq!(seen, expected);
    }

    /// Partitioning is deterministic: identical input yields identical layout.
    #[test]
    fn partition_by_key_is_deterministic(
        values in prop::collection::vec(any::<i64>(), 0..64),
        num_partitions in 1usize..8,
    ) {
        let first: Vec<Vec<i64>> = buffer(&values)
            .partition_by_key(|view| view.v, num_partitions)
            .iter()
            .map(logical_values)
            .collect();
        let second: Vec<Vec<i64>> = buffer(&values)
            .partition_by_key(|view| view.v, num_partitions)
            .iter()
            .map(logical_values)
            .collect();
        prop_assert_eq!(first, second);
    }
}
