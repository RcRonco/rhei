#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]
//! Property / fuzz tests for [`RheiBuffer`] over a **multi-column** schema
//! (`{ id: Int64, name: Utf8 }`), complementing the single-column coverage in
//! `buffer_properties.rs`.
//!
//! These guard the zero-copy selection-vector machinery (`with_mask`,
//! `and_mask`, `compact`, `should_compact`), structural cloning, and keyed
//! partitioning — the invariants every operator builds on.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use proptest::prelude::*;
use rstest::rstest;

use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema};

// ── A two-column schema `{ id: Int64, name: Utf8 }` ─────────────────

#[derive(Debug, Clone, PartialEq)]
struct Row {
    id: i64,
    name: String,
}

struct RowBuilder {
    id: arrow_array::builder::Int64Builder,
    name: arrow_array::builder::StringBuilder,
}

struct RowView<'a> {
    id: i64,
    name: &'a str,
}

#[allow(dead_code)]
struct RowCols<'a> {
    id: &'a Int64Array,
    name: &'a StringArray,
}

impl RheiBuilder for RowBuilder {
    type Item = Row;
    fn append(&mut self, item: Row) {
        self.id.append_value(item.id);
        self.name.append_value(&item.name);
    }
    fn append_null(&mut self) {
        self.id.append_null();
        self.name.append_null();
    }
    fn len(&self) -> usize {
        self.id.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Row::arrow_schema(),
            vec![Arc::new(self.id.finish()), Arc::new(self.name.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for Row {
    type Builder = RowBuilder;
    type View<'a> = RowView<'a>;
    type Columns<'a> = RowCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        RowBuilder {
            id: arrow_array::builder::Int64Builder::with_capacity(capacity),
            name: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        RowView {
            id: batch.column(0).as_primitive::<Int64Type>().value(index),
            name: batch.column(1).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        RowCols {
            id: batch.column(0).as_primitive::<Int64Type>(),
            name: batch.column(1).as_string::<i32>(),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn buffer(rows: &[Row]) -> RheiBuffer<Row> {
    let mut b = Row::builder(rows.len());
    for r in rows {
        b.append(r.clone());
    }
    RheiBuffer::from_builder(b)
}

fn logical(buf: &RheiBuffer<Row>) -> Vec<Row> {
    buf.iter()
        .map(|v| Row {
            id: v.id,
            name: v.name.to_string(),
        })
        .collect()
}

/// std-hash a key the same way `partition_by_key` does internally.
fn route(id: i64, n: usize) -> usize {
    let mut h = std::hash::DefaultHasher::new();
    id.hash(&mut h);
    (h.finish() as usize) % n
}

/// Rows plus a same-length boolean mask.
fn rows_and_mask() -> impl Strategy<Value = (Vec<Row>, Vec<bool>)> {
    prop::collection::vec((any::<i64>(), ".{0,6}"), 0..48).prop_flat_map(|pairs| {
        let rows: Vec<Row> = pairs
            .into_iter()
            .map(|(id, name)| Row { id, name })
            .collect();
        let len = rows.len();
        (Just(rows), prop::collection::vec(any::<bool>(), len..=len))
    })
}

// ── Properties ──────────────────────────────────────────────────────

proptest! {
    /// `compact` materializes exactly the masked-in rows (== manual filter),
    /// and is fully materialized afterward (`len == physical_len`).
    #[test]
    fn compact_equals_manual_filter((rows, mask) in rows_and_mask()) {
        let masked = buffer(&rows).with_mask(BooleanArray::from(mask.clone()));
        let expected: Vec<Row> = rows
            .iter()
            .zip(&mask)
            .filter(|(_, keep)| **keep)
            .map(|(r, _)| r.clone())
            .collect();

        let compacted = masked.compact();
        prop_assert_eq!(logical(&compacted), expected);
        prop_assert_eq!(compacted.len(), compacted.physical_len());
    }

    /// `compact` is idempotent: compacting twice equals compacting once.
    #[test]
    fn compact_is_idempotent((rows, mask) in rows_and_mask()) {
        let masked = buffer(&rows).with_mask(BooleanArray::from(mask));
        let once = masked.compact();
        let twice = once.compact();
        prop_assert_eq!(logical(&once), logical(&twice));
        prop_assert_eq!(once.physical_len(), twice.physical_len());
    }

    /// `should_compact` exactly matches its documented definition.
    #[test]
    fn should_compact_matches_definition((rows, mask) in rows_and_mask()) {
        let buf = buffer(&rows).with_mask(BooleanArray::from(mask.clone()));
        let true_count = mask.iter().filter(|b| **b).count();
        let expected = true_count < rows.len() / 2;
        prop_assert_eq!(buf.should_compact(), expected);
    }

    /// `and_mask` is commutative on the resulting logical sequence: applying
    /// masks in either order yields the same surviving rows.
    #[test]
    fn and_mask_is_commutative(
        (rows, mask_a) in rows_and_mask(),
        seed in any::<u64>(),
    ) {
        let mask_b: Vec<bool> =
            (0..rows.len()).map(|i| ((seed >> (i % 64)) & 1) == 1).collect();

        let ab = buffer(&rows)
            .with_mask(BooleanArray::from(mask_a.clone()))
            .and_mask(BooleanArray::from(mask_b.clone()));
        let ba = buffer(&rows)
            .with_mask(BooleanArray::from(mask_b))
            .and_mask(BooleanArray::from(mask_a));

        prop_assert_eq!(logical(&ab), logical(&ba));
    }

    /// `clone` preserves the full logical view (values, order, and mask).
    #[test]
    fn clone_preserves_logical_view((rows, mask) in rows_and_mask()) {
        let buf = buffer(&rows).with_mask(BooleanArray::from(mask));
        let cloned = buf.clone();
        prop_assert_eq!(logical(&buf), logical(&cloned));
        prop_assert_eq!(buf.len(), cloned.len());
    }
}

// ── Keyed partitioning across a matrix of partition counts ──────────

/// For each partition count, partitioning must (a) conserve every row as a
/// multiset, (b) be deterministic, and (c) route every row to the partition
/// its key hashes to. Driven by an `rstest` matrix over partition counts, with
/// `proptest` fuzzing the row data inside each case.
#[rstest]
fn partition_by_key_invariants(#[values(1, 2, 3, 5, 8, 13)] num_partitions: usize) {
    proptest!(|(pairs in prop::collection::vec((any::<i64>(), ".{0,6}"), 0..48))| {
        let rows: Vec<Row> = pairs
            .into_iter()
            .map(|(id, name)| Row { id, name })
            .collect();

        let parts = buffer(&rows).partition_by_key(|v| v.id, num_partitions);

        // (a) exactly `num_partitions` partitions.
        prop_assert_eq!(parts.len(), num_partitions);

        // (b) conservation: multiset of ids across partitions == input.
        let mut seen: Vec<i64> = parts.iter().flat_map(logical).map(|r| r.id).collect();
        let mut expected: Vec<i64> = rows.iter().map(|r| r.id).collect();
        seen.sort_unstable();
        expected.sort_unstable();
        prop_assert_eq!(seen, expected);

        // (c) routing: every row in partition `p` hashes to `p`.
        for (p, part) in parts.iter().enumerate() {
            for r in logical(part) {
                prop_assert_eq!(route(r.id, num_partitions), p);
            }
        }

        // (d) determinism: a second partitioning is byte-for-byte identical.
        let again = buffer(&rows).partition_by_key(|v| v.id, num_partitions);
        let first_ids: Vec<Vec<i64>> =
            parts.iter().map(|p| logical(p).into_iter().map(|r| r.id).collect()).collect();
        let again_ids: Vec<Vec<i64>> =
            again.iter().map(|p| logical(p).into_iter().map(|r| r.id).collect()).collect();
        prop_assert_eq!(first_ids, again_ids);
    });
}
