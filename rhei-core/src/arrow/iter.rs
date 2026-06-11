use std::marker::PhantomData;

use arrow_array::{BooleanArray, RecordBatch};

use super::schema::RheiSchema;

/// Iterator over logically valid rows in a `RheiBuffer`, yielding zero-copy views.
///
/// Skips rows that are masked out (false in the selection vector).
#[derive(Debug)]
pub struct RheiIter<'a, T: RheiSchema> {
    batch: &'a RecordBatch,
    mask: Option<&'a BooleanArray>,
    current: usize,
    physical_len: usize,
    _marker: PhantomData<T>,
}

impl<'a, T: RheiSchema> RheiIter<'a, T> {
    pub(crate) fn new(batch: &'a RecordBatch, mask: Option<&'a BooleanArray>) -> Self {
        Self {
            batch,
            mask,
            current: 0,
            physical_len: batch.num_rows(),
            _marker: PhantomData,
        }
    }

    /// Returns an iterator that yields `(physical_index, View)` pairs,
    /// allowing callers to know the physical row index of each yielded view.
    pub fn enumerate_physical(self) -> RheiIterPhysical<'a, T> {
        RheiIterPhysical { inner: self }
    }

    fn is_valid(&self, index: usize) -> bool {
        match self.mask {
            None => true,
            Some(mask) => mask.value(index),
        }
    }
}

impl<'a, T: RheiSchema> Iterator for RheiIter<'a, T> {
    type Item = T::View<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current < self.physical_len {
            let idx = self.current;
            self.current += 1;
            if self.is_valid(idx) {
                return Some(T::view(self.batch, idx));
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_physical = self.physical_len - self.current;
        match self.mask {
            None => (remaining_physical, Some(remaining_physical)),
            Some(_) => (0, Some(remaining_physical)),
        }
    }
}

/// Iterator yielding `(physical_index, View)` pairs.
#[derive(Debug)]
pub struct RheiIterPhysical<'a, T: RheiSchema> {
    inner: RheiIter<'a, T>,
}

impl<'a, T: RheiSchema> Iterator for RheiIterPhysical<'a, T> {
    type Item = (usize, T::View<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.inner.current < self.inner.physical_len {
            let idx = self.inner.current;
            self.inner.current += 1;
            if self.inner.is_valid(idx) {
                return Some((idx, T::view(self.inner.batch, idx)));
            }
        }
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use crate::arrow::{RheiBuffer, RheiBuilder};
    use arrow_array::builder::ArrayBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // Single i64 column `v`, with the value also used as the logical row id.
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

    fn buffer(n: usize) -> RheiBuffer<N> {
        let mut builder = N::builder(n);
        for i in 0..n {
            builder.append(N { v: i as i64 });
        }
        RheiBuffer::from_builder(builder)
    }

    #[test]
    fn iterates_all_rows_when_unmasked() {
        let buf = buffer(4);
        let values: Vec<i64> = buf.iter().map(|view| view.v).collect();
        assert_eq!(values, vec![0, 1, 2, 3]);
    }

    #[test]
    fn iterating_empty_buffer_yields_nothing() {
        let buf = buffer(0);
        assert_eq!(buf.iter().count(), 0);
    }

    #[test]
    fn masked_iteration_skips_invalid_rows() {
        let buf = buffer(5).with_mask(BooleanArray::from(vec![
            true, false, true, false, true,
        ]));
        let values: Vec<i64> = buf.iter().map(|view| view.v).collect();
        assert_eq!(values, vec![0, 2, 4]);
    }

    #[test]
    fn enumerate_physical_reports_physical_indices() {
        // Logical rows {1, 3} survive, but their *physical* positions are 1 and 3.
        let buf = buffer(4).with_mask(BooleanArray::from(vec![false, true, false, true]));
        let pairs: Vec<(usize, i64)> =
            buf.iter().enumerate_physical().map(|(i, view)| (i, view.v)).collect();
        assert_eq!(pairs, vec![(1, 1), (3, 3)]);
    }

    #[test]
    fn size_hint_is_exact_without_mask() {
        let buf = buffer(3);
        let mut iter = buf.iter();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next();
        assert_eq!(iter.size_hint(), (2, Some(2)));
    }

    #[test]
    fn size_hint_is_bounded_with_mask() {
        // With a mask the exact count is unknown up front: lower bound 0, upper
        // bound the remaining physical rows.
        let buf = buffer(4).with_mask(BooleanArray::from(vec![true, false, true, false]));
        let iter = buf.iter();
        assert_eq!(iter.size_hint(), (0, Some(4)));
    }
}
