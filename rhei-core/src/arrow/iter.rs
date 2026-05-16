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
