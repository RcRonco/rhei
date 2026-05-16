use arrow_array::RecordBatch;

/// Trait for columnar builders that accumulate rows into Arrow arrays.
///
/// A builder collects individual owned values (one per row) and seals them
/// into a `RecordBatch` when `finish()` is called.
pub trait RheiBuilder: Send {
    /// The owned item type that this builder accepts.
    type Item;

    /// Append a single row to the builder.
    fn append(&mut self, item: Self::Item);

    /// Append a null row (all fields null). Used when the schema requires
    /// representing absent rows in a buffer.
    fn append_null(&mut self);

    /// Returns the number of rows appended so far.
    fn len(&self) -> usize;

    /// Returns true if no rows have been appended.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Seal the builder and produce an Arrow `RecordBatch`.
    /// Consumes the builder.
    fn finish(self) -> RecordBatch;
}
