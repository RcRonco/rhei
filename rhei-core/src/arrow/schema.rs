use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use super::RheiBuilder;

/// Trait connecting a user-defined struct to its Arrow representation.
///
/// Typically derived via `#[derive(RheiSchema)]`. Provides access to:
/// - The Arrow schema (field names and types)
/// - A columnar builder for appending owned values
/// - A zero-copy view for reading individual rows
/// - Typed column accessors for vectorized processing
pub trait RheiSchema: Sized + Send + 'static {
    /// The columnar builder type that accumulates rows into Arrow arrays.
    type Builder: RheiBuilder<Item = Self>;

    /// Zero-copy view of a single row, borrowing from the underlying Arrow buffers.
    /// Primitives are copied (cheap), strings and binary data are borrowed.
    type View<'a>;

    /// Typed column accessors providing direct references to Arrow arrays.
    type Columns<'a>;

    /// Returns the Arrow schema describing this struct's columnar layout.
    fn arrow_schema() -> Arc<Schema>;

    /// Creates a new builder with the given initial capacity (number of rows).
    fn builder(capacity: usize) -> Self::Builder;

    /// Constructs a zero-copy view of the row at `index` within `batch`.
    ///
    /// # Panics
    /// Panics if `index >= batch.num_rows()`.
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_>;

    /// Returns typed column accessors for the given batch.
    fn columns(batch: &RecordBatch) -> Self::Columns<'_>;
}
