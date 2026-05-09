//! Vectorized aggregator trait for batch window operators.
//!
//! Unlike the row-based [`crate::operators::aggregator::Aggregator`] which
//! folds one element at a time, `ArrowAggregator` can accumulate entire
//! `RecordBatch` slices in one call, enabling SIMD-friendly operations.

use arrow_array::RecordBatch;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Vectorized aggregator operating on Arrow `RecordBatch` data.
///
/// Implementations can use column-level arithmetic (e.g., `arrow::compute::sum`)
/// for maximum throughput when processing windowed batches.
pub trait ArrowAggregator: Send + Sync {
    /// Running accumulator state, stored between batches.
    type Accumulator: Serialize + DeserializeOwned + Default + Send + Sync;
    /// Final output value produced when a window closes.
    type Output: Send;

    /// Accumulate all rows in the batch into the accumulator.
    fn accumulate_batch(&self, acc: &mut Self::Accumulator, batch: &RecordBatch);

    /// Produce the final aggregation result.
    fn finish(&self, acc: &Self::Accumulator) -> Self::Output;

    /// Reset the accumulator for reuse (default: replace with default).
    fn reset(&self, acc: &mut Self::Accumulator) {
        *acc = Self::Accumulator::default();
    }
}

/// Counts all rows in a batch.
#[derive(Debug, Clone)]
pub struct CountAgg;

impl ArrowAggregator for CountAgg {
    type Accumulator = u64;
    type Output = u64;

    fn accumulate_batch(&self, acc: &mut u64, batch: &RecordBatch) {
        *acc += batch.num_rows() as u64;
    }

    fn finish(&self, acc: &u64) -> u64 {
        *acc
    }
}

/// Sums a named numeric column across all rows.
#[derive(Debug, Clone)]
pub struct SumAgg {
    column_name: String,
}

impl SumAgg {
    /// Creates a sum aggregator for the given column name.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
        }
    }
}

impl ArrowAggregator for SumAgg {
    type Accumulator = f64;
    type Output = f64;

    fn accumulate_batch(&self, acc: &mut f64, batch: &RecordBatch) {
        let Ok(col_idx) = batch.schema().index_of(&self.column_name) else {
            return;
        };
        let col = batch.column(col_idx);

        if col
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .is_some()
        {
            *acc += arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Float64Type>(
                col.as_ref(),
            )
            .iter()
            .flatten()
            .sum::<f64>();
        } else if col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .is_some()
        {
            let sum: i64 =
                arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Int64Type>(
                    col.as_ref(),
                )
                .iter()
                .flatten()
                .sum();
            *acc += sum as f64;
        }
    }

    fn finish(&self, acc: &f64) -> f64 {
        *acc
    }
}

/// Computes the average of a named numeric column.
#[derive(Debug, Clone)]
pub struct AvgAgg {
    column_name: String,
}

impl AvgAgg {
    /// Creates an average aggregator for the given column name.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
        }
    }
}

impl ArrowAggregator for AvgAgg {
    type Accumulator = (f64, u64);
    type Output = f64;

    fn accumulate_batch(&self, acc: &mut (f64, u64), batch: &RecordBatch) {
        let Ok(col_idx) = batch.schema().index_of(&self.column_name) else {
            return;
        };
        let col = batch.column(col_idx);

        if col
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .is_some()
        {
            for val in arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Float64Type>(
                col.as_ref(),
            )
            .iter()
            .flatten()
            {
                acc.0 += val;
                acc.1 += 1;
            }
        } else if col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .is_some()
        {
            for val in arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Int64Type>(
                col.as_ref(),
            )
            .iter()
            .flatten()
            {
                acc.0 += val as f64;
                acc.1 += 1;
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finish(&self, acc: &(f64, u64)) -> f64 {
        if acc.1 == 0 {
            0.0
        } else {
            acc.0 / acc.1 as f64
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        use arrow_array::Float64Array;
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let values = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    #[test]
    fn count_agg_counts_rows() {
        let agg = CountAgg;
        let mut acc = 0u64;
        agg.accumulate_batch(&mut acc, &make_batch());
        assert_eq!(agg.finish(&acc), 5);
    }

    #[test]
    fn sum_agg_sums_column() {
        let agg = SumAgg::new("value");
        let mut acc = 0.0;
        agg.accumulate_batch(&mut acc, &make_batch());
        assert!((agg.finish(&acc) - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn avg_agg_averages_column() {
        let agg = AvgAgg::new("value");
        let mut acc = (0.0, 0);
        agg.accumulate_batch(&mut acc, &make_batch());
        assert!((agg.finish(&acc) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn count_agg_accumulates_multiple_batches() {
        let agg = CountAgg;
        let mut acc = 0u64;
        agg.accumulate_batch(&mut acc, &make_batch());
        agg.accumulate_batch(&mut acc, &make_batch());
        assert_eq!(agg.finish(&acc), 10);
    }
}
