//! Aggregator trait and built-in aggregators for windowed computations.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// A pluggable aggregation strategy for windowed operators.
///
/// Implementations define how to accumulate input elements into a running state
/// and how to produce a final output value from that state.
pub trait Aggregator: Send + Sync {
    /// The element type being aggregated.
    type Input;
    /// The running accumulator state, stored in operator state between events.
    type Accumulator: Serialize + DeserializeOwned + Default + Send;
    /// The output value produced when a window closes.
    type Output;

    /// Folds a single input element into the accumulator.
    fn accumulate(&self, acc: &mut Self::Accumulator, input: &Self::Input);

    /// Produces the final output from the accumulated state.
    fn finish(&self, acc: &Self::Accumulator) -> Self::Output;
}

/// Counts the number of elements in a window.
#[derive(Clone)]
pub struct Count<T>(PhantomData<T>);

impl<T> fmt::Debug for Count<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Count").finish()
    }
}

impl<T> Count<T> {
    /// Creates a new `Count` aggregator.
    pub fn new() -> Self {
        Count(PhantomData)
    }
}

impl<T> Default for Count<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync> Aggregator for Count<T> {
    type Input = T;
    type Accumulator = u64;
    type Output = u64;

    fn accumulate(&self, acc: &mut u64, _input: &T) {
        *acc += 1;
    }

    fn finish(&self, acc: &u64) -> u64 {
        *acc
    }
}

/// Sums a numeric field extracted from each element.
pub struct Sum<F, T> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<F: Clone, T> Clone for Sum<F, T> {
    fn clone(&self) -> Self {
        Self {
            extractor: self.extractor.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<F, T> fmt::Debug for Sum<F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sum").finish_non_exhaustive()
    }
}

impl<F, T> Sum<F, T>
where
    F: Fn(&T) -> f64,
{
    /// Creates a new `Sum` aggregator with the given field extractor.
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<F, T> Aggregator for Sum<F, T>
where
    F: Fn(&T) -> f64 + Send + Sync,
    T: Send + Sync,
{
    type Input = T;
    type Accumulator = f64;
    type Output = f64;

    fn accumulate(&self, acc: &mut f64, input: &T) {
        *acc += (self.extractor)(input);
    }

    fn finish(&self, acc: &f64) -> f64 {
        *acc
    }
}

/// Computes the running average of a numeric field.
pub struct Avg<F, T> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<F: Clone, T> Clone for Avg<F, T> {
    fn clone(&self) -> Self {
        Self {
            extractor: self.extractor.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<F, T> fmt::Debug for Avg<F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Avg").finish_non_exhaustive()
    }
}

impl<F, T> Avg<F, T>
where
    F: Fn(&T) -> f64,
{
    /// Creates a new `Avg` aggregator with the given field extractor.
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<F, T> Aggregator for Avg<F, T>
where
    F: Fn(&T) -> f64 + Send + Sync,
    T: Send + Sync,
{
    type Input = T;
    type Accumulator = (f64, u64);
    type Output = f64;

    fn accumulate(&self, acc: &mut (f64, u64), input: &T) {
        acc.0 += (self.extractor)(input);
        acc.1 += 1;
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
