//! Stateless filter operator.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

/// Passes through elements that satisfy a predicate, dropping the rest.
pub struct FilterOp<F, T> {
    predicate: F,
    _phantom: PhantomData<fn(T)>,
}

impl<F, T> fmt::Debug for FilterOp<F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterOp").finish_non_exhaustive()
    }
}

impl<F, T> FilterOp<F, T>
where
    F: Fn(&T) -> bool,
{
    /// Creates a new `FilterOp` with the given predicate.
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> StreamFunction for FilterOp<F, T>
where
    F: Fn(&T) -> bool + Send + Sync,
    T: Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn process(&mut self, input: T, _ctx: &mut StateContext) -> Vec<T> {
        if (self.predicate)(&input) {
            vec![input]
        } else {
            vec![]
        }
    }
}
