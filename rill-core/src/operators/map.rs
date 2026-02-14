//! Stateless map and flat-map operators.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

/// Transforms each input element into exactly one output element.
pub struct MapOp<F, I, O> {
    f: F,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<F, I, O> fmt::Debug for MapOp<F, I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapOp").finish_non_exhaustive()
    }
}

impl<F, I, O> MapOp<F, I, O>
where
    F: Fn(I) -> O,
{
    /// Creates a new `MapOp` with the given transformation function.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> StreamFunction for MapOp<F, I, O>
where
    F: Fn(I) -> O + Send + Sync,
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn process(&mut self, input: I, _ctx: &mut StateContext) -> Vec<O> {
        vec![(self.f)(input)]
    }
}

/// Transforms each input element into zero or more output elements.
pub struct FlatMapOp<F, I, O> {
    f: F,
    _phantom: PhantomData<fn(I) -> Vec<O>>,
}

impl<F, I, O> fmt::Debug for FlatMapOp<F, I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlatMapOp").finish_non_exhaustive()
    }
}

impl<F, I, O> FlatMapOp<F, I, O>
where
    F: Fn(I) -> Vec<O>,
{
    /// Creates a new `FlatMapOp` with the given transformation function.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> StreamFunction for FlatMapOp<F, I, O>
where
    F: Fn(I) -> Vec<O> + Send + Sync,
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn process(&mut self, input: I, _ctx: &mut StateContext) -> Vec<O> {
        (self.f)(input)
    }
}
