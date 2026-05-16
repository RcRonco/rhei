//! Batch map and flat-map operators.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};

/// Transforms each input row (via View) into exactly one output row.
///
/// The closure receives a zero-copy view of each input row and returns an
/// owned output value that is appended to the output builder.
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
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> O,
{
    /// Creates a new batch map operator with the given transformation.
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
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        let len = input.len();
        if len == 0 {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(len);
        for view in &input {
            builder.append((self.f)(view));
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

/// Transforms each input row (via View) into zero or more output rows.
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
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> Vec<O>,
{
    /// Creates a new batch flat-map operator with the given transformation.
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
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> Vec<O> + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        let len = input.len();
        if len == 0 {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(len * 2);
        for view in &input {
            for item in (self.f)(view) {
                builder.append(item);
            }
        }

        if builder.is_empty() {
            return Ok(BufferOutput::None);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}
