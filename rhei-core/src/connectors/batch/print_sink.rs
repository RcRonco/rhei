//! A batch sink that prints rows to stdout via their View's Debug representation.

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::arrow::{BatchSink, RheiBuffer, RheiSchema};

/// A sink that iterates over buffer rows and prints each via `Debug`.
#[derive(Debug)]
pub struct BatchPrintSink<T: RheiSchema> {
    prefix: Option<String>,
    _marker: PhantomData<T>,
}

impl<T: RheiSchema> BatchPrintSink<T> {
    /// Creates a new `BatchPrintSink` with no prefix.
    pub fn new() -> Self {
        Self {
            prefix: None,
            _marker: PhantomData,
        }
    }

    /// Sets a prefix that is prepended to each printed line.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }
}

impl<T: RheiSchema> Default for BatchPrintSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: RheiSchema + Sync> BatchSink for BatchPrintSink<T>
where
    for<'a> T::View<'a>: Debug,
{
    type Input = T;

    async fn write_batch(&mut self, input: RheiBuffer<T>) -> anyhow::Result<()> {
        for view in &input {
            match &self.prefix {
                Some(pfx) => println!("{pfx}: {view:?}"),
                None => println!("{view:?}"),
            }
        }
        Ok(())
    }
}
