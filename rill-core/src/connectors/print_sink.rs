use std::fmt::Display;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::traits::Sink;

/// A sink that writes each element to stdout.
pub struct PrintSink<T> {
    prefix: Option<String>,
    _marker: PhantomData<T>,
}

impl<T> PrintSink<T> {
    pub fn new() -> Self {
        Self {
            prefix: None,
            _marker: PhantomData,
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }
}

impl<T> Default for PrintSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Send + Sync + Display + 'static> Sink for PrintSink<T> {
    type Input = T;

    async fn write(&mut self, input: T) -> anyhow::Result<()> {
        match &self.prefix {
            Some(pfx) => println!("{pfx}: {input}"),
            None => println!("{input}"),
        }
        Ok(())
    }
}
