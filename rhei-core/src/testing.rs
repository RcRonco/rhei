//! Test harness for rhei pipelines.
//!
//! Provides in-memory sources, collecting sinks, and assertion helpers
//! so you can write pipeline tests without Kafka, files, or networking.
//!
//! # Example
//!
//! ```ignore
//! use rhei_core::testing::{TestSource, CollectSink, run_test_pipeline};
//!
//! #[tokio::test]
//! async fn test_my_pipeline() {
//!     let source = TestSource::from(vec![1, 2, 3, 4, 5]);
//!     let sink = CollectSink::<i32>::new();
//!
//!     let graph = DataflowGraph::new();
//!     graph.source(source)
//!         .filter(|x: &i32| x % 2 == 0)
//!         .map(|x: i32| x * 10)
//!         .sink(sink.clone());
//!
//!     run_test_pipeline(graph).await.unwrap();
//!
//!     sink.assert_eq(vec![20, 40]);
//!     sink.assert_contains(&20);
//!     sink.assert_len(2);
//! }
//! ```

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::traits::{Sink, Source};

// ── TestSource ──────────────────────────────────────────────────────

/// An in-memory source for testing. Wraps a `VecSource` with a
/// convenient `From<Vec<T>>` constructor.
///
/// # Example
///
/// ```ignore
/// let source = TestSource::from(vec!["hello", "world"]);
/// ```
#[derive(Debug)]
pub struct TestSource<T: Send> {
    inner: crate::connectors::vec_source::VecSource<T>,
}

impl<T: Send> TestSource<T> {
    /// Create a new test source from a vector of items.
    pub fn new(items: Vec<T>) -> Self {
        Self {
            inner: crate::connectors::vec_source::VecSource::new(items),
        }
    }

    /// Set the batch size (number of items emitted per `next_batch` call).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.inner = self.inner.with_batch_size(size);
        self
    }
}

impl<T: Send> From<Vec<T>> for TestSource<T> {
    fn from(items: Vec<T>) -> Self {
        Self::new(items)
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> Source for TestSource<T> {
    type Output = T;

    async fn next_batch(&mut self) -> Option<Vec<T>> {
        self.inner.next_batch().await
    }

    fn should_emit_watermark(&self) -> bool {
        self.inner.should_emit_watermark()
    }
}

// ── CollectSink ─────────────────────────────────────────────────────

/// A sink that collects all received items into a shared `Vec`.
///
/// `Clone`-able so you can pass it to `.sink()` and keep a handle for
/// assertions. Thread-safe via `Arc<Mutex<_>>`.
///
/// # Example
///
/// ```ignore
/// let sink = CollectSink::<String>::new();
/// // ... wire into graph ...
/// sink.assert_eq(vec!["hello".to_string(), "world".to_string()]);
/// ```
#[derive(Debug)]
pub struct CollectSink<T> {
    collected: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectSink<T> {
    /// Create a new empty collecting sink.
    pub fn new() -> Self {
        Self {
            collected: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a snapshot of all collected items.
    ///
    /// Recovers from a poisoned mutex by accessing the inner data.
    pub fn items(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.collected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns the number of collected items.
    pub fn len(&self) -> usize {
        self.collected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Returns `true` if no items have been collected.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Assert that collected items match expected (order-sensitive).
    ///
    /// # Panics
    ///
    /// Panics with a diff-friendly message if items don't match.
    pub fn assert_eq(&self, expected: Vec<T>)
    where
        T: Clone + std::fmt::Debug + PartialEq,
    {
        let actual = self.items();
        assert_eq!(
            actual, expected,
            "collected items do not match expected\n  actual:   {actual:?}\n  expected: {expected:?}"
        );
    }

    /// Assert that collected items match expected after sorting.
    ///
    /// Use when pipeline output order is non-deterministic (e.g. multi-worker).
    ///
    /// # Panics
    ///
    /// Panics with a diff-friendly message if sorted items don't match.
    pub fn assert_eq_unordered(&self, mut expected: Vec<T>)
    where
        T: Clone + std::fmt::Debug + PartialEq + Ord,
    {
        let mut actual = self.items();
        actual.sort();
        expected.sort();
        assert_eq!(
            actual, expected,
            "collected items (sorted) do not match expected\n  actual:   {actual:?}\n  expected: {expected:?}"
        );
    }

    /// Assert that collected items contain a specific value.
    ///
    /// # Panics
    ///
    /// Panics if the value is not found.
    pub fn assert_contains(&self, value: &T)
    where
        T: Clone + std::fmt::Debug + PartialEq,
    {
        let items = self.items();
        assert!(
            items.contains(value),
            "expected to find {value:?} in collected items: {items:?}"
        );
    }

    /// Assert that the number of collected items matches.
    ///
    /// # Panics
    ///
    /// Panics if length doesn't match.
    pub fn assert_len(&self, expected: usize)
    where
        T: Clone + std::fmt::Debug,
    {
        let actual = self.len();
        assert_eq!(
            actual,
            expected,
            "expected {expected} items but collected {actual}: {:?}",
            self.items()
        );
    }

    /// Assert that all collected items satisfy a predicate.
    ///
    /// # Panics
    ///
    /// Panics if any item fails the predicate.
    pub fn assert_all<F>(&self, predicate: F)
    where
        T: Clone + std::fmt::Debug,
        F: Fn(&T) -> bool,
    {
        let items = self.items();
        for (i, item) in items.iter().enumerate() {
            assert!(
                predicate(item),
                "item at index {i} failed predicate: {item:?}"
            );
        }
    }

    /// Assert that no items were collected.
    ///
    /// # Panics
    ///
    /// Panics if any items were collected.
    pub fn assert_empty(&self)
    where
        T: Clone + std::fmt::Debug,
    {
        let items = self.items();
        assert!(
            items.is_empty(),
            "expected no items but collected {}: {:?}",
            items.len(),
            items
        );
    }
}

impl<T> Default for CollectSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for CollectSink<T> {
    fn clone(&self) -> Self {
        Self {
            collected: self.collected.clone(),
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Sink for CollectSink<T> {
    type Input = T;

    async fn write(&mut self, input: T) -> anyhow::Result<()> {
        self.collected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(input);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_source_emits_all_items() {
        let mut source = TestSource::from(vec![1, 2, 3]);
        let mut all = Vec::new();
        while let Some(batch) = source.next_batch().await {
            all.extend(batch);
        }
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn collect_sink_basic() {
        let sink = CollectSink::<i32>::new();
        let mut writer = sink.clone();
        writer.write(1).await.unwrap();
        writer.write(2).await.unwrap();
        writer.write(3).await.unwrap();

        sink.assert_eq(vec![1, 2, 3]);
        sink.assert_len(3);
        sink.assert_contains(&2);
    }

    #[tokio::test]
    async fn collect_sink_unordered() {
        let sink = CollectSink::<i32>::new();
        let mut writer = sink.clone();
        writer.write(3).await.unwrap();
        writer.write(1).await.unwrap();
        writer.write(2).await.unwrap();

        sink.assert_eq_unordered(vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn collect_sink_assert_all() {
        let sink = CollectSink::<i32>::new();
        let mut writer = sink.clone();
        writer.write(2).await.unwrap();
        writer.write(4).await.unwrap();
        writer.write(6).await.unwrap();

        sink.assert_all(|x| x % 2 == 0);
    }

    #[test]
    fn collect_sink_empty() {
        let sink = CollectSink::<i32>::new();
        assert!(sink.is_empty());
        sink.assert_empty();
    }
}
