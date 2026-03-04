//! Async I/O enrichment operator.
//!
//! Bounded-concurrency async lookup for enriching stream elements.
//! Uses `process_batch` override for batch-level concurrency via
//! semaphore-guarded tokio spawns.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

/// An async enrichment operator with bounded concurrency.
///
/// Wraps an async function `F: Fn(T) -> Future<Output = Result<O>>`.
/// Single-element processing applies `f` with a timeout.
/// Batch processing spawns up to `concurrency` concurrent tasks,
/// preserving input ordering.
pub struct EnrichOp<F, T, O> {
    f: Arc<F>,
    concurrency: usize,
    timeout: Duration,
    _phantom: PhantomData<(T, O)>,
}

impl<F, T, O> Clone for EnrichOp<F, T, O> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            concurrency: self.concurrency,
            timeout: self.timeout,
            _phantom: PhantomData,
        }
    }
}

impl<F, T, O> fmt::Debug for EnrichOp<F, T, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrichOp")
            .field("concurrency", &self.concurrency)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl<F, T, O> EnrichOp<F, T, O> {
    /// Creates a new `EnrichOp`.
    ///
    /// - `concurrency`: max parallel lookups per batch.
    /// - `timeout`: per-element timeout.
    /// - `f`: the async enrichment function.
    pub fn new(concurrency: usize, timeout: Duration, f: F) -> Self {
        Self {
            f: Arc::new(f),
            concurrency,
            timeout,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T, O, Fut> StreamFunction for EnrichOp<F, T, O>
where
    T: Clone + Send + Sync + std::fmt::Debug + 'static,
    O: Clone + Send + Sync + std::fmt::Debug + 'static,
    Fut: std::future::Future<Output = anyhow::Result<O>> + Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
{
    type Input = T;
    type Output = O;

    async fn process(&mut self, input: T, _ctx: &mut StateContext) -> anyhow::Result<Vec<O>> {
        let result = tokio::time::timeout(self.timeout, (self.f)(input)).await;
        match result {
            Ok(Ok(output)) => Ok(vec![output]),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(anyhow::anyhow!("enrichment timed out")),
        }
    }

    async fn process_batch(
        &mut self,
        inputs: Vec<T>,
        _ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        let sem = Arc::new(Semaphore::new(self.concurrency));
        let timeout = self.timeout;
        let f = self.f.clone();

        let handles: Vec<_> = inputs
            .into_iter()
            .map(|input| {
                let sem = sem.clone();
                let f = f.clone();
                tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    tokio::time::timeout(timeout, f(input)).await
                })
            })
            .collect();

        let mut outputs = Vec::with_capacity(handles.len());
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| anyhow::anyhow!("join error: {e}"))?;
            match result {
                Ok(Ok(output)) => outputs.push(output),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(anyhow::anyhow!("enrichment timed out")),
            }
        }
        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_enrich_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn single_enrichment() {
        let mut ctx = test_ctx("single");
        let mut op = EnrichOp::new(
            2,
            Duration::from_secs(1),
            |x: i32| async move { Ok(x * 10) },
        );

        let r = op.process(5, &mut ctx).await.unwrap();
        assert_eq!(r, vec![50]);
    }

    #[tokio::test]
    async fn batch_preserves_ordering() {
        let mut ctx = test_ctx("batch");
        let mut op = EnrichOp::new(2, Duration::from_secs(1), |x: i32| async move {
            Ok(format!("v{x}"))
        });

        let inputs = vec![1, 2, 3, 4, 5];
        let r = op.process_batch(inputs, &mut ctx).await.unwrap();
        assert_eq!(r, vec!["v1", "v2", "v3", "v4", "v5"]);
    }

    #[tokio::test]
    async fn failed_lookup_returns_error() {
        let mut ctx = test_ctx("failed");
        let mut op = EnrichOp::new(2, Duration::from_secs(1), |x: i32| async move {
            if x < 0 {
                Err(anyhow::anyhow!("negative lookup"))
            } else {
                Ok(x * 10)
            }
        });

        // Successful lookup
        let r = op.process(5, &mut ctx).await;
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), vec![50]);

        // Failed lookup
        let r = op.process(-1, &mut ctx).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("negative lookup"));
    }

    #[tokio::test]
    async fn timeout_returns_error() {
        let mut ctx = test_ctx("timeout");
        let mut op = EnrichOp::new(2, Duration::from_millis(10), |_x: i32| async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(0i32)
        });

        let r = op.process(1, &mut ctx).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("timed out"));
    }
}
