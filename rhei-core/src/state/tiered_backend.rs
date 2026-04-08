use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::{DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};

use super::backend::{BatchOp, StateBackend};
use super::slatedb_backend::SlateDbBackend;

/// Configuration for the L2 Foyer disk cache layer.
#[derive(Debug)]
pub struct TieredBackendConfig {
    /// Directory for Foyer's on-disk cache files.
    /// Default: `/tmp/rhei-foyer-{pid}` (PID-scoped to avoid collisions
    /// between concurrent processes sharing the same host).
    pub foyer_dir: PathBuf,
    /// In-memory capacity for the Foyer cache (bytes). Default: 64 MiB.
    pub foyer_memory_capacity: usize,
    /// On-disk capacity for the Foyer cache (bytes). Default: 256 MiB.
    pub foyer_disk_capacity: usize,
}

impl Default for TieredBackendConfig {
    fn default() -> Self {
        Self {
            foyer_dir: PathBuf::from(format!("/tmp/rhei-foyer-{}", std::process::id())),
            foyer_memory_capacity: 64 * 1024 * 1024,
            foyer_disk_capacity: 256 * 1024 * 1024,
        }
    }
}

/// A shared Foyer L2 cache that can be used across multiple operators.
///
/// Clone is cheap — `HybridCache` is internally `Arc`-based.
#[derive(Clone)]
pub struct SharedL2Cache {
    cache: HybridCache<Vec<u8>, Bytes>,
}

impl SharedL2Cache {
    /// Build a shared cache with the given config.
    pub async fn open(config: &TieredBackendConfig) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.foyer_dir)?;

        let device = FsDeviceBuilder::new(&config.foyer_dir)
            .with_capacity(config.foyer_disk_capacity)
            .build()?;

        let cache = HybridCacheBuilder::new()
            .memory(config.foyer_memory_capacity)
            .storage()
            .with_engine_config(
                foyer::BlockEngineConfig::new(device).with_block_size(4 * 1024 * 1024),
            )
            .build()
            .await?;

        Ok(Self { cache })
    }

    /// Build a memory-only shared cache (for tests).
    pub async fn memory_only() -> anyhow::Result<Self> {
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .storage()
            .build()
            .await?;
        Ok(Self { cache })
    }

    /// Create a `TieredBackend` using this shared cache.
    pub fn create_tiered_backend(&self, l3: Arc<SlateDbBackend>) -> TieredBackend {
        TieredBackend::with_cache(self.cache.clone(), l3)
    }
}

impl std::fmt::Debug for SharedL2Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedL2Cache").finish_non_exhaustive()
    }
}

/// L2 + L3 backend: Foyer `HybridCache` (disk/memory) in front of `SlateDB` (object storage).
///
/// Read path: L2 Foyer → L3 `SlateDB` → backfill L2 on hit.
/// Write path: write-through to L3, update L2.
pub struct TieredBackend {
    l2: HybridCache<Vec<u8>, Bytes>,
    l3: Arc<SlateDbBackend>,
}

impl std::fmt::Debug for TieredBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredBackend")
            .field("l3", &self.l3)
            .finish_non_exhaustive()
    }
}

impl TieredBackend {
    /// Build a new `TieredBackend` with a Foyer disk cache and L3 `SlateDB` backend.
    pub async fn open(
        config: TieredBackendConfig,
        l3: Arc<SlateDbBackend>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.foyer_dir)?;

        let device = FsDeviceBuilder::new(&config.foyer_dir)
            .with_capacity(config.foyer_disk_capacity)
            .build()?;

        let l2 = HybridCacheBuilder::new()
            .memory(config.foyer_memory_capacity)
            .storage()
            .with_engine_config(
                foyer::BlockEngineConfig::new(device).with_block_size(4 * 1024 * 1024), // 4 MiB blocks
            )
            .build()
            .await?;

        Ok(Self { l2, l3 })
    }

    /// Build a `TieredBackend` with a pre-built `HybridCache` (useful for testing).
    pub fn with_cache(l2: HybridCache<Vec<u8>, Bytes>, l3: Arc<SlateDbBackend>) -> Self {
        Self { l2, l3 }
    }

    /// Close both L2 and L3 gracefully.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.l2.close().await?;
        self.l3.close().await?;
        Ok(())
    }
}

#[async_trait]
impl StateBackend for TieredBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let key_vec = key.to_vec();

        // Check L2 first
        if let Some(entry) = self.l2.get(&key_vec).await? {
            tracing::trace!("L2 cache hit");
            metrics::counter!("state_l2_hits_total").increment(1);
            return Ok(Some(entry.value().clone()));
        }
        metrics::counter!("state_l2_misses_total").increment(1);

        // Fall through to L3
        let result = self.l3.get(key).await?;
        if let Some(ref value) = result {
            tracing::trace!("L3 hit, backfilling L2");
            metrics::counter!("state_l3_hits_total").increment(1);
            // Backfill L2
            self.l2.insert(key_vec, value.clone());
        } else {
            metrics::counter!("state_l3_misses_total").increment(1);
        }

        Ok(result)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let key_vec = key.to_vec();

        // Write-through: L3 first, then update L2
        self.l3.put(key, value).await?;
        self.l2.insert(key_vec, Bytes::from(value.to_vec()));
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        let key_vec = key.to_vec();

        // Remove from both layers
        self.l3.delete(key).await?;
        self.l2.remove(&key_vec);
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        // Delegate to L3 (no-op for SlateDB, but respects the trait contract)
        self.l3.checkpoint().await
    }

    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        // Write-through: atomic batch to L3, then update L2 cache.
        // L2 updates are best-effort cache population — not critical for
        // correctness since L2 is a read cache that can be repopulated.
        let l2_ops: Vec<(Vec<u8>, Option<Bytes>)> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => (key.clone(), Some(Bytes::from(value.clone()))),
                BatchOp::Delete { key } => (key.clone(), None),
            })
            .collect();

        self.l3.put_batch(ops).await?;

        // Update L2 cache to reflect the batch writes.
        for (key, value) in l2_ops {
            match value {
                Some(v) => {
                    self.l2.insert(key, v);
                }
                None => {
                    self.l2.remove(&key);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;

    /// Build a memory-only `HybridCache` for fast tests (no disk I/O).
    /// Skips `.with_engine_config()` so foyer treats storage as noop.
    async fn memory_only_cache() -> HybridCache<Vec<u8>, Bytes> {
        HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .storage()
            .build()
            .await
            .unwrap()
    }

    async fn setup(name: &str) -> TieredBackend {
        let store = Arc::new(InMemory::new());
        let l3 = Arc::new(
            SlateDbBackend::open(format!("test_{name}"), store)
                .await
                .unwrap(),
        );
        let l2 = memory_only_cache().await;
        TieredBackend::with_cache(l2, l3)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_through_and_read() {
        let backend = setup("write_through").await;

        backend.put(b"key", b"value").await.unwrap();
        let val = backend.get(b"key").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value")));

        backend.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn l3_fallback_and_backfill() {
        let store = Arc::new(InMemory::new());
        let l3 = Arc::new(SlateDbBackend::open("test_fallback", store).await.unwrap());

        // Write directly to L3, bypassing L2
        l3.put(b"deep", b"value").await.unwrap();

        let l2 = memory_only_cache().await;
        let backend = TieredBackend::with_cache(l2, l3);

        // First read: L2 miss → L3 hit → backfill L2
        let val = backend.get(b"deep").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value")));

        // Second read should hit L2 (we can't easily assert which layer,
        // but the value should still be correct)
        let val = backend.get(b"deep").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value")));

        backend.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_consistency() {
        let backend = setup("delete").await;

        backend.put(b"key", b"val").await.unwrap();
        backend.delete(b"key").await.unwrap();

        let val = backend.get(b"key").await.unwrap();
        assert_eq!(val, None);

        backend.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_miss_returns_none() {
        let backend = setup("miss").await;

        let val = backend.get(b"nonexistent").await.unwrap();
        assert_eq!(val, None);

        backend.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_l2_cache_across_operators() {
        use crate::state::prefixed_backend::PrefixedBackend;

        let store = Arc::new(InMemory::new());
        let l3 = Arc::new(SlateDbBackend::open("test_shared_l2", store).await.unwrap());

        let shared = SharedL2Cache::memory_only().await.unwrap();

        // Two operators sharing the same L2 cache (namespaced via PrefixedBackend).
        let backend_a = shared.create_tiered_backend(l3.clone());
        let op_a = PrefixedBackend::new("op_a", Box::new(backend_a)).unwrap();

        let backend_b = shared.create_tiered_backend(l3.clone());
        let op_b = PrefixedBackend::new("op_b", Box::new(backend_b)).unwrap();

        // Write via op_a, read via op_a — should work.
        op_a.put(b"key", b"val_a").await.unwrap();
        assert_eq!(
            op_a.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"val_a"))
        );

        // op_b's "key" is namespaced differently — should not see op_a's data.
        assert_eq!(op_b.get(b"key").await.unwrap(), None);

        // Write via op_b independently.
        op_b.put(b"key", b"val_b").await.unwrap();
        assert_eq!(
            op_b.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"val_b"))
        );

        // op_a's data is unchanged.
        assert_eq!(
            op_a.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"val_a"))
        );

        l3.close().await.unwrap();
    }
}
