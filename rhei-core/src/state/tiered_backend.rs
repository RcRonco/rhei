use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use foyer::{DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};

use super::backend::StateBackend;
use super::slatedb_backend::SlateDbBackend;

/// Configuration for the L2 Foyer disk cache layer.
#[derive(Debug)]
pub struct TieredBackendConfig {
    /// Directory for Foyer's on-disk cache files.
    pub foyer_dir: PathBuf,
    /// In-memory capacity for the Foyer cache (bytes). Default: 64 MiB.
    pub foyer_memory_capacity: usize,
    /// On-disk capacity for the Foyer cache (bytes). Default: 256 MiB.
    pub foyer_disk_capacity: usize,
}

impl Default for TieredBackendConfig {
    fn default() -> Self {
        Self {
            foyer_dir: PathBuf::from("/tmp/rhei-foyer"),
            foyer_memory_capacity: 64 * 1024 * 1024,
            foyer_disk_capacity: 256 * 1024 * 1024,
        }
    }
}

/// L2 + L3 backend: Foyer `HybridCache` (disk/memory) in front of `SlateDB` (object storage).
///
/// Read path: L2 Foyer → L3 `SlateDB` → backfill L2 on hit.
/// Write path: write-through to L3, update L2.
pub struct TieredBackend {
    l2: HybridCache<Vec<u8>, Vec<u8>>,
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
    pub fn with_cache(l2: HybridCache<Vec<u8>, Vec<u8>>, l3: Arc<SlateDbBackend>) -> Self {
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
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
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
        let value_vec = value.to_vec();

        // Write-through: L3 first, then update L2
        self.l3.put(key, value).await?;
        self.l2.insert(key_vec, value_vec);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// Build a memory-only `HybridCache` for fast tests (no disk I/O).
    /// Skips `.with_engine_config()` so foyer treats storage as noop.
    async fn memory_only_cache() -> HybridCache<Vec<u8>, Vec<u8>> {
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
        assert_eq!(val, Some(b"value".to_vec()));

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
        assert_eq!(val, Some(b"value".to_vec()));

        // Second read should hit L2 (we can't easily assert which layer,
        // but the value should still be correct)
        let val = backend.get(b"deep").await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

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
}
