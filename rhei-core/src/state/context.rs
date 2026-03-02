use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::backend::StateBackend;
use super::memtable::MemTable;

/// Operator-scoped state context.
///
/// Reads go through the L1 `MemTable` first (read-your-own-writes). On miss the
/// request falls through to the backend. Writes always go to the `MemTable`
/// (synchronous, fast). Dirty entries are flushed to the backend on checkpoint.
pub struct StateContext {
    memtable: MemTable,
    backend: Box<dyn StateBackend>,
    /// Optional worker label for per-worker metrics. `None` means no label.
    worker_label: Option<String>,
}

impl std::fmt::Debug for StateContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateContext")
            .field("memtable", &self.memtable)
            .finish_non_exhaustive()
    }
}

impl StateContext {
    /// Creates a new `StateContext` backed by the given state backend.
    pub fn new(backend: Box<dyn StateBackend>) -> Self {
        Self {
            memtable: MemTable::new(),
            backend,
            worker_label: None,
        }
    }

    /// Creates a new `StateContext` with a worker label for per-worker metrics.
    pub fn with_worker_label(mut self, label: String) -> Self {
        self.worker_label = Some(label);
        self
    }

    /// Replace the memtable with one using the given configuration.
    pub fn with_memtable_config(mut self, config: super::memtable::MemTableConfig) -> Self {
        self.memtable = MemTable::with_config(config);
        self
    }

    /// Get a typed value — deserializes via bincode.
    pub async fn get<V: DeserializeOwned>(&mut self, key: &[u8]) -> anyhow::Result<Option<V>> {
        match self.get_raw(key).await? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Put a typed value — serializes via bincode.
    pub fn put<V: Serialize>(&mut self, key: &[u8], value: &V) {
        let encoded = bincode::serialize(value).expect("bincode serialization failed");
        self.put_raw(key, &encoded);
    }

    /// Get raw bytes — checks memtable first, then falls back to backend.
    pub async fn get_raw(&mut self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        if let Some(ref wl) = self.worker_label {
            metrics::counter!("state_gets_total", "worker" => wl.clone()).increment(1);
        } else {
            metrics::counter!("state_gets_total").increment(1);
        }
        let start = std::time::Instant::now();

        let result = match self.memtable.get(key) {
            Some(Some(v)) => {
                if let Some(ref wl) = self.worker_label {
                    metrics::counter!("state_l1_hits_total", "worker" => wl.clone()).increment(1);
                } else {
                    metrics::counter!("state_l1_hits_total").increment(1);
                }
                Ok(Some(v.clone()))
            }
            Some(None) => {
                if let Some(ref wl) = self.worker_label {
                    metrics::counter!("state_l1_hits_total", "worker" => wl.clone()).increment(1);
                } else {
                    metrics::counter!("state_l1_hits_total").increment(1);
                }
                Ok(None) // tombstone — key was deleted
            }
            None => {
                if let Some(ref wl) = self.worker_label {
                    metrics::counter!("state_l1_misses_total", "worker" => wl.clone()).increment(1);
                } else {
                    metrics::counter!("state_l1_misses_total").increment(1);
                }
                // Cache miss — fetch from backend
                let result = self.backend.get(key).await?;
                if let Some(ref v) = result {
                    self.memtable.merge(key.to_vec(), v.clone());
                }
                Ok(result)
            }
        };

        metrics::histogram!("state_get_duration_seconds").record(start.elapsed().as_secs_f64());
        result
    }

    /// Put raw bytes — writes to memtable only (synchronous).
    pub fn put_raw(&mut self, key: &[u8], value: &[u8]) {
        if let Some(ref wl) = self.worker_label {
            metrics::counter!("state_puts_total", "worker" => wl.clone()).increment(1);
        } else {
            metrics::counter!("state_puts_total").increment(1);
        }
        self.memtable
            .put(key.to_vec(), Bytes::copy_from_slice(value));
    }

    /// Delete a key — marks as deleted in memtable.
    pub fn delete(&mut self, key: &[u8]) {
        if let Some(ref wl) = self.worker_label {
            metrics::counter!("state_deletes_total", "worker" => wl.clone()).increment(1);
        } else {
            metrics::counter!("state_deletes_total").increment(1);
        }
        self.memtable.delete(key.to_vec());
    }

    /// Flush dirty entries from memtable to backend, then trigger backend checkpoint.
    pub async fn checkpoint(&mut self) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let dirty = self.memtable.flush();
        let dirty_count = dirty.len();

        tracing::info!(dirty_keys = dirty_count, "checkpointing state");
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("state_checkpoint_dirty_keys").set(dirty_count as f64);

        for (key, value) in dirty {
            match value {
                Some(v) => self.backend.put(&key, &v).await?,
                None => self.backend.delete(&key).await?,
            }
        }
        self.backend.checkpoint().await?;

        metrics::counter!("state_checkpoints_total").increment(1);
        metrics::histogram!("state_checkpoint_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rhei_ctx_test_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn typed_read_your_own_writes() {
        let path = temp_path("typed_ryw");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));

        ctx.put(b"count", &42u64);
        let val: Option<u64> = ctx.get(b"count").await.unwrap();
        assert_eq!(val, Some(42));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn raw_read_your_own_writes() {
        let path = temp_path("raw_ryw");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));

        ctx.put_raw(b"key", b"value");
        let val = ctx.get_raw(b"key").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"value".as_slice()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn typed_checkpoint_roundtrip() {
        let path = temp_path("typed_ckpt");
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            ctx.put(b"a", &100u64);
            ctx.put(b"b", &"hello".to_string());
            ctx.checkpoint().await.unwrap();
        }

        // Reopen and verify via a fresh context
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        assert_eq!(ctx.get::<u64>(b"a").await.unwrap(), Some(100));
        assert_eq!(
            ctx.get::<String>(b"b").await.unwrap().as_deref(),
            Some("hello")
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn delete_hides_backend_value() {
        let path = temp_path("delete_hide");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        backend.put(b"key", b"val").await.unwrap();

        let mut ctx = StateContext::new(Box::new(backend));
        ctx.delete(b"key");
        let val = ctx.get_raw(b"key").await.unwrap();
        assert_eq!(val, None);

        let _ = std::fs::remove_file(&path);
    }
}
