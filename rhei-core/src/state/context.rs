use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::backend::{BatchOp, StateBackend};
use super::memtable::MemTable;
use super::timer_service::TimerService;

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
    /// Lazy-initialized timer service for event-time callbacks.
    timer_service: Option<TimerService>,
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
            timer_service: None,
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
    pub fn put<V: Serialize>(&mut self, key: &[u8], value: &V) -> anyhow::Result<()> {
        let encoded = bincode::serialize(value)?;
        self.put_raw(key, &encoded);
        Ok(())
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
                Ok(Some(v))
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

    /// Returns all keys in the memtable that start with the given prefix.
    ///
    /// **Important:** This only returns keys currently in the L1 memtable —
    /// it does NOT scan the backend (L2/L3). For TTL cleanup this is sufficient
    /// because all active state passes through the memtable on read.
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.memtable.keys_with_prefix(prefix)
    }

    /// Returns a mutable reference to the timer service, creating it if needed.
    pub fn timers(&mut self) -> &mut TimerService {
        self.timer_service.get_or_insert_with(TimerService::new)
    }

    /// Returns `true` if a timer service has been created and has registered timers.
    pub fn has_timers(&self) -> bool {
        self.timer_service.as_ref().is_some_and(|ts| !ts.is_empty())
    }

    /// Restore timer state from the backend.
    pub async fn restore_timers(&mut self) -> anyhow::Result<()> {
        if let Some(bytes) = self.get_raw(super::timer_service::TIMER_STATE_KEY).await? {
            self.timer_service = Some(TimerService::restore(&bytes)?);
        }
        Ok(())
    }

    /// Flush dirty entries from memtable to backend, then trigger backend checkpoint.
    ///
    /// Dirty keys are collected into a single `put_batch` call so that backends
    /// with native atomic batches (e.g. SlateDB `WriteBatch`) can apply them
    /// atomically. This prevents partial flushes from leaving the backend in
    /// an inconsistent state on crash.
    ///
    /// # Invariants
    ///
    /// - After a successful checkpoint, all dirty entries are persisted to the
    ///   backend and the memtable's dirty set is empty.
    /// - Timer state is included in the same batch when dirty.
    /// - The backend's `checkpoint()` is called after the batch write to ensure
    ///   durability (no-op for SlateDB which is WAL-durable by default).
    pub async fn checkpoint(&mut self) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let dirty = self.memtable.flush();
        let dirty_count = dirty.len();

        tracing::info!(dirty_keys = dirty_count, "checkpointing state");
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("state_checkpoint_dirty_keys").set(dirty_count as f64);

        // Collect all dirty entries into batch operations.
        let mut ops: Vec<BatchOp> = Vec::with_capacity(dirty_count + 1);
        for (key, value) in dirty {
            match value {
                Some(v) => ops.push(BatchOp::Put {
                    key,
                    value: v.to_vec(),
                }),
                None => ops.push(BatchOp::Delete { key }),
            }
        }

        // Include timer state in the same batch if dirty.
        if let Some(ref mut ts) = self.timer_service
            && ts.is_dirty()
        {
            let timer_bytes = ts.serialize()?;
            ops.push(BatchOp::Put {
                key: super::timer_service::TIMER_STATE_KEY.to_vec(),
                value: timer_bytes,
            });
            ts.clear_dirty();
        }

        if !ops.is_empty() {
            self.backend.put_batch(ops).await?;
        }

        self.backend.checkpoint().await?;

        metrics::counter!("state_checkpoints_total").increment(1);
        metrics::histogram!("state_checkpoint_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

        ctx.put(b"count", &42u64).unwrap();
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
            ctx.put(b"a", &100u64).unwrap();
            ctx.put(b"b", &"hello".to_string()).unwrap();
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
    async fn keys_with_prefix_returns_matching_keys() {
        let path = temp_path("prefix_keys");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));

        ctx.put_raw(b"map:key1", b"val1");
        ctx.put_raw(b"map:key2", b"val2");
        ctx.put_raw(b"other:key3", b"val3");

        let mut keys = ctx.keys_with_prefix(b"map:");
        keys.sort();
        assert_eq!(keys, vec![b"map:key1".to_vec(), b"map:key2".to_vec()]);

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
