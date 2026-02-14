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
        }
    }

    /// Get a value — checks memtable first, then falls back to backend.
    pub async fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        metrics::counter!("state_gets_total").increment(1);
        let start = std::time::Instant::now();

        let result = match self.memtable.get(key) {
            Some(Some(v)) => {
                metrics::counter!("state_l1_hits_total").increment(1);
                Ok(Some(v.clone()))
            }
            Some(None) => {
                metrics::counter!("state_l1_hits_total").increment(1);
                Ok(None) // tombstone — key was deleted
            }
            None => {
                metrics::counter!("state_l1_misses_total").increment(1);
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

    /// Put a value — writes to memtable only (synchronous).
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        metrics::counter!("state_puts_total").increment(1);
        self.memtable.put(key.to_vec(), value.to_vec());
    }

    /// Delete a key — marks as deleted in memtable.
    pub fn delete(&mut self, key: &[u8]) {
        metrics::counter!("state_deletes_total").increment(1);
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
        std::env::temp_dir().join(format!("rill_ctx_test_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn read_your_own_writes() {
        let path = temp_path("ryw");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));

        ctx.put(b"key", b"value");
        let val = ctx.get(b"key").await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn read_falls_through_to_backend() {
        let path = temp_path("fallthrough");
        let _ = std::fs::remove_file(&path);

        // Pre-populate the backend
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        backend.put(b"backend_key", b"backend_val").await.unwrap();

        let mut ctx = StateContext::new(Box::new(backend));
        let val = ctx.get(b"backend_key").await.unwrap();
        assert_eq!(val, Some(b"backend_val".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn checkpoint_persists_to_backend() {
        let path = temp_path("ckpt");
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            ctx.put(b"a", b"1");
            ctx.put(b"b", b"2");
            ctx.checkpoint().await.unwrap();
        }

        // Verify persistence via a new backend
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        assert_eq!(backend.get(b"a").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(backend.get(b"b").await.unwrap(), Some(b"2".to_vec()));

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
        let val = ctx.get(b"key").await.unwrap();
        assert_eq!(val, None);

        let _ = std::fs::remove_file(&path);
    }
}
