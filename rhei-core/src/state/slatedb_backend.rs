use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use super::backend::{BatchOp, StateBackend};
use slatedb::object_store::path::Path;

/// Re-export `SlateDB` settings for direct configuration.
pub use slatedb::config::Settings as SlateDbSettings;

/// Configuration for the `SlateDB` L3 backend.
///
/// Wraps [`slatedb::config::Settings`] to expose tunables such as:
/// - Write buffer size
/// - Compaction options
/// - Garbage collection intervals
/// - Compression codec
/// - SST block size
///
/// Use `Default::default()` for `SlateDB`'s built-in defaults.
#[derive(Debug, Clone, Default)]
pub struct SlateDbConfig {
    /// The underlying `SlateDB` settings. All fields are public via the
    /// re-exported [`SlateDbSettings`] type.
    pub settings: SlateDbSettings,
}

/// L3 backend wrapping a `SlateDB` instance on object storage.
///
/// `SlateDB` is durable by default (writes go to a WAL backed by the object store),
/// so `checkpoint()` is a no-op here.
pub struct SlateDbBackend {
    db: slatedb::Db,
    path: Path,
    object_store: Arc<dyn object_store::ObjectStore>,
}

/// Durable identity of an immutable `SlateDB` checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SlateDbSnapshot {
    /// Object-store database namespace that owns the checkpoint.
    pub database_path: String,
    /// `SlateDB` checkpoint UUID. It is retained until explicitly deleted.
    pub checkpoint_id: String,
}

impl std::fmt::Debug for SlateDbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDbBackend").finish_non_exhaustive()
    }
}

impl SlateDbBackend {
    /// Open (or create) a `SlateDB` database at the given object-store path
    /// with default settings.
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let db = slatedb::Db::open(path.clone(), object_store.clone()).await?;
        Ok(Self {
            db,
            path,
            object_store,
        })
    }

    /// Open (or create) a `SlateDB` database with custom configuration.
    ///
    /// Use this when you need to tune write buffer size, compaction, or
    /// other `SlateDB` internals.
    pub async fn open_with_config(
        path: impl Into<Path>,
        object_store: Arc<dyn object_store::ObjectStore>,
        config: SlateDbConfig,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let db = slatedb::Db::builder(path.clone(), object_store.clone())
            .with_settings(config.settings)
            .build()
            .await?;
        Ok(Self {
            db,
            path,
            object_store,
        })
    }

    /// Capture all prior writes in an immutable checkpoint.
    /// The caller must first quiesce every writer contributing to the job cut.
    pub async fn snapshot(&self) -> anyhow::Result<SlateDbSnapshot> {
        let checkpoint = self
            .db
            .create_checkpoint(
                slatedb::config::CheckpointScope::All,
                &slatedb::config::CheckpointOptions::default(),
            )
            .await?;
        Ok(SlateDbSnapshot {
            database_path: self.path.to_string(),
            checkpoint_id: checkpoint.id.to_string(),
        })
    }

    /// Open an isolated attempt, optionally cloned from an exact checkpoint.
    /// An attempt must use a fresh suffix; the live database is never rolled back.
    pub async fn recovery_attempt(
        &self,
        suffix: &str,
        snapshot: Option<&SlateDbSnapshot>,
    ) -> anyhow::Result<Self> {
        let path = self.path.child("recovery-attempts").child(suffix);
        anyhow::ensure!(
            self.object_store.list(Some(&path)).next().await.is_none(),
            "recovery attempt path already exists: {path}"
        );
        if let Some(snapshot) = snapshot {
            let admin =
                slatedb::admin::Admin::builder(path.clone(), self.object_store.clone()).build();
            admin
                .create_clone(
                    snapshot.database_path.as_str(),
                    Some(snapshot.checkpoint_id.parse()?),
                )
                .await
                .map_err(|e| anyhow::anyhow!("SlateDB snapshot restore failed: {e}"))?;
        }
        Self::open(path, self.object_store.clone()).await
    }

    /// Gracefully close the database, flushing any pending writes.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.db.close().await?;
        Ok(())
    }
}

#[async_trait]
impl StateBackend for SlateDbBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        let result = self.db.get(key).await?;
        Ok(result)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db.put(key, value).await?;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.db.delete(key).await?;
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        // SlateDB is durable by default — no explicit checkpoint needed.
        Ok(())
    }

    /// Atomic batch write using `SlateDB`'s native `WriteBatch`.
    ///
    /// All operations in the batch are applied atomically — either all
    /// succeed or none are visible. This is critical for checkpoint
    /// correctness: a partial flush must not leave the backend in an
    /// inconsistent state.
    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut batch = slatedb::WriteBatch::new();
        for op in ops {
            match op {
                BatchOp::Put { key, value } => batch.put(&key, &value),
                BatchOp::Delete { key } => batch.delete(&key),
            }
        }
        self.db.write(batch).await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;

    async fn open_test_db(store: Arc<InMemory>, path: &str) -> SlateDbBackend {
        SlateDbBackend::open(path, store).await.unwrap()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let store = Arc::new(InMemory::new());
        let backend = open_test_db(store, "test_roundtrip").await;

        backend.put(b"hello", b"world").await.unwrap();
        let val = backend.get(b"hello").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"world")));

        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn get_miss_returns_none() {
        let store = Arc::new(InMemory::new());
        let backend = open_test_db(store, "test_miss").await;

        let val = backend.get(b"nonexistent").await.unwrap();
        assert_eq!(val, None);

        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let store = Arc::new(InMemory::new());
        let backend = open_test_db(store, "test_delete").await;

        backend.put(b"key", b"val").await.unwrap();
        backend.delete(b"key").await.unwrap();

        let val = backend.get(b"key").await.unwrap();
        assert_eq!(val, None);

        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn put_batch_atomic_roundtrip() {
        let store = Arc::new(InMemory::new());
        let backend = open_test_db(store, "test_batch").await;

        let ops = vec![
            BatchOp::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            BatchOp::Put {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
            BatchOp::Delete {
                key: b"k1".to_vec(),
            },
        ];

        backend.put_batch(ops).await.unwrap();

        // k1 was put then deleted in the same batch — should be gone.
        assert_eq!(backend.get(b"k1").await.unwrap(), None);
        // k2 should be present.
        assert_eq!(
            backend.get(b"k2").await.unwrap(),
            Some(Bytes::from_static(b"v2"))
        );

        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn put_batch_empty_is_noop() {
        let store = Arc::new(InMemory::new());
        let backend = open_test_db(store, "test_batch_empty").await;

        backend.put_batch(vec![]).await.unwrap();

        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_reopen_persistence() {
        let store = Arc::new(InMemory::new());

        // Write and close
        {
            let backend = open_test_db(store.clone(), "test_persist").await;
            backend.put(b"survive", b"restart").await.unwrap();
            backend.close().await.unwrap();
        }

        // Reopen and verify
        {
            let backend = open_test_db(store, "test_persist").await;
            let val = backend.get(b"survive").await.unwrap();
            assert_eq!(val, Some(Bytes::from_static(b"restart")));
            backend.close().await.unwrap();
        }
    }
}
