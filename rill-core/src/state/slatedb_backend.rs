use std::sync::Arc;

use async_trait::async_trait;
use object_store::path::Path;

use super::backend::StateBackend;

/// L3 backend wrapping a SlateDB instance on object storage.
///
/// SlateDB is durable by default (writes go to a WAL backed by the object store),
/// so `checkpoint()` is a no-op here.
pub struct SlateDbBackend {
    db: slatedb::Db,
}

impl SlateDbBackend {
    /// Open (or create) a SlateDB database at the given object-store path.
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> anyhow::Result<Self> {
        let db = slatedb::Db::open(path, object_store).await?;
        Ok(Self { db })
    }

    /// Gracefully close the database, flushing any pending writes.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.db.close().await?;
        Ok(())
    }
}

#[async_trait]
impl StateBackend for SlateDbBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.db.get(key).await?;
        Ok(result.map(|b| b.to_vec()))
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(val, Some(b"world".to_vec()));

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
            assert_eq!(val, Some(b"restart".to_vec()));
            backend.close().await.unwrap();
        }
    }
}
