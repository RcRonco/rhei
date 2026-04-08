use async_trait::async_trait;
use bytes::Bytes;

/// A single operation in a batch write: either a put or a delete.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Store a key-value pair.
    Put {
        /// The key to write.
        key: Vec<u8>,
        /// The value to associate with the key.
        value: Vec<u8>,
    },
    /// Delete a key.
    Delete {
        /// The key to remove.
        key: Vec<u8>,
    },
}

/// Trait abstracting a key-value state backend (e.g. local disk, S3/SlateDB).
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Retrieves the value for `key`, or `None` if it does not exist.
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>>;
    /// Stores a key-value pair, overwriting any previous value.
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    /// Removes the given key from the backend.
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
    /// Durably persists all pending writes.
    async fn checkpoint(&self) -> anyhow::Result<()>;

    /// Atomically writes a batch of put/delete operations.
    ///
    /// The default implementation applies operations sequentially. Backends
    /// that support native atomic batches (e.g. SlateDB `WriteBatch`) should
    /// override this for atomicity and better performance.
    ///
    /// # Invariant
    ///
    /// After a successful `put_batch`, all operations in the batch must be
    /// visible to subsequent reads. On failure, the backend may be in a
    /// partially-applied state (unless the backend provides native atomicity).
    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        for op in ops {
            match op {
                BatchOp::Put { key, value } => self.put(&key, &value).await?,
                BatchOp::Delete { key } => self.delete(&key).await?,
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T: StateBackend> StateBackend for std::sync::Arc<T> {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        (**self).get(key).await
    }
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        (**self).put(key, value).await
    }
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        (**self).delete(key).await
    }
    async fn checkpoint(&self) -> anyhow::Result<()> {
        (**self).checkpoint().await
    }
    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        (**self).put_batch(ops).await
    }
}
