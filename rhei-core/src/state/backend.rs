use async_trait::async_trait;
use bytes::Bytes;

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
}
