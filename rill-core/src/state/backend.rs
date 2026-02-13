use async_trait::async_trait;

/// Trait abstracting a key-value state backend (e.g. local disk, S3/SlateDB).
#[async_trait]
pub trait StateBackend: Send + Sync {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
    async fn checkpoint(&self) -> anyhow::Result<()>;
}
