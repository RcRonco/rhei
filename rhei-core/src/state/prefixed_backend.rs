use async_trait::async_trait;

use super::backend::StateBackend;

/// A key-namespacing wrapper that prepends a prefix to every key.
///
/// This allows multiple operators to share a single backend instance
/// without key collisions. Each operator gets its own `PrefixedBackend`
/// with a unique prefix (typically the operator name).
pub struct PrefixedBackend {
    prefix: Vec<u8>,
    inner: Box<dyn StateBackend>,
}

impl std::fmt::Debug for PrefixedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixedBackend")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl PrefixedBackend {
    /// Wraps `inner` so that every key is prefixed with `prefix/`.
    pub fn new(prefix: impl Into<String>, inner: Box<dyn StateBackend>) -> Self {
        let mut prefix_bytes = prefix.into().into_bytes();
        prefix_bytes.push(b'/');
        Self {
            prefix: prefix_bytes,
            inner,
        }
    }

    fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        let mut prefixed = Vec::with_capacity(self.prefix.len() + key.len());
        prefixed.extend_from_slice(&self.prefix);
        prefixed.extend_from_slice(key);
        prefixed
    }
}

#[async_trait]
impl StateBackend for PrefixedBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(&self.prefixed_key(key)).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.inner.put(&self.prefixed_key(key), value).await
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.inner.delete(&self.prefixed_key(key)).await
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rhei_prefix_test_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn prefixed_keys_dont_collide() {
        let path = temp_path("collide");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        // Wrap in Arc so we can share the same backend via two PrefixedBackends.
        // Since LocalBackend uses Mutex internally, we can share it.
        let shared: std::sync::Arc<LocalBackend> = std::sync::Arc::new(backend);

        // Create two prefixed views on the same backend
        let op_a = PrefixedBackend::new("operator_a", Box::new(ArcBackend(shared.clone())));
        let op_b = PrefixedBackend::new("operator_b", Box::new(ArcBackend(shared.clone())));

        // Both write to "count"
        op_a.put(b"count", b"10").await.unwrap();
        op_b.put(b"count", b"20").await.unwrap();

        // Each sees its own value
        assert_eq!(op_a.get(b"count").await.unwrap(), Some(b"10".to_vec()));
        assert_eq!(op_b.get(b"count").await.unwrap(), Some(b"20".to_vec()));

        // Delete from one doesn't affect the other
        op_a.delete(b"count").await.unwrap();
        assert_eq!(op_a.get(b"count").await.unwrap(), None);
        assert_eq!(op_b.get(b"count").await.unwrap(), Some(b"20".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn prefix_is_applied_correctly() {
        let path = temp_path("prefix_format");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let shared = std::sync::Arc::new(backend);

        let prefixed = PrefixedBackend::new("myop", Box::new(ArcBackend(shared.clone())));
        prefixed.put(b"key", b"val").await.unwrap();

        // The raw backend should have the prefixed key
        let raw = shared.get(b"myop/key").await.unwrap();
        assert_eq!(raw, Some(b"val".to_vec()));

        // Non-prefixed key should not exist
        let raw = shared.get(b"key").await.unwrap();
        assert_eq!(raw, None);

        let _ = std::fs::remove_file(&path);
    }

    /// Helper: wraps an `Arc<T: StateBackend>` so it can be boxed as `dyn StateBackend`.
    struct ArcBackend<T: StateBackend>(std::sync::Arc<T>);

    #[async_trait]
    impl<T: StateBackend> StateBackend for ArcBackend<T> {
        async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
            self.0.put(key, value).await
        }
        async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
            self.0.delete(key).await
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            self.0.checkpoint().await
        }
    }
}
