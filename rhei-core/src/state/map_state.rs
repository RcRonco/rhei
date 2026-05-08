//! Map-valued typed state wrapper over [`StateContext`].
//!
//! [`MapState`] stores individual key-value pairs under sub-keys
//! `"{prefix}:" ++ bincode(K)` (raw bytes) for efficient per-key access.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::StateContext;

/// A typed map state accessor with per-key storage.
///
/// Each entry is stored under `b"{name}:" ++ bincode(key)` (raw bytes)
/// so individual keys can be read/written without loading the entire map.
///
/// Using raw bincode bytes instead of hex encoding halves the key size,
/// reducing memtable memory usage and backend I/O. The keys are opaque
/// byte slices to the storage layer, so binary content is safe.
///
/// # Breaking change: key encoding format
///
/// Prior versions encoded sub-keys as `"{prefix}:" ++ hex(bincode(K))`.
/// The current encoding uses raw bincode bytes directly:
/// `"{prefix}:" ++ bincode(K)`. This means **existing persisted state from
/// the hex-encoding era is inaccessible** after upgrading -- reads for
/// old keys will miss, and writes will create new keys under the binary
/// encoding. There is no automatic migration.
///
/// **Migration options:**
/// - **Reprocess from source:** If the stream can be replayed (e.g. Kafka
///   with sufficient retention), drop the old state and reprocess.
/// - **Offline migration script:** Read all keys with the old hex encoding,
///   decode them, re-encode with raw bincode, and write them back.
/// - **Dual-read fallback:** Not currently implemented. If needed, a
///   wrapper could try the binary key first and fall back to hex on miss.
pub struct MapState<'a, K, V> {
    ctx: &'a mut StateContext,
    /// Pre-computed prefix bytes: `b"{name}:"`.
    prefix_bytes: Vec<u8>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> fmt::Debug for MapState<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapState")
            .field("prefix_bytes_len", &self.prefix_bytes.len())
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> MapState<'a, K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `MapState` with the given name prefix.
    pub fn new(ctx: &'a mut StateContext, name: &str) -> Self {
        let mut prefix_bytes = Vec::with_capacity(name.len() + 1);
        prefix_bytes.extend_from_slice(name.as_bytes());
        prefix_bytes.push(b':');
        Self {
            ctx,
            prefix_bytes,
            _phantom: PhantomData,
        }
    }

    /// Build the full state key for a map entry.
    ///
    /// Format: `prefix_bytes ++ bincode(key)` (raw bytes, no hex encoding).
    fn state_key(&self, key: &K) -> anyhow::Result<Vec<u8>> {
        let key_bytes = bincode::serialize(key)?;
        let mut buf = Vec::with_capacity(self.prefix_bytes.len() + key_bytes.len());
        buf.extend_from_slice(&self.prefix_bytes);
        buf.extend_from_slice(&key_bytes);
        Ok(buf)
    }

    /// Retrieves the value for the given key, or `None` if absent.
    pub async fn get(&mut self, key: &K) -> anyhow::Result<Option<V>> {
        let sk = self.state_key(key)?;
        self.ctx.get(&sk).await
    }

    /// Stores a key-value pair.
    pub fn put(&mut self, key: &K, value: &V) -> anyhow::Result<()> {
        let sk = self.state_key(key)?;
        self.ctx.put(&sk, value)
    }

    /// Removes the given key.
    pub fn remove(&mut self, key: &K) -> anyhow::Result<()> {
        let sk = self.state_key(key)?;
        self.ctx.delete(&sk);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_ms_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let mut ctx = test_ctx("roundtrip");
        {
            let mut ms = MapState::<String, u64>::new(&mut ctx, "scores");
            ms.put(&"alice".to_string(), &100).unwrap();
            ms.put(&"bob".to_string(), &200).unwrap();
            assert_eq!(ms.get(&"alice".to_string()).await.unwrap(), Some(100));
            assert_eq!(ms.get(&"bob".to_string()).await.unwrap(), Some(200));
        }
    }

    #[tokio::test]
    async fn get_returns_none_when_absent() {
        let mut ctx = test_ctx("absent");
        let mut ms = MapState::<String, u64>::new(&mut ctx, "m");
        assert_eq!(ms.get(&"missing".to_string()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn remove_deletes_key() {
        let mut ctx = test_ctx("remove");
        {
            let mut ms = MapState::<String, u64>::new(&mut ctx, "m");
            ms.put(&"key".to_string(), &42).unwrap();
            assert_eq!(ms.get(&"key".to_string()).await.unwrap(), Some(42));
            ms.remove(&"key".to_string()).unwrap();
            assert_eq!(ms.get(&"key".to_string()).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn checkpoint_persistence() {
        let path = std::env::temp_dir().join(format!("rhei_ms_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let mut ms = MapState::<String, u64>::new(&mut ctx, "m");
            ms.put(&"a".to_string(), &1).unwrap();
            ms.put(&"b".to_string(), &2).unwrap();
            drop(ms);
            ctx.checkpoint().await.unwrap();
        }

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        let mut ms = MapState::<String, u64>::new(&mut ctx, "m");
        assert_eq!(ms.get(&"a".to_string()).await.unwrap(), Some(1));
        assert_eq!(ms.get(&"b".to_string()).await.unwrap(), Some(2));

        let _ = std::fs::remove_file(&path);
    }
}
