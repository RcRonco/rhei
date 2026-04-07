//! Map-valued typed state wrapper over [`StateContext`].
//!
//! [`MapState`] stores individual key-value pairs under sub-keys
//! `"{prefix}:{bincode_hex(K)}"` for efficient per-key access.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::StateContext;

/// A typed map state accessor with per-key storage.
///
/// Each entry is stored under `"{name}:{hex(bincode(key))}"` so individual
/// keys can be read/written without loading the entire map.
pub struct MapState<'a, K, V> {
    ctx: &'a mut StateContext,
    prefix: String,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> fmt::Debug for MapState<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapState")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Encode a key to a hex string of its bincode serialization.
fn key_to_hex<K: Serialize>(key: &K) -> anyhow::Result<String> {
    let bytes = bincode::serialize(key)?;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in &bytes {
        use std::fmt::Write;
        write!(hex, "{b:02x}")?;
    }
    Ok(hex)
}

impl<'a, K, V> MapState<'a, K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `MapState` with the given name prefix.
    pub fn new(ctx: &'a mut StateContext, name: &str) -> Self {
        Self {
            ctx,
            prefix: name.to_string(),
            _phantom: PhantomData,
        }
    }

    /// Build the full state key for a map entry.
    fn state_key(&self, key: &K) -> anyhow::Result<Vec<u8>> {
        Ok(format!("{}:{}", self.prefix, key_to_hex(key)?).into_bytes())
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
