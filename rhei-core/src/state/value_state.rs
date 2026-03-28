//! Single-value typed state wrapper over [`StateContext`].
//!
//! [`ValueState`] stores a single value under a named key, using bincode
//! for serialization (matching `StateContext::get`/`put`).

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::StateContext;

/// A typed single-value state accessor.
///
/// Stores one value under `"{name}"` in the operator's state namespace.
pub struct ValueState<'a, V> {
    ctx: &'a mut StateContext,
    key: Vec<u8>,
    _phantom: PhantomData<V>,
}

impl<V> fmt::Debug for ValueState<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueState")
            .field("key", &String::from_utf8_lossy(&self.key))
            .finish_non_exhaustive()
    }
}

impl<'a, V> ValueState<'a, V>
where
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `ValueState` with the given name.
    pub fn new(ctx: &'a mut StateContext, name: &str) -> Self {
        Self {
            ctx,
            key: name.as_bytes().to_vec(),
            _phantom: PhantomData,
        }
    }

    /// Retrieves the stored value, or `None` if absent.
    pub async fn get(&mut self) -> anyhow::Result<Option<V>> {
        self.ctx.get(&self.key).await
    }

    /// Stores a value.
    pub fn set(&mut self, value: &V) {
        self.ctx.put(&self.key, value);
    }

    /// Removes the stored value.
    pub fn clear(&mut self) {
        self.ctx.delete(&self.key);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_vs_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn set_get_roundtrip() {
        let mut ctx = test_ctx("roundtrip");
        {
            let mut vs = ValueState::<u64>::new(&mut ctx, "counter");
            vs.set(&42);
            assert_eq!(vs.get().await.unwrap(), Some(42));
        }
    }

    #[tokio::test]
    async fn get_returns_none_when_absent() {
        let mut ctx = test_ctx("absent");
        let mut vs = ValueState::<String>::new(&mut ctx, "missing");
        assert_eq!(vs.get().await.unwrap(), None);
    }

    #[tokio::test]
    async fn clear_removes_value() {
        let mut ctx = test_ctx("clear");
        {
            let mut vs = ValueState::<u64>::new(&mut ctx, "val");
            vs.set(&100);
            assert_eq!(vs.get().await.unwrap(), Some(100));
            vs.clear();
            assert_eq!(vs.get().await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn checkpoint_persistence() {
        let path = std::env::temp_dir().join(format!("rhei_vs_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let mut vs = ValueState::<String>::new(&mut ctx, "name");
            vs.set(&"hello".to_string());
            drop(vs);
            ctx.checkpoint().await.unwrap();
        }

        // Reopen
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        let mut vs = ValueState::<String>::new(&mut ctx, "name");
        assert_eq!(vs.get().await.unwrap(), Some("hello".to_string()));

        let _ = std::fs::remove_file(&path);
    }
}
