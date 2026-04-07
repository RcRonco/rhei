//! List-valued typed state wrapper over [`StateContext`].
//!
//! [`ListState`] stores a `Vec<V>` as a single bincode-encoded value
//! under a named key.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::StateContext;

/// A typed list state accessor.
///
/// The entire list is stored as one bincode-encoded `Vec<V>` under `"{name}"`.
pub struct ListState<'a, V> {
    ctx: &'a mut StateContext,
    key: Vec<u8>,
    _phantom: PhantomData<V>,
}

impl<V> fmt::Debug for ListState<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListState")
            .field("key", &String::from_utf8_lossy(&self.key))
            .finish_non_exhaustive()
    }
}

impl<'a, V> ListState<'a, V>
where
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `ListState` with the given name.
    pub fn new(ctx: &'a mut StateContext, name: &str) -> Self {
        Self {
            ctx,
            key: name.as_bytes().to_vec(),
            _phantom: PhantomData,
        }
    }

    /// Retrieves the stored list, or an empty vec if absent.
    pub async fn get(&mut self) -> anyhow::Result<Vec<V>> {
        Ok(self.ctx.get::<Vec<V>>(&self.key).await?.unwrap_or_default())
    }

    /// Appends a single value to the list.
    pub async fn append(&mut self, value: &V) -> anyhow::Result<()> {
        let mut list = self.get().await?;
        list.push(bincode::deserialize(&bincode::serialize(value)?)?);
        self.ctx.put(&self.key, &list)?;
        Ok(())
    }

    /// Appends all values from a slice to the list.
    pub async fn append_all(&mut self, values: &[V]) -> anyhow::Result<()>
    where
        V: Clone,
    {
        let mut list = self.get().await?;
        list.extend(values.iter().cloned());
        self.ctx.put(&self.key, &list)?;
        Ok(())
    }

    /// Clears the list.
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
        let path = std::env::temp_dir().join(format!("rhei_ls_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn append_and_get() {
        let mut ctx = test_ctx("append");
        {
            let mut ls = ListState::<u64>::new(&mut ctx, "nums");
            ls.append(&1).await.unwrap();
            ls.append(&2).await.unwrap();
            ls.append(&3).await.unwrap();
            assert_eq!(ls.get().await.unwrap(), vec![1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn append_all_extends() {
        let mut ctx = test_ctx("append_all");
        {
            let mut ls = ListState::<String>::new(&mut ctx, "words");
            ls.append(&"hello".to_string()).await.unwrap();
            ls.append_all(&["world".to_string(), "!".to_string()])
                .await
                .unwrap();
            assert_eq!(
                ls.get().await.unwrap(),
                vec!["hello".to_string(), "world".to_string(), "!".to_string()]
            );
        }
    }

    #[tokio::test]
    async fn get_returns_empty_when_absent() {
        let mut ctx = test_ctx("absent");
        let mut ls = ListState::<u64>::new(&mut ctx, "missing");
        assert!(ls.get().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn clear_removes_list() {
        let mut ctx = test_ctx("clear");
        {
            let mut ls = ListState::<u64>::new(&mut ctx, "nums");
            ls.append(&42).await.unwrap();
            ls.clear();
            assert!(ls.get().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn checkpoint_persistence() {
        let path = std::env::temp_dir().join(format!("rhei_ls_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let mut ls = ListState::<u64>::new(&mut ctx, "nums");
            ls.append(&10).await.unwrap();
            ls.append(&20).await.unwrap();
            drop(ls);
            ctx.checkpoint().await.unwrap();
        }

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let mut ctx = StateContext::new(Box::new(backend));
        let mut ls = ListState::<u64>::new(&mut ctx, "nums");
        assert_eq!(ls.get().await.unwrap(), vec![10, 20]);

        let _ = std::fs::remove_file(&path);
    }
}
