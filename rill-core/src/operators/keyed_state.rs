//! Typed state wrapper over [`StateContext`].
//!
//! [`KeyedState`] provides ergonomic typed get/put/delete on top of the raw
//! byte-level [`StateContext`] API, using `serde_json` for key and value encoding.

use std::fmt;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::state::context::StateContext;

/// A typed key-value state accessor scoped to a given prefix.
///
/// Wraps [`StateContext`]'s byte API so operators can work with concrete Rust
/// types instead of raw `&[u8]`.
///
/// # Key encoding
///
/// Keys are stored as `"{prefix}:{serde_json::to_string(key)}"`.
///
/// # Value encoding
///
/// Values are stored as `serde_json::to_vec(value)`.
pub struct KeyedState<'a, K, V> {
    ctx: &'a mut StateContext,
    prefix: &'a str,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> fmt::Debug for KeyedState<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedState")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> KeyedState<'a, K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `KeyedState` scoped to the given prefix.
    pub fn new(ctx: &'a mut StateContext, prefix: &'a str) -> Self {
        Self {
            ctx,
            prefix,
            _phantom: PhantomData,
        }
    }

    /// Retrieves the value for the given key, or `None` if absent.
    pub async fn get(&mut self, key: &K) -> anyhow::Result<Option<V>> {
        let encoded_key = format!("{}:{}", self.prefix, serde_json::to_string(key)?);
        match self.ctx.get(encoded_key.as_bytes()).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Stores a key-value pair.
    pub fn put(&mut self, key: &K, value: &V) {
        let encoded_key = format!("{}:{}", self.prefix, serde_json::to_string(key).unwrap());
        let encoded_value = serde_json::to_vec(value).unwrap();
        self.ctx.put(encoded_key.as_bytes(), &encoded_value);
    }

    /// Deletes the given key.
    pub fn delete(&mut self, key: &K) {
        let encoded_key = format!("{}:{}", self.prefix, serde_json::to_string(key).unwrap());
        self.ctx.delete(encoded_key.as_bytes());
    }
}
