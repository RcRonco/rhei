//! Typed state wrapper over [`StateContext`].
//!
//! [`KeyedState`] provides ergonomic typed get/put/delete on top of the raw
//! byte-level [`StateContext`] API. By default it uses JSON encoding for both
//! keys and values, which is human-readable and easy to debug.
//!
//! For performance-sensitive operators, switch to [`BincodeEncoder`] which
//! produces compact binary encodings:
//!
//! ```ignore
//! let state = KeyedState::<u64, MyStruct, BincodeEncoder>::with_encoder(
//!     ctx, "counts", BincodeEncoder,
//! );
//! ```

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::state::context::StateContext;

// ── Encoder trait ───────────────────────────────────────────────────

/// Trait for encoding keys and values to/from bytes in [`KeyedState`].
///
/// Two built-in implementations are provided:
/// - [`JsonEncoder`] (default) — human-readable JSON encoding, easy to inspect
/// - [`BincodeEncoder`] — compact binary encoding, better throughput
///
/// Implement this trait to use a custom serialization format (e.g. `MessagePack`,
/// CBOR, `Protobuf`).
pub trait KeyEncoder: Send + Sync + fmt::Debug {
    /// Encode a key to a string suitable for use as a state key suffix.
    fn encode_key<K: Serialize>(&self, key: &K) -> anyhow::Result<String>;

    /// Encode a value to bytes for storage.
    fn encode_value<V: Serialize>(&self, value: &V) -> anyhow::Result<Vec<u8>>;

    /// Decode a value from bytes.
    fn decode_value<V: DeserializeOwned>(&self, bytes: &[u8]) -> anyhow::Result<V>;
}

// ── JSON encoder (default) ──────────────────────────────────────────

/// JSON encoder for [`KeyedState`]. This is the default.
///
/// Keys are stored as `"{prefix}:{json_key}"`.
/// Values are stored as JSON bytes.
///
/// Advantages: human-readable state, easy debugging with `cat` or `jq`.
/// Disadvantages: larger storage footprint, slower than binary formats.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEncoder;

impl KeyEncoder for JsonEncoder {
    fn encode_key<K: Serialize>(&self, key: &K) -> anyhow::Result<String> {
        Ok(serde_json::to_string(key)?)
    }

    fn encode_value<V: Serialize>(&self, value: &V) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode_value<V: DeserializeOwned>(&self, bytes: &[u8]) -> anyhow::Result<V> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

// ── Bincode encoder ─────────────────────────────────────────────────

/// Compact binary encoder for [`KeyedState`].
///
/// Keys are hex-encoded bincode bytes: `"{prefix}:{hex(bincode(key))}"`.
/// Values are raw bincode bytes.
///
/// Advantages: ~2-5x smaller than JSON, faster serialization.
/// Disadvantages: not human-readable.
///
/// # Example
///
/// ```ignore
/// let state = KeyedState::<u64, Stats, BincodeEncoder>::with_encoder(
///     ctx, "stats", BincodeEncoder,
/// );
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct BincodeEncoder;

impl KeyEncoder for BincodeEncoder {
    fn encode_key<K: Serialize>(&self, key: &K) -> anyhow::Result<String> {
        let bytes = bincode::serialize(key)?;
        Ok(hex::encode(bytes))
    }

    fn encode_value<V: Serialize>(&self, value: &V) -> anyhow::Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    fn decode_value<V: DeserializeOwned>(&self, bytes: &[u8]) -> anyhow::Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }
}

// ── KeyedState ──────────────────────────────────────────────────────

/// A typed key-value state accessor scoped to a given prefix.
///
/// Wraps [`StateContext`]'s byte API so operators can work with concrete Rust
/// types instead of raw `&[u8]`.
///
/// # Encoder selection
///
/// The default encoder is [`JsonEncoder`] (backward-compatible with existing
/// state). For better performance, use [`BincodeEncoder`]:
///
/// ```ignore
/// // JSON (default, backward-compatible):
/// let state = KeyedState::<String, u64>::new(ctx, "counts");
///
/// // Bincode (faster, smaller):
/// let state = KeyedState::<String, u64, BincodeEncoder>::with_encoder(
///     ctx, "counts", BincodeEncoder,
/// );
/// ```
pub struct KeyedState<'a, K, V, E = JsonEncoder> {
    ctx: &'a mut StateContext,
    prefix: &'a str,
    encoder: E,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, E: fmt::Debug> fmt::Debug for KeyedState<'_, K, V, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedState")
            .field("prefix", &self.prefix)
            .field("encoder", &self.encoder)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> KeyedState<'a, K, V, JsonEncoder>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new `KeyedState` with the default JSON encoder.
    ///
    /// This is the simplest constructor and is backward-compatible with
    /// existing state written by previous versions of rhei.
    pub fn new(ctx: &'a mut StateContext, prefix: &'a str) -> Self {
        Self {
            ctx,
            prefix,
            encoder: JsonEncoder,
            _phantom: PhantomData,
        }
    }
}

impl<'a, K, V, E> KeyedState<'a, K, V, E>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
    E: KeyEncoder,
{
    /// Creates a new `KeyedState` with a custom encoder.
    ///
    /// Use [`BincodeEncoder`] for better throughput, or implement
    /// [`KeyEncoder`] for custom formats.
    pub fn with_encoder(ctx: &'a mut StateContext, prefix: &'a str, encoder: E) -> Self {
        Self {
            ctx,
            prefix,
            encoder,
            _phantom: PhantomData,
        }
    }

    /// Retrieves the value for the given key, or `None` if absent.
    pub async fn get(&mut self, key: &K) -> anyhow::Result<Option<V>> {
        let encoded_key = format!("{}:{}", self.prefix, self.encoder.encode_key(key)?);
        match self.ctx.get_raw(encoded_key.as_bytes()).await? {
            Some(bytes) => Ok(Some(self.encoder.decode_value(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Stores a key-value pair.
    pub fn put(&mut self, key: &K, value: &V) -> anyhow::Result<()> {
        let encoded_key = format!("{}:{}", self.prefix, self.encoder.encode_key(key)?);
        let encoded_value = self.encoder.encode_value(value)?;
        self.ctx.put_raw(encoded_key.as_bytes(), &encoded_value);
        Ok(())
    }

    /// Deletes the given key.
    pub fn delete(&mut self, key: &K) -> anyhow::Result<()> {
        let encoded_key = format!("{}:{}", self.prefix, self.encoder.encode_key(key)?);
        self.ctx.delete(encoded_key.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!(
            "rhei_keyed_state_test_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn json_encoder_roundtrip() {
        let mut ctx = test_ctx("json_rt");
        let mut state = KeyedState::<String, u64>::new(&mut ctx, "counts");
        state.put(&"hello".to_string(), &42).unwrap();
        let val = state.get(&"hello".to_string()).await.unwrap();
        assert_eq!(val, Some(42));
    }

    #[tokio::test]
    async fn bincode_encoder_roundtrip() {
        let mut ctx = test_ctx("bincode_rt");
        let mut state = KeyedState::<String, u64, BincodeEncoder>::with_encoder(
            &mut ctx,
            "counts",
            BincodeEncoder,
        );
        state.put(&"world".to_string(), &99).unwrap();
        let val = state.get(&"world".to_string()).await.unwrap();
        assert_eq!(val, Some(99));
    }

    #[tokio::test]
    async fn json_encoder_missing_key() {
        let mut ctx = test_ctx("json_miss");
        let mut state = KeyedState::<String, u64>::new(&mut ctx, "missing");
        let val = state.get(&"nope".to_string()).await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn bincode_encoder_delete() {
        let mut ctx = test_ctx("bincode_del");
        let mut state = KeyedState::<u32, String, BincodeEncoder>::with_encoder(
            &mut ctx,
            "data",
            BincodeEncoder,
        );
        state.put(&1, &"value".to_string()).unwrap();
        assert_eq!(state.get(&1).await.unwrap(), Some("value".to_string()));
        state.delete(&1).unwrap();
        assert_eq!(state.get(&1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn backward_compatible_with_default() {
        // Verify that KeyedState::<K, V>::new() still works
        // without specifying an encoder (backward compatibility).
        let mut ctx = test_ctx("backward");
        let mut state = KeyedState::<i64, Vec<String>>::new(&mut ctx, "lists");
        let val = vec!["a".to_string(), "b".to_string()];
        state.put(&42, &val).unwrap();
        let got = state.get(&42).await.unwrap();
        assert_eq!(got, Some(val));
    }
}
