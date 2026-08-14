//! Key-group-addressed state namespacing.
//!
//! [`PrefixedBackend`](super::prefixed_backend::PrefixedBackend) namespaces
//! keys by operator, and the runtime historically folded the worker index and
//! process ID into that prefix (`p{pid}/w{idx}/{op}`). That makes durable
//! state a function of the cluster layout: change the worker count and every
//! key lands under a prefix nobody looks up any more.
//!
//! `KeyGroupBackend` addresses state by *key group* instead:
//!
//! ```text
//! kg{group:05}/{operator}/{user_key}
//! ```
//!
//! The group is derived from the user key at access time, so the physical key
//! is identical no matter which worker or process performs the access. After a
//! rescale, the worker that inherits key group 37 issues exactly the reads the
//! previous owner issued, and shared L3 storage serves them. No state
//! migration, no rewrite — just cache warming on the gaining worker.
//!
//! The zero-padded group ID keeps the lexicographic order of physical keys
//! aligned with numeric group order, so an owned range is a contiguous scan in
//! ordered stores like `SlateDB`.

use async_trait::async_trait;
use bytes::Bytes;

use super::backend::{BatchOp, StateBackend};
use crate::cluster::key_group::{KeyGroupId, key_group_for};

/// Width of the zero-padded key group component. Covers the 32768 upper bound.
const KEY_GROUP_WIDTH: usize = 5;

/// Physical key prefix for `key_group` under `operator`: `kg{group:05}/{operator}/`.
///
/// Free-standing counterpart to [`KeyGroupBackend::key_group_prefix`] for
/// callers that have no backend instance.
#[must_use]
pub fn key_group_prefix_for(operator: &str, key_group: KeyGroupId) -> String {
    format!("kg{key_group:0KEY_GROUP_WIDTH$}/{operator}/")
}

/// Physical key under which `key` is stored for `operator`, without needing a
/// backend instance.
///
/// The durable key layout is defined here and nowhere else. Tests and offline
/// tooling that need to locate a key in storage must call this rather than
/// re-deriving the format by hand: a second copy of the layout goes stale
/// silently the moment the layout changes, and the resulting failure looks
/// like data loss rather than a stale test.
#[must_use]
pub fn physical_key_for(operator: &str, max_parallelism: usize, key: &[u8]) -> Vec<u8> {
    let prefix = key_group_prefix_for(operator, key_group_for(key, max_parallelism));
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(key);
    out
}

/// A backend wrapper that namespaces keys by key group and operator.
///
/// Unlike `PrefixedBackend`, the prefix is computed per key rather than fixed
/// at construction, because the key group depends on the key itself.
pub struct KeyGroupBackend {
    operator: String,
    max_parallelism: usize,
    inner: Box<dyn StateBackend>,
}

impl std::fmt::Debug for KeyGroupBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyGroupBackend")
            .field("operator", &self.operator)
            .field("max_parallelism", &self.max_parallelism)
            .finish_non_exhaustive()
    }
}

impl KeyGroupBackend {
    /// Wrap `inner` so every key is stored as `kg{group}/{operator}/{key}`.
    ///
    /// # Errors
    /// Returns an error if `operator` contains a `/` (which would make the
    /// namespace ambiguous) or if `max_parallelism` is out of range.
    pub fn new(
        operator: impl Into<String>,
        max_parallelism: usize,
        inner: Box<dyn StateBackend>,
    ) -> anyhow::Result<Self> {
        let operator = operator.into();
        anyhow::ensure!(
            !operator.contains('/'),
            "operator name must not contain '/': got {operator:?}"
        );
        crate::cluster::key_group::validate_max_parallelism(max_parallelism)?;
        Ok(Self {
            operator,
            max_parallelism,
            inner,
        })
    }

    /// The key group a user key belongs to under this backend's max parallelism.
    #[must_use]
    pub fn key_group_of(&self, key: &[u8]) -> KeyGroupId {
        key_group_for(key, self.max_parallelism)
    }

    /// Physical key prefix for a key group: `kg{group:05}/{operator}/`.
    ///
    /// Exposed so callers can range-scan an owned key group in stores that
    /// support prefix iteration.
    #[must_use]
    pub fn key_group_prefix(&self, key_group: KeyGroupId) -> String {
        key_group_prefix_for(&self.operator, key_group)
    }

    fn physical_key(&self, key: &[u8]) -> Vec<u8> {
        physical_key_for(&self.operator, self.max_parallelism, key)
    }
}

#[async_trait]
impl StateBackend for KeyGroupBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(&self.physical_key(key)).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.inner.put(&self.physical_key(key), value).await
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.inner.delete(&self.physical_key(key)).await
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }

    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        let mapped = ops
            .into_iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => BatchOp::Put {
                    key: self.physical_key(&key),
                    value,
                },
                BatchOp::Delete { key } => BatchOp::Delete {
                    key: self.physical_key(&key),
                },
            })
            .collect();
        self.inner.put_batch(mapped).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;
    use std::sync::Arc;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rhei_kg_backend_{name}_{}", std::process::id()))
    }

    /// Wraps an `Arc<T>` so one backend can be shared by several wrappers.
    struct ArcBackend<T: StateBackend>(Arc<T>);

    #[async_trait]
    impl<T: StateBackend> StateBackend for ArcBackend<T> {
        async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
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
        async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
            self.0.put_batch(ops).await
        }
    }

    #[tokio::test]
    async fn physical_key_is_independent_of_worker_identity() {
        // The point of the whole design: two *different* workers wrapping the
        // same shared store must produce the same physical key for the same
        // user key, so ownership can move without touching stored data.
        let path = temp_path("worker_independent");
        let _ = std::fs::remove_file(&path);
        let shared = Arc::new(LocalBackend::new(path.clone(), None).unwrap());

        let worker_a =
            KeyGroupBackend::new("counter", 128, Box::new(ArcBackend(shared.clone()))).unwrap();
        let worker_b =
            KeyGroupBackend::new("counter", 128, Box::new(ArcBackend(shared.clone()))).unwrap();

        worker_a.put(b"user-1", b"42").await.unwrap();

        // Worker B — a different worker after a rescale — reads the same value.
        assert_eq!(
            worker_b.get(b"user-1").await.unwrap(),
            Some(Bytes::from_static(b"42"))
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn operators_do_not_collide() {
        let path = temp_path("op_collide");
        let _ = std::fs::remove_file(&path);
        let shared = Arc::new(LocalBackend::new(path.clone(), None).unwrap());

        let op_a = KeyGroupBackend::new("a", 128, Box::new(ArcBackend(shared.clone()))).unwrap();
        let op_b = KeyGroupBackend::new("b", 128, Box::new(ArcBackend(shared.clone()))).unwrap();

        op_a.put(b"count", b"1").await.unwrap();
        op_b.put(b"count", b"2").await.unwrap();

        assert_eq!(
            op_a.get(b"count").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(
            op_b.get(b"count").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn physical_key_layout_is_stable() {
        // The on-disk format is a compatibility surface: changing it silently
        // orphans every existing checkpoint.
        let path = temp_path("layout");
        let _ = std::fs::remove_file(&path);
        let shared = Arc::new(LocalBackend::new(path.clone(), None).unwrap());
        let backend =
            KeyGroupBackend::new("myop", 128, Box::new(ArcBackend(shared.clone()))).unwrap();

        backend.put(b"key", b"val").await.unwrap();

        let kg = key_group_for(b"key", 128);
        let expected = format!("kg{kg:05}/myop/key");
        assert_eq!(
            shared.get(expected.as_bytes()).await.unwrap(),
            Some(Bytes::from_static(b"val")),
            "expected physical key {expected}"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn physical_key_for_matches_what_the_backend_writes() {
        // `physical_key_for` exists so external callers (tests, offline tooling)
        // can locate a key without re-deriving the layout. That is only useful
        // if it cannot drift from the backend's own mapping — a stale copy of
        // the layout reads as missing data, not as a stale helper.
        let path = temp_path("free_fn");
        let _ = std::fs::remove_file(&path);
        let shared = Arc::new(LocalBackend::new(path.clone(), None).unwrap());
        let backend =
            KeyGroupBackend::new("myop", 128, Box::new(ArcBackend(shared.clone()))).unwrap();

        for key in [b"key".as_slice(), b"another", b"counts:\"alpha\"", b""] {
            backend.put(key, b"val").await.unwrap();
            let derived = physical_key_for("myop", 128, key);
            assert_eq!(
                shared.get(&derived).await.unwrap(),
                Some(Bytes::from_static(b"val")),
                "physical_key_for disagrees with KeyGroupBackend for key {key:?}"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn delete_and_batch_use_the_same_mapping() {
        let path = temp_path("batch");
        let _ = std::fs::remove_file(&path);
        let shared = Arc::new(LocalBackend::new(path.clone(), None).unwrap());
        let backend =
            KeyGroupBackend::new("op", 128, Box::new(ArcBackend(shared.clone()))).unwrap();

        backend
            .put_batch(vec![
                BatchOp::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                BatchOp::Put {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                },
            ])
            .await
            .unwrap();

        assert_eq!(
            backend.get(b"a").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(
            backend.get(b"b").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );

        backend
            .put_batch(vec![BatchOp::Delete { key: b"a".to_vec() }])
            .await
            .unwrap();
        assert_eq!(backend.get(b"a").await.unwrap(), None);
        assert_eq!(
            backend.get(b"b").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );

        backend.delete(b"b").await.unwrap();
        assert_eq!(backend.get(b"b").await.unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn key_group_prefix_is_zero_padded_for_ordering() {
        let path = temp_path("padding");
        let _ = std::fs::remove_file(&path);
        let backend = KeyGroupBackend::new(
            "op",
            128,
            Box::new(LocalBackend::new(path.clone(), None).unwrap()),
        )
        .unwrap();

        assert_eq!(backend.key_group_prefix(7), "kg00007/op/");
        assert_eq!(backend.key_group_prefix(127), "kg00127/op/");
        // Lexicographic order must match numeric order, so an owned range scans
        // contiguously in an ordered store.
        assert!(backend.key_group_prefix(7) < backend.key_group_prefix(127));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_slash_in_operator_name() {
        let path = temp_path("slash");
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let err = KeyGroupBackend::new("bad/name", 128, Box::new(backend)).unwrap_err();
        assert!(err.to_string().contains("must not contain"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_invalid_max_parallelism() {
        let path = temp_path("bad_maxp");
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        assert!(KeyGroupBackend::new("op", 0, Box::new(backend)).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
