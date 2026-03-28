use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::backend::StateBackend;

/// Copy-on-write state backend for checkpoint fork mode.
///
/// Wraps a **local** backend (reads + writes) and a **remote** backend
/// (read-only fallback). This enables fork-from-checkpoint semantics:
/// the remote backend holds the base checkpoint state while the local
/// backend accumulates all mutations since the fork.
///
/// # Read path
///
/// 1. Check tombstones — if the key has been deleted, return `None` immediately.
/// 2. Check local backend — if the key exists locally, return it.
/// 3. Fall through to remote backend.
///
/// # Write path
///
/// All writes (`put`, `delete`) go to the local backend only.
/// The remote backend is never modified.
///
/// # Tombstone invariant
///
/// When a key is deleted via this backend, it is added to an in-memory
/// tombstone set. This prevents the key from being resurrected by a
/// subsequent read that falls through to the remote backend.
///
/// A `put` to a tombstoned key clears the tombstone (resurrection),
/// ensuring the new value is visible on the next read.
///
/// # Checkpoint
///
/// Only the local backend is checkpointed. The remote backend is
/// treated as immutable (it represents a prior checkpoint).
pub struct ForkBackend {
    local: Box<dyn StateBackend>,
    remote: Box<dyn StateBackend>,
    /// Keys that have been deleted since the fork.
    ///
    /// Invariant: if `tombstones` contains a key, then `get` for that key
    /// must return `None` regardless of what the remote backend holds.
    tombstones: Mutex<HashSet<Vec<u8>>>,
}

impl std::fmt::Debug for ForkBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkBackend")
            .field(
                "tombstone_count",
                &self
                    .tombstones
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish_non_exhaustive()
    }
}

impl ForkBackend {
    /// Creates a new `ForkBackend` with the given local and remote backends.
    ///
    /// The remote backend is used only for read fallthrough; it is never
    /// mutated by this wrapper.
    pub fn new(local: Box<dyn StateBackend>, remote: Box<dyn StateBackend>) -> Self {
        Self {
            local,
            remote,
            tombstones: Mutex::new(HashSet::new()),
        }
    }
}

#[async_trait]
impl StateBackend for ForkBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        // 1. Tombstone check — deleted keys must not resurface from remote.
        if self
            .tombstones
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key)
        {
            return Ok(None);
        }

        // 2. Local backend — contains all mutations since fork.
        if let Some(val) = self.local.get(key).await? {
            return Ok(Some(val));
        }

        // 3. Remote fallthrough — base checkpoint state.
        self.remote.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        // Clear tombstone first: a put resurrects a previously deleted key.
        // The tombstone must be removed before the local write so that a
        // concurrent reader does not see the tombstone after the value lands.
        self.tombstones
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
        self.local.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        // Record tombstone before local delete to prevent a concurrent read
        // from falling through to the remote backend during the delete.
        self.tombstones
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.to_vec());
        self.local.delete(key).await
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        // Only checkpoint the local backend. The remote is immutable.
        self.local.checkpoint().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;
    use bytes::Bytes;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rhei_fork_test_{name}_{}", std::process::id()))
    }

    /// Helper: create a `LocalBackend` at a fresh temp path, cleaning up any prior file.
    fn fresh_local(name: &str) -> (std::path::PathBuf, LocalBackend) {
        let path = temp_path(name);
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        (path, backend)
    }

    #[tokio::test]
    async fn reads_fall_through_to_remote() {
        let (local_path, local) = fresh_local("fallthrough_local");
        let (remote_path, remote) = fresh_local("fallthrough_remote");

        // Seed remote with a key.
        remote.put(b"key", b"remote_val").await.unwrap();

        let fork = ForkBackend::new(Box::new(local), Box::new(remote));

        // Local has nothing — should fall through to remote.
        let val = fork.get(b"key").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"remote_val")));

        let _ = std::fs::remove_file(&local_path);
        let _ = std::fs::remove_file(&remote_path);
    }

    #[tokio::test]
    async fn local_shadows_remote() {
        let (local_path, local) = fresh_local("shadow_local");
        let (remote_path, remote) = fresh_local("shadow_remote");

        // Both have the same key with different values.
        local.put(b"key", b"local_val").await.unwrap();
        remote.put(b"key", b"remote_val").await.unwrap();

        let fork = ForkBackend::new(Box::new(local), Box::new(remote));

        // Local value takes precedence.
        let val = fork.get(b"key").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"local_val")));

        let _ = std::fs::remove_file(&local_path);
        let _ = std::fs::remove_file(&remote_path);
    }

    #[tokio::test]
    async fn writes_go_to_local_only() {
        let (local_path, local) = fresh_local("write_local");
        let (remote_path, remote) = fresh_local("write_remote");

        let fork = ForkBackend::new(Box::new(local), Box::new(remote));

        // Write through the fork.
        fork.put(b"key", b"val").await.unwrap();
        fork.checkpoint().await.unwrap();

        // Verify via fresh backends reading the checkpointed files.
        let local_check = LocalBackend::new(local_path.clone(), None).unwrap();
        let remote_check = LocalBackend::new(remote_path.clone(), None).unwrap();

        assert_eq!(
            local_check.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"val")),
        );
        assert_eq!(
            remote_check.get(b"key").await.unwrap(),
            None,
            "remote must not be modified by fork writes",
        );

        let _ = std::fs::remove_file(&local_path);
        let _ = std::fs::remove_file(&remote_path);
    }

    #[tokio::test]
    async fn delete_tombstones_prevent_remote_fallthrough() {
        let (local_path, local) = fresh_local("tombstone_local");
        let (remote_path, remote) = fresh_local("tombstone_remote");

        // Remote has a key that we will delete via the fork.
        remote.put(b"key", b"remote_val").await.unwrap();

        let fork = ForkBackend::new(Box::new(local), Box::new(remote));

        // Confirm visible before delete.
        assert_eq!(
            fork.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"remote_val")),
        );

        // Delete via fork — should tombstone the key.
        fork.delete(b"key").await.unwrap();

        // Must not fall through to remote.
        assert_eq!(
            fork.get(b"key").await.unwrap(),
            None,
            "tombstone must prevent remote fallthrough after delete",
        );

        let _ = std::fs::remove_file(&local_path);
        let _ = std::fs::remove_file(&remote_path);
    }

    #[tokio::test]
    async fn put_after_delete_resurrects_key() {
        let (local_path, local) = fresh_local("resurrect_local");
        let (remote_path, remote) = fresh_local("resurrect_remote");

        let fork = ForkBackend::new(Box::new(local), Box::new(remote));

        // Write, then delete, then verify gone.
        fork.put(b"key", b"original").await.unwrap();
        fork.delete(b"key").await.unwrap();
        assert_eq!(
            fork.get(b"key").await.unwrap(),
            None,
            "key should be gone after delete",
        );

        // Put again — should clear the tombstone and make the key visible.
        fork.put(b"key", b"resurrected").await.unwrap();
        assert_eq!(
            fork.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"resurrected")),
            "put after delete must resurrect the key with the new value",
        );

        let _ = std::fs::remove_file(&local_path);
        let _ = std::fs::remove_file(&remote_path);
    }
}
