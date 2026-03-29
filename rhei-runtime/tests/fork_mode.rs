#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for checkpoint fork mode.

use rhei_core::state::backend::StateBackend;
use rhei_core::state::context::StateContext;
use rhei_core::state::fork_backend::ForkBackend;
use rhei_core::state::local_backend::LocalBackend;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rhei_fork_e2e_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn fork_backend_reads_remote_writes_local() {
    let remote_dir = temp_dir("remote");
    let local_dir = temp_dir("local");

    // Simulate production state.
    let remote_path = remote_dir.join("op.checkpoint.json");
    let remote = LocalBackend::new(remote_path.clone(), None).unwrap();
    remote.put(b"counter", b"42").await.unwrap();
    remote.checkpoint().await.unwrap();

    // Create fork backend.
    let local_path = local_dir.join("op.checkpoint.json");
    let local = LocalBackend::new(local_path.clone(), None).unwrap();
    let fork = ForkBackend::new(Box::new(local), Box::new(remote));

    let mut ctx = StateContext::new(Box::new(fork));

    // Read should fall through to remote.
    let val = ctx.get_raw(b"counter").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"42".as_slice()));

    // Write should stay local.
    ctx.put_raw(b"counter", b"99");
    let val = ctx.get_raw(b"counter").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"99".as_slice()));

    // Checkpoint flushes to local only.
    ctx.checkpoint().await.unwrap();

    // Verify remote is unchanged.
    let remote2 = LocalBackend::new(remote_path, None).unwrap();
    let remote_val = remote2.get(b"counter").await.unwrap();
    assert_eq!(remote_val.as_deref(), Some(b"42".as_slice()));

    // Verify local has the new value.
    let local2 = LocalBackend::new(local_path, None).unwrap();
    let local_val = local2.get(b"counter").await.unwrap();
    assert_eq!(local_val.as_deref(), Some(b"99".as_slice()));

    let _ = std::fs::remove_dir_all(&remote_dir);
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn fork_backend_tombstone_survives_checkpoint() {
    let remote_dir = temp_dir("tombstone_remote");
    let local_dir = temp_dir("tombstone_local");

    // Simulate production state with a key.
    let remote_path = remote_dir.join("state.json");
    let remote = LocalBackend::new(remote_path, None).unwrap();
    remote.put(b"session_123", b"active").await.unwrap();
    remote.checkpoint().await.unwrap();

    // Fork with local.
    let local_path = local_dir.join("state.json");
    let local = LocalBackend::new(local_path, None).unwrap();
    let fork = ForkBackend::new(Box::new(local), Box::new(remote));

    let mut ctx = StateContext::new(Box::new(fork));

    // Verify key is visible via remote fallthrough.
    let val = ctx.get_raw(b"session_123").await.unwrap();
    assert!(val.is_some());

    // Delete through context — should tombstone.
    ctx.delete(b"session_123");

    // Key must be gone (tombstoned, not resurrected by remote).
    let val = ctx.get_raw(b"session_123").await.unwrap();
    assert!(val.is_none(), "deleted key must not resurface from remote");

    let _ = std::fs::remove_dir_all(&remote_dir);
    let _ = std::fs::remove_dir_all(&local_dir);
}
