//! Failure and cancellation boundaries for checkpoint preparation.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use rhei_core::state::backend::{BatchOp, StateBackend};
use rhei_core::state::context::StateContext;
use rhei_core::state::key_group_addressing::StateAddressing;
use rhei_core::state::local_backend::LocalBackend;
use rhei_core::state::memtable::MemTableConfig;
use tokio::sync::Notify;

struct FailingBackend {
    inner: LocalBackend,
    fail_write: AtomicBool,
    fail_barrier: AtomicBool,
    pause_write: AtomicBool,
    entered_write: Notify,
    resume_write: Notify,
}

#[async_trait]
impl StateBackend for FailingBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key).await
    }
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.inner.put(key, value).await
    }
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
    async fn put_batch(&self, ops: Vec<BatchOp>) -> anyhow::Result<()> {
        if self.pause_write.load(Ordering::SeqCst) {
            self.entered_write.notify_one();
            self.resume_write.notified().await;
        }
        if self.fail_write.swap(false, Ordering::SeqCst) {
            // A backend is allowed to partially apply a failed batch.
            if let Some(op) = ops.first() {
                self.inner.put_batch(vec![op.clone()]).await?;
            }
            anyhow::bail!("injected partial write failure");
        }
        self.inner.put_batch(ops).await
    }
    async fn checkpoint(&self) -> anyhow::Result<()> {
        if self.fail_barrier.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected durability failure");
        }
        self.inner.checkpoint().await
    }
}

fn backend(path: std::path::PathBuf) -> Arc<FailingBackend> {
    Arc::new(FailingBackend {
        inner: LocalBackend::new(path, None).unwrap(),
        fail_write: AtomicBool::new(false),
        fail_barrier: AtomicBool::new(false),
        pause_write: AtomicBool::new(false),
        entered_write: Notify::new(),
        resume_write: Notify::new(),
    })
}

fn context(backend: Arc<FailingBackend>, operator: &str, worker: usize) -> StateContext {
    StateContext::new(Box::new(backend))
        .with_addressing(StateAddressing::new(operator, 128, worker).unwrap())
        .with_memtable_config(MemTableConfig {
            max_bytes: 0,
            max_entries: 0,
        })
}

#[tokio::test]
async fn immutable_slate_snapshot_ignores_newer_uncommitted_writes_and_deletes() {
    use rhei_core::state::slatedb_backend::SlateDbBackend;
    let store = Arc::new(object_store::memory::InMemory::new());
    let database = SlateDbBackend::open("root", store).await.unwrap();
    database.put(b"kept", b"committed").await.unwrap();
    database
        .put(b"deleted-later", b"still-committed")
        .await
        .unwrap();
    let checkpoint = database.snapshot().await.unwrap();
    database.put(b"kept", b"uncommitted").await.unwrap();
    database.delete(b"deleted-later").await.unwrap();
    database.put(b"new", b"uncommitted").await.unwrap();
    let restored = database
        .recovery_attempt("attempt-2", Some(&checkpoint))
        .await
        .unwrap();
    assert_eq!(
        restored.get(b"kept").await.unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    assert_eq!(
        restored.get(b"deleted-later").await.unwrap().as_deref(),
        Some(b"still-committed".as_slice())
    );
    assert_eq!(restored.get(b"new").await.unwrap(), None);
    restored.put(b"kept", b"new-attempt").await.unwrap();
    let again = database
        .recovery_attempt("attempt-3", Some(&checkpoint))
        .await
        .unwrap();
    assert_eq!(
        again.get(b"kept").await.unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    again.close().await.unwrap();
    restored.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn partial_write_and_durability_failures_preserve_values_deletes_and_timers() {
    for fail_barrier in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let store = backend(path.clone());
        let mut state = context(store.clone(), "operator", 0);
        state.put_raw(b"deleted", b"old");
        state.timers().register(10, "old".into());
        state.checkpoint().await.unwrap();

        state.put_raw(b"new", b"new-value");
        state.delete(b"deleted");
        state.timers().drain_fired(10);
        state.timers().register(20, "new".into());
        if fail_barrier {
            store.fail_barrier.store(true, Ordering::SeqCst);
        } else {
            store.fail_write.store(true, Ordering::SeqCst);
        }
        assert!(state.checkpoint().await.is_err());
        assert_eq!(
            state.get_raw(b"new").await.unwrap().as_deref(),
            Some(b"new-value".as_slice())
        );
        assert_eq!(state.get_raw(b"deleted").await.unwrap(), None);
        assert!(state.timers().is_dirty());

        state.checkpoint().await.unwrap();
        let mut restored = context(backend(path), "operator", 0);
        assert_eq!(
            restored.get_raw(b"new").await.unwrap().as_deref(),
            Some(b"new-value".as_slice())
        );
        assert_eq!(restored.get_raw(b"deleted").await.unwrap(), None);
        restored.restore_timers().await.unwrap();
        assert_eq!(restored.timers().drain_fired(100), vec![(20, "new".into())]);
    }
}

#[tokio::test]
async fn cancelled_checkpoint_can_retry_without_losing_pending_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let store = backend(path.clone());
    let mut state = context(store.clone(), "operator", 0);
    state.put_raw(b"key", b"value");
    state.timers().register(30, "timer".into());
    store.pause_write.store(true, Ordering::SeqCst);

    tokio::select! {
        result = state.checkpoint() => panic!("checkpoint should be paused: {result:?}"),
        () = store.entered_write.notified() => {}
    }
    store.pause_write.store(false, Ordering::SeqCst);
    state.checkpoint().await.unwrap();

    let mut restored = context(backend(path), "operator", 0);
    assert_eq!(
        restored.get_raw(b"key").await.unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    restored.restore_timers().await.unwrap();
    assert_eq!(
        restored.timers().drain_fired(100),
        vec![(30, "timer".into())]
    );
}

#[tokio::test]
async fn timers_are_isolated_by_operator_and_worker_and_empty_sets_persist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let store = backend(path.clone());
    for (operator, worker, timestamp) in [("a", 0, 10), ("a", 1, 20), ("b", 0, 30)] {
        let mut state = context(store.clone(), operator, worker);
        state.timers().register(timestamp, operator.into());
        state.checkpoint().await.unwrap();
    }
    let store = backend(path.clone());
    for (operator, worker, timestamp) in [("a", 0, 10), ("a", 1, 20), ("b", 0, 30)] {
        let mut restored = context(store.clone(), operator, worker);
        restored.restore_timers().await.unwrap();
        assert_eq!(
            restored.timers().drain_fired(100),
            vec![(timestamp, operator.into())]
        );
        restored.checkpoint().await.unwrap();
    }
    let mut restored = context(backend(path), "a", 0);
    restored.restore_timers().await.unwrap();
    assert!(restored.timers().is_empty());
}
