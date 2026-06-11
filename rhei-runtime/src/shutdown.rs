//! Graceful shutdown coordination.
//!
//! Provides a [`ShutdownHandle`] that can be passed to the executor. When a
//! shutdown signal is received, the executor finishes the current batch,
//! checkpoints all operator state, commits source offsets, flushes the sink,
//! and returns cleanly.

use tokio::sync::watch;

/// A cloneable handle that signals graceful shutdown.
///
/// Create one with [`shutdown_signal`], which installs a `SIGINT`/`SIGTERM`
/// handler. Alternatively, use [`ShutdownHandle::new`] for manual control
/// (useful in tests).
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    rx: watch::Receiver<bool>,
}

/// A trigger that can be used to initiate shutdown manually.
#[derive(Debug)]
pub struct ShutdownTrigger {
    tx: watch::Sender<bool>,
}

impl ShutdownTrigger {
    /// Signal shutdown.
    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

impl ShutdownHandle {
    /// Create a new shutdown handle and its corresponding trigger.
    pub fn new() -> (Self, ShutdownTrigger) {
        let (tx, rx) = watch::channel(false);
        (Self { rx }, ShutdownTrigger { tx })
    }

    /// Returns `true` if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        *self.rx.borrow()
    }

    /// Returns a new `watch::Receiver` that can be used with `tokio::select!`
    /// to await shutdown notification.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.rx.clone()
    }
}

/// Install signal handlers for `SIGINT` and `SIGTERM` and return a
/// [`ShutdownHandle`] that becomes active when either signal is received.
///
/// This should be called once at process startup.
pub fn shutdown_signal() -> ShutdownHandle {
    let (tx, rx) = watch::channel(false);

    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            #[allow(clippy::expect_used)]
            // invariant: SIGTERM handler can always be installed on unix
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

            tokio::select! {
                _ = ctrl_c => {},
                _ = sigterm.recv() => {},
            }
        }

        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }

        tracing::info!("shutdown signal received, draining pipeline...");
        let _ = tx.send(true);
    });

    ShutdownHandle { rx }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_starts_not_shutdown() {
        let (handle, _trigger) = ShutdownHandle::new();
        assert!(!handle.is_shutdown());
    }

    #[test]
    fn trigger_sets_shutdown() {
        let (handle, trigger) = ShutdownHandle::new();
        trigger.shutdown();
        assert!(handle.is_shutdown());
    }

    #[test]
    fn shutdown_is_visible_across_clones() {
        let (handle, trigger) = ShutdownHandle::new();
        let clone = handle.clone();
        trigger.shutdown();
        assert!(handle.is_shutdown());
        assert!(clone.is_shutdown());
    }

    #[tokio::test]
    async fn subscriber_is_notified_on_shutdown() {
        let (handle, trigger) = ShutdownHandle::new();
        let mut rx = handle.subscribe();
        assert!(!*rx.borrow());

        trigger.shutdown();

        // The subscribed receiver observes the transition exactly once.
        rx.changed().await.expect("sender still alive");
        assert!(*rx.borrow());
    }

    #[test]
    fn shutdown_is_idempotent() {
        let (handle, trigger) = ShutdownHandle::new();
        trigger.shutdown();
        trigger.shutdown();
        assert!(handle.is_shutdown());
    }
}
