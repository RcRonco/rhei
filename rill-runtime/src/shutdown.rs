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
