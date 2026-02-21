use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rill_core::traits::{Sink, Source};

use crate::dataflow::{AnyItem, ErasedSource};
use crate::shutdown::ShutdownHandle;

/// Bridges an async `Source` into a `tokio::sync::mpsc::Receiver` for use in Timely.
///
/// Spawns a Tokio task that calls `source.next_batch().await` in a loop,
/// sending each batch via a bounded async channel. The Timely side reads
/// with `try_recv()` (non-blocking). Channel close signals source exhaustion.
pub fn source_bridge<S>(
    mut source: S,
    rt: &tokio::runtime::Handle,
) -> tokio::sync::mpsc::Receiver<Vec<S::Output>>
where
    S: Source + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    rt.spawn(async move {
        while let Some(batch) = source.next_batch().await {
            if tx.send(batch).await.is_err() {
                break; // receiver dropped
            }
        }
        // tx drops here, closing the channel
    });
    rx
}

/// Bridges a `tokio::sync::mpsc::Sender` to an async `Sink`.
///
/// Spawns a Tokio task that reads from the receiver (non-blocking async) and
/// calls `sink.write(item).await`. Calls `sink.flush().await` when the
/// channel closes. The Timely side sends with `blocking_send()`.
pub fn sink_bridge<K>(
    mut sink: K,
    rt: &tokio::runtime::Handle,
) -> tokio::sync::mpsc::Sender<K::Input>
where
    K: Sink + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<K::Input>(16);
    rt.spawn(async move {
        while let Some(item) = rx.recv().await {
            if let Err(e) = sink.write(item).await {
                tracing::error!("sink write error: {e}");
                break;
            }
        }
        if let Err(e) = sink.flush().await {
            tracing::error!("sink flush error: {e}");
        }
    });
    tx
}

/// Bridges a type-erased [`ErasedSource`] into a `tokio::sync::mpsc::Receiver`,
/// also sharing current source offsets via an `Arc<Mutex<HashMap>>`.
///
/// Spawns a Tokio task that calls `source.next_batch().await` in a loop,
/// sending each batch via a bounded async channel. After each batch the
/// source's `current_offsets()` are copied into the shared map. The Timely
/// side reads with `try_recv()` (non-blocking). Channel close signals
/// source exhaustion.
///
/// When a `ShutdownHandle` is provided, the bridge stops reading from the
/// source once shutdown is signalled.
#[allow(clippy::type_complexity)]
pub(crate) fn erased_source_bridge_with_offsets(
    mut source: Box<dyn ErasedSource>,
    rt: &tokio::runtime::Handle,
    shutdown: Option<ShutdownHandle>,
) -> (
    tokio::sync::mpsc::Receiver<Vec<AnyItem>>,
    Arc<Mutex<HashMap<String, String>>>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let shared_offsets: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let offsets_writer = shared_offsets.clone();

    rt.spawn(async move {
        while let Some(batch) = source.next_batch().await {
            // Snapshot offsets after reading each batch.
            let offsets = source.current_offsets();
            if !offsets.is_empty() {
                *offsets_writer.lock().unwrap() = offsets;
            }

            if tx.send(batch).await.is_err() {
                break; // receiver dropped
            }
            if let Some(ref handle) = shutdown
                && handle.is_shutdown()
            {
                tracing::info!("shutdown requested, stopping source bridge");
                break;
            }
        }
        // tx drops here, closing the channel
    });
    (rx, shared_offsets)
}
