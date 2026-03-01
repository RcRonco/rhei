use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rhei_core::traits::{Sink, Source};

use crate::dataflow::{AnyItem, ErasedSource};
use crate::shutdown::ShutdownHandle;

/// Bridges an async `Source` into a `flume::Receiver` for use in Timely.
///
/// Spawns a Tokio task that calls `source.next_batch().await` in a loop,
/// sending each batch via a bounded channel. The Timely side reads
/// with `try_recv()` (non-blocking). Channel close signals source exhaustion.
pub fn source_bridge<S>(
    mut source: S,
    rt: &tokio::runtime::Handle,
) -> flume::Receiver<Vec<S::Output>>
where
    S: Source + 'static,
{
    let (tx, rx) = flume::bounded(16);
    rt.spawn(async move {
        while let Some(batch) = source.next_batch().await {
            if tx.send_async(batch).await.is_err() {
                tracing::warn!("source bridge: receiver dropped");
                break; // receiver dropped
            }
        }
        // tx drops here, closing the channel
    });
    rx
}

/// Bridges a `flume::Sender` to an async `Sink`.
///
/// Spawns a Tokio task that reads from the receiver (non-blocking async) and
/// calls `sink.write(item).await`. Calls `sink.flush().await` when the
/// channel closes. The Timely side sends with `send()` (blocking but not
/// requiring a Tokio runtime context).
pub fn sink_bridge<K>(mut sink: K, rt: &tokio::runtime::Handle) -> flume::Sender<K::Input>
where
    K: Sink + 'static,
{
    let (tx, rx) = flume::bounded::<K::Input>(16);
    rt.spawn(async move {
        while let Ok(item) = rx.recv_async().await {
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

/// Creates a future that bridges a type-erased [`ErasedSource`] into a
/// `flume::Sender`, suitable for `spawn_local` on a per-worker runtime.
///
/// Returns a future (for `tokio::task::spawn_local`) that reads batches from
/// the source, updates shared offsets/watermarks, and sends batches through
/// the flume channel. Source I/O is co-located with the Timely worker on
/// the same core.
pub(crate) async fn local_source_bridge(
    mut source: Box<dyn ErasedSource>,
    tx: flume::Sender<Vec<AnyItem>>,
    offsets_writer: Arc<Mutex<HashMap<String, String>>>,
    wm_writer: Arc<AtomicU64>,
    shutdown: Option<ShutdownHandle>,
) {
    while let Some(batch) = source.next_batch().await {
        // Snapshot offsets after reading each batch.
        let offsets = source.current_offsets();
        if !offsets.is_empty() {
            *offsets_writer.lock().unwrap() = offsets;
        }

        // Update source watermark monotonically.
        if let Some(wm) = source.current_watermark() {
            wm_writer.fetch_max(wm, Ordering::Release);
        }

        if tx.send_async(batch).await.is_err() {
            break; // receiver dropped
        }
        if let Some(ref handle) = shutdown
            && handle.is_shutdown()
        {
            tracing::info!("shutdown requested, stopping source bridge");
            return;
        }
    }
    // Source naturally exhausted.
    wm_writer.store(
        crate::executor::Sentinel::SourceExhausted as u64,
        Ordering::Release,
    );
}
