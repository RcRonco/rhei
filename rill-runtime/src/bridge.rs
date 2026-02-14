use rill_core::traits::{Sink, Source};

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
