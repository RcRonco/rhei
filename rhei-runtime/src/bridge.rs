use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rhei_core::traits::{Sink, Source};

use crate::any_item::AnyItem;
use crate::erased::ErasedSource;
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

/// A batch produced by the source bridge, carrying data and an optional
/// watermark. The watermark is applied by the Timely source operator *after*
/// emitting items into the exchange, preventing the watermark from racing
/// ahead of the data.
pub(crate) type SourceBatch = (Vec<AnyItem>, Option<u64>);

/// Creates a future that bridges a type-erased [`ErasedSource`] into a
/// `flume::Sender`, suitable for `spawn_local` on a per-worker runtime.
///
/// Reads batches from the source and sends `(items, watermark)` tuples through
/// the flume channel. The watermark is NOT written to the shared atomic here —
/// the Timely source operator does that after emitting items, ensuring the
/// watermark never races ahead of the data in the exchange.
///
/// The only direct atomic write is the `SourceExhausted` sentinel when the
/// source is naturally exhausted (returns `None`).
pub(crate) async fn local_source_bridge(
    mut source: Box<dyn ErasedSource>,
    tx: flume::Sender<SourceBatch>,
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

        // Capture watermark to send alongside the batch data. The Timely
        // source operator will commit it to the shared atomic after emitting
        // the items, keeping the watermark in sync with the data flow.
        let wm = source.current_watermark();

        if tx.send_async((batch, wm)).await.is_err() {
            break; // receiver dropped
        }
        if let Some(ref handle) = shutdown
            && handle.is_shutdown()
        {
            tracing::info!("shutdown requested, stopping source bridge");
            return;
        }
    }
    // Source naturally exhausted — written directly because there is no
    // more data to synchronize with.
    wm_writer.store(
        crate::executor::Sentinel::SourceExhausted as u64,
        Ordering::Release,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Sentinel;

    /// Minimal `ErasedSource` for testing: yields pre-loaded batches, tracks offsets/watermarks.
    struct MockErasedSource {
        batches: Vec<Vec<AnyItem>>,
        idx: usize,
        watermark: Option<u64>,
        offsets: HashMap<String, String>,
    }

    impl MockErasedSource {
        fn new(batches: Vec<Vec<AnyItem>>) -> Self {
            Self {
                batches,
                idx: 0,
                watermark: None,
                offsets: HashMap::new(),
            }
        }

        fn with_watermarks(mut self, wm: u64) -> Self {
            self.watermark = Some(wm);
            self
        }

        fn with_offsets(mut self, offsets: HashMap<String, String>) -> Self {
            self.offsets = offsets;
            self
        }
    }

    #[async_trait::async_trait]
    impl ErasedSource for MockErasedSource {
        async fn next_batch(&mut self) -> Option<Vec<AnyItem>> {
            if self.idx < self.batches.len() {
                let batch = self.batches[self.idx].clone();
                self.idx += 1;
                Some(batch)
            } else {
                None
            }
        }

        async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn current_offsets(&self) -> HashMap<String, String> {
            self.offsets.clone()
        }

        async fn restore_offsets(
            &mut self,
            _offsets: &HashMap<String, String>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn partition_count(&self) -> Option<usize> {
            None
        }

        fn create_partition_source(&self, _assigned: &[usize]) -> Option<Box<dyn ErasedSource>> {
            None
        }

        fn current_watermark(&self) -> Option<u64> {
            self.watermark
        }

        fn register_output_type(&self) {}
    }

    #[tokio::test]
    async fn local_source_bridge_delivers_all_batches() {
        let batches = vec![
            vec![AnyItem::new(1i32), AnyItem::new(2i32)],
            vec![AnyItem::new(3i32)],
        ];
        let source = MockErasedSource::new(batches);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        local_source_bridge(Box::new(source), tx, offsets, wm.clone(), None).await;

        // Collect all received batches.
        let mut received = Vec::new();
        while let Ok((batch, _wm)) = rx.try_recv() {
            received.push(batch);
        }
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].len(), 2);
        assert_eq!(received[1].len(), 1);
        assert_eq!(received[0][0].clone().downcast::<i32>(), 1);
        assert_eq!(received[1][0].clone().downcast::<i32>(), 3);
    }

    #[tokio::test]
    async fn local_source_bridge_sets_exhausted_sentinel() {
        let source = MockErasedSource::new(vec![vec![AnyItem::new(42i32)]]);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        local_source_bridge(Box::new(source), tx, offsets, wm.clone(), None).await;

        // Drain the data batch.
        let _ = rx.try_recv();
        // Watermark should be set to SourceExhausted sentinel.
        assert_eq!(wm.load(Ordering::Acquire), Sentinel::SourceExhausted as u64);
    }

    #[tokio::test]
    async fn local_source_bridge_updates_offsets() {
        let offsets_data: HashMap<String, String> =
            [("p0".into(), "100".into())].into_iter().collect();
        let source =
            MockErasedSource::new(vec![vec![AnyItem::new(1i32)]]).with_offsets(offsets_data);
        let (tx, rx) = flume::bounded(16);
        let shared_offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        local_source_bridge(Box::new(source), tx, shared_offsets.clone(), wm, None).await;

        let _ = rx.try_recv();
        let offsets = shared_offsets.lock().unwrap();
        assert_eq!(offsets.get("p0").map(String::as_str), Some("100"));
    }

    #[tokio::test]
    async fn local_source_bridge_sends_watermark_with_batch() {
        let source = MockErasedSource::new(vec![vec![AnyItem::new(1i32)]]).with_watermarks(5000);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        local_source_bridge(Box::new(source), tx, offsets, wm.clone(), None).await;

        let (batch, batch_wm) = rx.try_recv().unwrap();
        assert_eq!(batch.len(), 1);
        // Watermark is sent alongside the batch, not written to the atomic.
        assert_eq!(batch_wm, Some(5000));
        // The shared atomic is NOT updated by the bridge (only SourceExhausted is).
        assert_eq!(wm.load(Ordering::Acquire), Sentinel::SourceExhausted as u64);
    }

    #[tokio::test]
    async fn local_source_bridge_stops_on_shutdown() {
        // Source that would produce 3 batches, but we shut down after the first.
        let source = MockErasedSource::new(vec![
            vec![AnyItem::new(1i32)],
            vec![AnyItem::new(2i32)],
            vec![AnyItem::new(3i32)],
        ]);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        let (handle, trigger) = ShutdownHandle::new();
        // Signal shutdown before the bridge starts — it should stop after the
        // first batch (shutdown is checked after each batch send).
        trigger.shutdown();

        local_source_bridge(Box::new(source), tx, offsets, wm.clone(), Some(handle)).await;

        let mut received = Vec::new();
        while let Ok((batch, _wm)) = rx.try_recv() {
            received.push(batch);
        }
        // Only the first batch should have been delivered before shutdown was detected.
        assert_eq!(received.len(), 1);
        // Watermark should NOT be set to SourceExhausted (shutdown is not exhaustion).
        assert_ne!(wm.load(Ordering::Acquire), Sentinel::SourceExhausted as u64);
    }

    #[tokio::test]
    async fn local_source_bridge_stops_when_receiver_dropped() {
        let source =
            MockErasedSource::new(vec![vec![AnyItem::new(1i32)], vec![AnyItem::new(2i32)]]);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        // Drop the receiver before the bridge runs.
        drop(rx);

        // Should complete without panic — the send_async will fail and break.
        local_source_bridge(Box::new(source), tx, offsets, wm, None).await;
    }

    #[tokio::test]
    async fn flume_source_bridge_delivers_batches() {
        let source = rhei_core::connectors::vec_source::VecSource::new(vec![10i32, 20, 30]);
        let rt = tokio::runtime::Handle::current();
        let rx = source_bridge(source, &rt);

        let mut all_items = Vec::new();
        while let Ok(batch) = rx.recv_async().await {
            all_items.extend(batch);
        }
        assert_eq!(all_items, vec![10, 20, 30]);
    }

    /// A simple collecting sink for testing.
    struct TestCollectSink {
        collected: Arc<Mutex<Vec<i32>>>,
    }

    #[async_trait::async_trait]
    impl rhei_core::traits::Sink for TestCollectSink {
        type Input = i32;
        async fn write(&mut self, input: i32) -> anyhow::Result<()> {
            self.collected.lock().unwrap().push(input);
            Ok(())
        }
    }

    #[tokio::test]
    async fn flume_sink_bridge_receives_items() {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = TestCollectSink {
            collected: collected.clone(),
        };
        let rt = tokio::runtime::Handle::current();
        let tx = sink_bridge(sink, &rt);

        tx.send_async(42i32).await.unwrap();
        tx.send_async(99i32).await.unwrap();
        drop(tx); // close the channel

        // Give the drain task time to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let items = collected.lock().unwrap();
        assert_eq!(*items, vec![42, 99]);
    }
}
