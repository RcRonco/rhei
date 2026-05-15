//! Async-to-sync channel bridges for Timely dataflow integration (batch/Arrow path).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::erased_batch::{ErasedSink, ErasedSource};
use crate::erased_buffer::ErasedBuffer;
use crate::shutdown::ShutdownHandle;

/// Default bounded channel capacity for source and sink bridges.
pub const DEFAULT_CHANNEL_SIZE: usize = 16;

/// A batch produced by the batch source bridge, carrying an `ErasedBuffer`
/// and an optional watermark.
pub(crate) type SourceBatch = (ErasedBuffer, Option<u64>);

/// Bridges a type-erased [`ErasedSource`] into a `flume::Sender`,
/// suitable for `spawn` on the shared Tokio runtime.
///
/// Reads batches from the source and sends `(ErasedBuffer, watermark)` tuples
/// through the flume channel. The watermark is committed by the Timely source
/// operator after emitting items, ensuring it never races ahead of the data.
pub(crate) async fn local_source_bridge(
    mut source: Box<dyn ErasedSource>,
    tx: flume::Sender<SourceBatch>,
    offsets_writer: Arc<Mutex<HashMap<String, String>>>,
    wm_writer: Arc<AtomicU64>,
    shutdown: Option<ShutdownHandle>,
) {
    while let Some(buf) = source.next_batch().await {
        let offsets = source.current_offsets();
        if !offsets.is_empty() {
            *offsets_writer.lock().unwrap_or_else(|e| {
                tracing::warn!("offsets_writer mutex poisoned, recovering: {e}");
                e.into_inner()
            }) = offsets;
        }

        let wm = source.current_watermark();

        if tx.send_async((buf, wm)).await.is_err() {
            break;
        }
        if let Some(ref handle) = shutdown
            && handle.is_shutdown()
        {
            tracing::info!("shutdown requested, stopping batch source bridge");
            return;
        }
    }
    wm_writer.store(
        crate::executor::Sentinel::SourceExhausted as u64,
        Ordering::Release,
    );
}

/// Bridges an [`ErasedSink`] to a `flume::Receiver` for batch writes.
///
/// Reads `ErasedBuffer` batches from the channel and writes them to the sink.
/// Flushes when the channel closes.
pub(crate) async fn sink_drain(
    mut sink: Box<dyn ErasedSink>,
    rx: flume::Receiver<ErasedBuffer>,
) -> anyhow::Result<()> {
    while let Ok(buf) = rx.recv_async().await {
        sink.write_batch(buf).await?;
    }
    sink.flush().await?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct MockSource {
        buffers: Vec<ErasedBuffer>,
        idx: usize,
        watermark: Option<u64>,
    }

    impl MockSource {
        fn new(buffers: Vec<ErasedBuffer>) -> Self {
            Self {
                buffers,
                idx: 0,
                watermark: None,
            }
        }

        #[allow(dead_code)]
        fn with_watermark(mut self, wm: u64) -> Self {
            self.watermark = Some(wm);
            self
        }
    }

    #[async_trait::async_trait]
    impl ErasedSource for MockSource {
        async fn next_batch(&mut self) -> Option<ErasedBuffer> {
            if self.idx < self.buffers.len() {
                let buf = self.buffers[self.idx].clone();
                self.idx += 1;
                Some(buf)
            } else {
                None
            }
        }

        async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn current_offsets(&self) -> HashMap<String, String> {
            HashMap::new()
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
    }

    #[tokio::test]
    async fn batch_bridge_sets_exhausted_sentinel() {
        let source = MockSource::new(vec![]);
        let (tx, rx) = flume::bounded(16);
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let wm = Arc::new(AtomicU64::new(0));

        local_source_bridge(Box::new(source), tx, offsets, wm.clone(), None).await;

        let _ = rx.try_recv();
        assert_eq!(
            wm.load(Ordering::Acquire),
            crate::executor::Sentinel::SourceExhausted as u64
        );
    }
}
