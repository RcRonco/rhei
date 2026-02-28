//! Cross-process checkpoint coordination via lightweight TCP.
//!
//! Process 0 runs a [`CheckpointCoordinator`] that collects `Ready` messages
//! from all processes (including itself) and broadcasts `Committed` once
//! every process has flushed state for the epoch.
//!
//! Each process creates a [`CheckpointParticipant`] that sends `Ready` after
//! its local checkpoint completes, then waits for the `Committed` response.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Wire message for checkpoint coordination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoordMessage {
    /// Participant → Coordinator: local checkpoint for `epoch` is complete.
    Ready {
        /// The process that completed its checkpoint.
        process_id: usize,
        /// The epoch that was checkpointed.
        epoch: u64,
    },
    /// Coordinator → Participants: all processes have flushed `epoch`.
    Committed {
        /// The epoch that was globally committed.
        epoch: u64,
    },
}

/// Encode a message as length-prefixed bincode.
#[allow(clippy::cast_possible_truncation)]
pub fn encode_message(msg: &CoordMessage) -> Vec<u8> {
    let payload = bincode::serialize(msg).expect("bincode serialize");
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);
    buf
}

/// Decode a length-prefixed bincode message from a stream.
pub async fn decode_message(stream: &mut TcpStream) -> anyhow::Result<CoordMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(bincode::deserialize(&payload)?)
}

/// Coordinator for cross-process checkpoint synchronization.
///
/// Listens on a TCP port and collects `Ready` messages from all N processes.
/// Once all have reported for an epoch, broadcasts `Committed`.
pub struct CheckpointCoordinator {
    listener: TcpListener,
    n_processes: usize,
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointCoordinator")
            .field("n_processes", &self.n_processes)
            .finish_non_exhaustive()
    }
}

impl CheckpointCoordinator {
    /// Bind to the given address and prepare to coordinate `n_processes`.
    pub async fn bind(addr: &str, n_processes: usize) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(
            addr = %listener.local_addr()?,
            n_processes,
            "checkpoint coordinator listening"
        );
        Ok(Self {
            listener,
            n_processes,
        })
    }

    /// Returns the local address the coordinator is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Run the coordinator loop.
    ///
    /// Accepts connections from N-1 remote participants (process 0 uses
    /// the local participant channel). Collects `Ready` messages from all
    /// processes, then broadcasts `Committed` once every process has
    /// reported for the current round.
    ///
    /// The coordinator is **process-based**, not epoch-based: it tracks
    /// which processes have sent `Ready` (regardless of their individual
    /// epoch numbers). Once all N processes have reported, it commits
    /// using the maximum epoch seen. This avoids deadlocks when the
    /// checkpoint drain loop coalesces to different epochs on different
    /// processes.
    ///
    /// `local_ready_rx` receives epoch numbers from the local (process 0)
    /// checkpoint task. `local_committed_tx` signals the local task that
    /// an epoch has been globally committed.
    pub async fn run(
        self,
        mut local_ready_rx: tokio::sync::mpsc::Receiver<u64>,
        local_committed_tx: tokio::sync::mpsc::Sender<u64>,
    ) -> anyhow::Result<()> {
        let n = self.n_processes;

        // Accept connections from N-1 remote participants.
        let mut streams: Vec<TcpStream> = Vec::with_capacity(n - 1);
        for _ in 0..n - 1 {
            let (stream, addr) = self.listener.accept().await?;
            tracing::debug!(%addr, "checkpoint participant connected");
            streams.push(stream);
        }

        // Per-round: track which processes have reported and the max epoch.
        let mut process_epochs: HashMap<usize, u64> = HashMap::new();

        loop {
            // Wait for either a local or remote Ready message.
            let msg = tokio::select! {
                local_epoch = local_ready_rx.recv() => {
                    match local_epoch {
                        Some(epoch) => CoordMessage::Ready { process_id: 0, epoch },
                        None => break, // local task shut down
                    }
                }
                msg = read_from_any(&mut streams) => {
                    match msg {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!(error = %e, "participant stream error");
                            break;
                        }
                    }
                }
            };

            if let CoordMessage::Ready { process_id, epoch } = msg {
                tracing::debug!(process_id, epoch, "received Ready");
                process_epochs.insert(process_id, epoch);

                if process_epochs.len() >= n {
                    let committed_epoch = *process_epochs.values().max().unwrap();
                    tracing::info!(committed_epoch, "all {} processes ready — committing", n);
                    process_epochs.clear();

                    // Broadcast Committed to remote participants.
                    let committed = encode_message(&CoordMessage::Committed {
                        epoch: committed_epoch,
                    });
                    for stream in &mut streams {
                        if let Err(e) = stream.write_all(&committed).await {
                            tracing::warn!(error = %e, "failed to send Committed");
                        }
                    }

                    // Signal local participant.
                    let _ = local_committed_tx.send(committed_epoch).await;
                }
            }
        }

        Ok(())
    }
}

/// Read a `CoordMessage` from whichever stream has data ready first.
async fn read_from_any(streams: &mut [TcpStream]) -> anyhow::Result<CoordMessage> {
    if streams.is_empty() {
        // No remote participants; hang forever (local-only path).
        std::future::pending::<()>().await;
        unreachable!();
    }

    // Poll all streams for the next message.
    // We use a simple round-robin poll via tokio::select on readable.
    loop {
        for stream in streams.iter_mut() {
            // Check if this stream has data without blocking others.
            let mut len_buf = [0u8; 4];
            match tokio::time::timeout(
                std::time::Duration::from_millis(10),
                stream.read_exact(&mut len_buf),
            )
            .await
            {
                Ok(Ok(_)) => {
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; len];
                    stream.read_exact(&mut payload).await?;
                    return Ok(bincode::deserialize(&payload)?);
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {} // timeout, try next stream
            }
        }
    }
}

/// Participant in the checkpoint coordination protocol.
///
/// Created by non-coordinator processes. Sends `Ready` to the coordinator
/// after local checkpoint, then waits for `Committed`.
pub struct CheckpointParticipant {
    stream: TcpStream,
    process_id: usize,
}

impl std::fmt::Debug for CheckpointParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointParticipant")
            .field("process_id", &self.process_id)
            .finish_non_exhaustive()
    }
}

impl CheckpointParticipant {
    /// Connect to the coordinator at `addr`.
    pub async fn connect(addr: &str, process_id: usize) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        tracing::info!(addr, process_id, "connected to checkpoint coordinator");
        Ok(Self { stream, process_id })
    }

    /// Notify the coordinator that local checkpoint for `epoch` is complete.
    pub async fn send_ready(&mut self, epoch: u64) -> anyhow::Result<()> {
        let msg = CoordMessage::Ready {
            process_id: self.process_id,
            epoch,
        };
        let bytes = encode_message(&msg);
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    /// Wait for the coordinator to confirm that `epoch` is globally committed.
    pub async fn wait_committed(&mut self) -> anyhow::Result<u64> {
        let msg = decode_message(&mut self.stream).await?;
        match msg {
            CoordMessage::Committed { epoch } => Ok(epoch),
            other @ CoordMessage::Ready { .. } => {
                anyhow::bail!("expected Committed, got {other:?}")
            }
        }
    }
}

/// Derive the checkpoint coordination port from the Timely peer address.
///
/// If `RHEI_CHECKPOINT_PORT` is set, uses that. Otherwise takes the first
/// peer's port + 1000.
pub fn coordination_port(peers: &[String]) -> u16 {
    if let Ok(val) = std::env::var("RHEI_CHECKPOINT_PORT")
        && let Ok(port) = val.parse::<u16>()
    {
        return port;
    }
    // Derive from process 0's peer address.
    if let Some(first) = peers.first()
        && let Some(port_str) = first.rsplit(':').next()
        && let Ok(port) = port_str.parse::<u16>()
    {
        return port + 1000;
    }
    9100 // fallback
}

/// Local participant channel pair for process 0.
///
/// Process 0 participates in coordination without TCP — it uses in-memory
/// channels to communicate with the coordinator task running in the same process.
#[derive(Debug)]
pub struct LocalParticipant {
    /// Send epoch to coordinator when local checkpoint completes.
    pub ready_tx: tokio::sync::mpsc::Sender<u64>,
    /// Receive committed epoch from coordinator.
    pub committed_rx: tokio::sync::mpsc::Receiver<u64>,
}

/// Helper to store the coordinator's channel pair for spawning.
#[derive(Debug)]
pub struct CoordinatorChannels {
    /// Receives local ready epochs.
    pub ready_rx: tokio::sync::mpsc::Receiver<u64>,
    /// Sends committed epochs to local participant.
    pub committed_tx: tokio::sync::mpsc::Sender<u64>,
}

/// Create a full coordinator setup (coordinator + channels + local participant).
pub async fn setup_coordinator_full(
    addr: &str,
    n_processes: usize,
) -> anyhow::Result<(CheckpointCoordinator, CoordinatorChannels, LocalParticipant)> {
    let coordinator = CheckpointCoordinator::bind(addr, n_processes).await?;
    let (ready_tx, ready_rx) = tokio::sync::mpsc::channel::<u64>(64);
    let (committed_tx, committed_rx) = tokio::sync::mpsc::channel::<u64>(64);

    Ok((
        coordinator,
        CoordinatorChannels {
            ready_rx,
            committed_tx,
        },
        LocalParticipant {
            ready_tx,
            committed_rx,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_serialization_round_trip() {
        let ready = CoordMessage::Ready {
            process_id: 42,
            epoch: 100,
        };
        let committed = CoordMessage::Committed { epoch: 100 };

        for msg in [&ready, &committed] {
            let bytes = encode_message(msg);
            assert!(bytes.len() > 4); // length prefix + payload

            // Verify length prefix.
            let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
            assert_eq!(len, bytes.len() - 4);

            // Verify payload.
            let decoded: CoordMessage = bincode::deserialize(&bytes[4..]).unwrap();
            assert_eq!(&decoded, msg);
        }
    }

    #[test]
    fn coordination_port_derives_from_peers() {
        // When RHEI_CHECKPOINT_PORT is not set, derives from first peer port + 1000.
        // (This test relies on the env var not being set in CI.)
        if std::env::var("RHEI_CHECKPOINT_PORT").is_ok() {
            return; // Skip if env var is set — can't test derivation.
        }
        let peers = vec!["127.0.0.1:2101".to_string(), "127.0.0.1:2102".to_string()];
        assert_eq!(coordination_port(&peers), 3101); // 2101 + 1000
    }

    #[test]
    fn coordination_port_fallback_no_peers() {
        if std::env::var("RHEI_CHECKPOINT_PORT").is_ok() {
            return;
        }
        let peers: Vec<String> = vec![];
        assert_eq!(coordination_port(&peers), 9100);
    }

    #[tokio::test]
    async fn coordinator_two_local_participants() {
        // Simulate coordination with 2 processes, both "local" (connected via TCP
        // on loopback). This tests the full message flow.
        let (coordinator, channels, local_part) =
            setup_coordinator_full("127.0.0.1:0", 2).await.unwrap();
        let coord_addr = coordinator.local_addr().unwrap().to_string();

        // Spawn coordinator.
        let coord_handle = tokio::spawn(async move {
            coordinator
                .run(channels.ready_rx, channels.committed_tx)
                .await
        });

        // Spawn remote participant (process 1) connected via TCP.
        let addr_clone = coord_addr.clone();
        let remote_handle = tokio::spawn(async move {
            let mut participant = CheckpointParticipant::connect(&addr_clone, 1)
                .await
                .unwrap();
            participant.send_ready(10).await.unwrap();
            let committed_epoch = participant.wait_committed().await.unwrap();
            assert_eq!(committed_epoch, 10);
        });

        // Local participant (process 0) sends ready via channel.
        let mut local_part = local_part;
        local_part.ready_tx.send(10).await.unwrap();
        let committed_epoch = local_part.committed_rx.recv().await.unwrap();
        assert_eq!(committed_epoch, 10);

        // Wait for remote to finish.
        remote_handle.await.unwrap();

        // Shut down coordinator by dropping the ready_tx.
        drop(local_part.ready_tx);
        // Coordinator will exit when local channel closes.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), coord_handle).await;
    }
}
