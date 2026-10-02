//! Framed control protocol: pause admission, align watermarks, prepare, commit.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};

use super::{Manifest, Prepared, Recovery};
use crate::bridge::SinkMessage;
use crate::controller::PipelineController;

const PHASE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
enum Message {
    Hello {
        process: usize,
        workers: usize,
        processes: usize,
        max_parallelism: usize,
        topology: serde_json::Value,
        checkpoint: Option<u64>,
    },
    Start {
        attempt: String,
    },
    Request {
        epoch: u64,
    },
    Pause {
        epoch: u64,
    },
    Paused {
        epoch: u64,
        watermark: u64,
        finished: bool,
    },
    Drain {
        epoch: u64,
        watermark: u64,
    },
    Prepared {
        epoch: u64,
        state: Prepared,
    },
    Commit {
        epoch: u64,
        finished: bool,
    },
}

pub(super) struct Session {
    writers: HashMap<usize, OwnedWriteHalf>,
    incoming: tokio::sync::mpsc::UnboundedReceiver<(usize, anyhow::Result<Message>)>,
    readers: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        for task in &self.readers {
            task.abort();
        }
    }
}

impl Session {
    pub async fn connect(
        controller: &PipelineController,
        topology: &serde_json::Value,
        prior: Option<&Manifest>,
    ) -> anyhow::Result<(Self, String)> {
        tokio::time::timeout(
            PHASE_TIMEOUT,
            Self::connect_inner(controller, topology, prior),
        )
        .await
        .map_err(|_| anyhow::anyhow!("recovery startup timed out waiting for all processes"))?
    }

    #[allow(clippy::too_many_lines)] // Paired leader/participant startup handshake.
    async fn connect_inner(
        controller: &PipelineController,
        topology: &serde_json::Value,
        prior: Option<&Manifest>,
    ) -> anyhow::Result<(Self, String)> {
        let pid = controller.process_id().unwrap_or(0);
        let processes = controller.total_workers() / controller.workers;
        let mut sockets = HashMap::new();
        let attempt;
        if pid == 0 {
            attempt = format!(
                "attempt-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos()
            );
            let path = controller.checkpoint_dir.join(&attempt);
            std::fs::create_dir(&path)?;
            super::write_new(&path.join("identity"), attempt.as_bytes())?;
            std::fs::File::open(&path)?.sync_all()?;
            std::fs::File::open(&controller.checkpoint_dir)?.sync_all()?;
            if processes > 1 {
                let peers = controller
                    .peers
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("recovery requires static peers"))?;
                let port = crate::checkpoint_coord::coordination_port(peers);
                let listener =
                    TcpListener::bind(crate::checkpoint_coord::coordinator_bind_addr(port)).await?;
                while sockets.len() < processes - 1 {
                    let (mut socket, _) = listener.accept().await?;
                    let Message::Hello {
                        process,
                        workers,
                        processes: count,
                        max_parallelism,
                        topology: remote_topology,
                        checkpoint,
                    } = read(&mut socket).await?
                    else {
                        anyhow::bail!("expected recovery participant handshake");
                    };
                    anyhow::ensure!(
                        process > 0 && process < processes && !sockets.contains_key(&process),
                        "unexpected or duplicate recovery participant"
                    );
                    anyhow::ensure!(
                        workers == controller.workers
                            && count == processes
                            && max_parallelism == controller.max_parallelism()
                            && &remote_topology == topology
                            && checkpoint == prior.map(|m| m.epoch),
                        "participants disagree on topology or committed checkpoint"
                    );
                    sockets.insert(process, socket);
                }
                for socket in sockets.values_mut() {
                    write(
                        socket,
                        &Message::Start {
                            attempt: attempt.clone(),
                        },
                    )
                    .await?;
                }
            }
        } else {
            let peers = controller
                .peers
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("recovery requires static peers"))?;
            let port = crate::checkpoint_coord::coordination_port(peers);
            let address = crate::checkpoint_coord::coordinator_connect_addr(peers, port);
            let mut socket = loop {
                match TcpStream::connect(&address).await {
                    Ok(socket) => break socket,
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            };
            write(
                &mut socket,
                &Message::Hello {
                    process: pid,
                    workers: controller.workers,
                    processes,
                    max_parallelism: controller.max_parallelism(),
                    topology: topology.clone(),
                    checkpoint: prior.map(|m| m.epoch),
                },
            )
            .await?;
            let Message::Start { attempt: name } = read(&mut socket).await? else {
                anyhow::bail!("expected recovery attempt identity");
            };
            anyhow::ensure!(
                std::path::Path::new(&name).components().count() == 1,
                "invalid recovery attempt path"
            );
            // This is deliberately required even with object-store state: the
            // publication directory must be one shared, durable filesystem.
            anyhow::ensure!(
                std::fs::read(controller.checkpoint_dir.join(&name).join("identity"))?
                    == name.as_bytes(),
                "processes must share the recovery publication directory"
            );
            attempt = name;
            sockets.insert(0, socket);
        }
        let (tx, incoming) = tokio::sync::mpsc::unbounded_channel();
        let mut writers = HashMap::new();
        let mut readers = Vec::new();
        for (pid, socket) in sockets {
            let (mut reader, writer) = socket.into_split();
            writers.insert(pid, writer);
            let tx = tx.clone();
            readers.push(tokio::spawn(async move {
                loop {
                    let message = read(&mut reader).await;
                    let failed = message.is_err();
                    if tx.send((pid, message)).is_err() || failed {
                        break;
                    }
                }
            }));
        }
        Ok((
            Self {
                writers,
                incoming,
                readers,
            },
            attempt,
        ))
    }

    async fn broadcast(&mut self, message: &Message) -> anyhow::Result<()> {
        for writer in self.writers.values_mut() {
            write(writer, message).await?;
        }
        Ok(())
    }

    async fn event(&mut self, recovery: &Recovery) -> anyhow::Result<Option<(usize, Message)>> {
        recovery.result()?;
        tokio::select! {
            () = recovery.changed.notified() => { recovery.result()?; Ok(None) }
            event = self.incoming.recv(), if !self.writers.is_empty() => {
                let (pid, message) = event.ok_or_else(|| anyhow::anyhow!("recovery control channel closed"))?;
                Ok(Some((pid, message?)))
            }
        }
    }

    pub async fn drive(
        &mut self,
        recovery: &Recovery,
        sinks: &[flume::Sender<SinkMessage>],
    ) -> anyhow::Result<u64> {
        loop {
            let epoch = recovery.lock().epoch;
            let finished = tokio::time::timeout(PHASE_TIMEOUT, async {
                if recovery.process_id == 0 {
                    self.leader_epoch(recovery, sinks, epoch).await
                } else {
                    self.participant_epoch(recovery, sinks, epoch).await
                }
            })
            .await
            .map_err(|_| {
                anyhow::anyhow!("recovery epoch {epoch} timed out; aborting the whole attempt")
            })??;
            recovery.commit(epoch, finished)?;
            if finished {
                return Ok(epoch);
            }
        }
    }

    async fn leader_epoch(
        &mut self,
        recovery: &Recovery,
        sinks: &[flume::Sender<SinkMessage>],
        epoch: u64,
    ) -> anyhow::Result<bool> {
        // A timer supplies progress for idle sources and low-volume streams.
        let timer = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(timer);
        while !recovery.lock().draining {
            tokio::select! {
                () = &mut timer => break,
                event = self.event(recovery) => {
                    if let Some((_, message)) = event? {
                        anyhow::ensure!(matches!(message, Message::Request { epoch: e } if e == epoch), "unexpected recovery request epoch");
                        break;
                    }
                }
            }
        }
        recovery.lock().draining = true;
        self.broadcast(&Message::Pause { epoch }).await?;
        let mut paused = BTreeMap::new();
        while paused.len() != recovery.processes {
            {
                let p = recovery.lock();
                if p.paused.len() == p.registered.len() {
                    paused.insert(
                        0,
                        (
                            p.sources
                                .values()
                                .map(|s| s.watermark)
                                .min()
                                .unwrap_or(u64::MAX),
                            p.ended.len() == p.registered.len(),
                        ),
                    );
                }
            }
            if paused.len() == recovery.processes {
                break;
            }
            if let Some((pid, message)) = self.event(recovery).await? {
                match message {
                    Message::Paused {
                        epoch: e,
                        watermark,
                        finished,
                    } if e == epoch => {
                        anyhow::ensure!(
                            paused.insert(pid, (watermark, finished)).is_none(),
                            "duplicate pause acknowledgement"
                        );
                    }
                    Message::Request { epoch: e } if e == epoch => {}
                    _ => anyhow::bail!("unexpected pause acknowledgement epoch or phase"),
                }
            }
        }
        let watermark = paused.values().map(|v| v.0).min().unwrap_or(0);
        let finished = paused.values().all(|v| v.1);
        recovery.lock().global_watermark = Some(watermark);
        self.broadcast(&Message::Drain { epoch, watermark }).await?;
        let local = recovery.prepare_snapshot(sinks).await?;
        let mut prepared = BTreeMap::from([(0, local)]);
        while prepared.len() != recovery.processes {
            if let Some((pid, message)) = self.event(recovery).await? {
                accept_prepared(&mut prepared, pid, epoch, recovery.processes, message)?;
            }
        }
        recovery.publish(epoch, prepared)?;
        self.broadcast(&Message::Commit { epoch, finished }).await?;
        Ok(finished)
    }

    async fn participant_epoch(
        &mut self,
        recovery: &Recovery,
        sinks: &[flume::Sender<SinkMessage>],
        epoch: u64,
    ) -> anyhow::Result<bool> {
        let mut requested = false;
        loop {
            if recovery.lock().draining && !requested {
                self.broadcast(&Message::Request { epoch }).await?;
                requested = true;
            }
            if let Some((_, message)) = self.event(recovery).await? {
                anyhow::ensure!(
                    matches!(message, Message::Pause { epoch: e } if e == epoch),
                    "unexpected pause epoch"
                );
                break;
            }
        }
        recovery.lock().draining = true;
        loop {
            let paused = {
                let p = recovery.lock();
                (p.paused.len() == p.registered.len()).then(|| {
                    (
                        p.sources
                            .values()
                            .map(|s| s.watermark)
                            .min()
                            .unwrap_or(u64::MAX),
                        p.ended.len() == p.registered.len(),
                    )
                })
            };
            if let Some((watermark, finished)) = paused {
                self.broadcast(&Message::Paused {
                    epoch,
                    watermark,
                    finished,
                })
                .await?;
                break;
            }
            if self.event(recovery).await?.is_some() {
                anyhow::bail!("unexpected message while pausing sources");
            }
        }
        loop {
            if let Some((_, message)) = self.event(recovery).await? {
                let Message::Drain {
                    epoch: e,
                    watermark,
                } = message
                else {
                    anyhow::bail!("expected drain decision");
                };
                anyhow::ensure!(e == epoch, "wrong drain epoch");
                recovery.lock().global_watermark = Some(watermark);
                break;
            }
        }
        let state = recovery.prepare_snapshot(sinks).await?;
        self.broadcast(&Message::Prepared { epoch, state }).await?;
        loop {
            if let Some((_, message)) = self.event(recovery).await? {
                let Message::Commit { epoch: e, finished } = message else {
                    anyhow::bail!("expected commit decision");
                };
                anyhow::ensure!(e == epoch, "wrong commit epoch");
                return Ok(finished);
            }
        }
    }
}

fn accept_prepared(
    prepared: &mut BTreeMap<usize, Prepared>,
    process: usize,
    epoch: u64,
    processes: usize,
    message: Message,
) -> anyhow::Result<()> {
    let Message::Prepared {
        epoch: received,
        state,
    } = message
    else {
        anyhow::bail!("unexpected snapshot acknowledgement phase");
    };
    anyhow::ensure!(
        received == epoch && process > 0 && process < processes && !prepared.contains_key(&process),
        "stale, duplicate, or unknown snapshot acknowledgement"
    );
    prepared.insert(process, state);
    Ok(())
}

async fn write(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    message: &Message,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(message)?;
    anyhow::ensure!(bytes.len() <= MAX_FRAME, "recovery control frame too large");
    writer.write_u32(u32::try_from(bytes.len())?).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

async fn read(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> anyhow::Result<Message> {
    let length = reader.read_u32().await? as usize;
    anyhow::ensure!(length <= MAX_FRAME, "recovery control frame too large");
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn prepared(epoch: u64) -> Message {
        Message::Prepared {
            epoch,
            state: Prepared {
                sources: BTreeMap::new(),
                source_layout: BTreeMap::new(),
                local_snapshots: BTreeMap::new(),
                slate_snapshot: None,
            },
        }
    }

    #[test]
    fn rejects_stale_duplicate_unknown_and_wrong_phase_acknowledgements() {
        let mut acknowledgements = BTreeMap::new();
        for message in [
            prepared(8),
            prepared(10),
            Message::Paused {
                epoch: 9,
                watermark: 0,
                finished: false,
            },
        ] {
            assert!(accept_prepared(&mut acknowledgements, 1, 9, 3, message).is_err());
            assert!(acknowledgements.is_empty());
        }
        for process in [0, 3] {
            assert!(accept_prepared(&mut acknowledgements, process, 9, 3, prepared(9)).is_err());
        }
        accept_prepared(&mut acknowledgements, 1, 9, 3, prepared(9)).unwrap();
        assert!(accept_prepared(&mut acknowledgements, 1, 9, 3, prepared(9)).is_err());
        assert_eq!(acknowledgements.len(), 1);
        accept_prepared(&mut acknowledgements, 2, 9, 3, prepared(9)).unwrap();
        assert_eq!(acknowledgements.len(), 2);
    }

    #[tokio::test]
    async fn rejects_truncated_and_oversized_control_frames() {
        let oversized = u32::try_from(MAX_FRAME + 1).unwrap().to_be_bytes();
        assert!(read(&mut oversized.as_slice()).await.is_err());
        let truncated = [0, 0, 0, 8, b'{'];
        assert!(read(&mut truncated.as_slice()).await.is_err());
    }
}
