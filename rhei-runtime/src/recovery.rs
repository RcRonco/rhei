//! Coordinated fixed-topology recovery. Execution epochs are not event time.

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rhei_core::arrow::OperatorContext;
use rhei_core::state::context::StateContext;
use rhei_core::state::key_group_addressing::StateAddressing;
use rhei_core::state::local_backend::LocalBackend;
use rhei_core::state::slatedb_backend::{SlateDbBackend, SlateDbSnapshot};
use serde::{Deserialize, Serialize};

use crate::bridge::{SinkMessage, SourceBatch};
use crate::compiler::CompiledGraph;
use crate::controller::PipelineController;
use crate::erased_buffer::ErasedBuffer;
use crate::shutdown::ShutdownHandle;
use crate::task_manager::TaskManager;
mod coordination;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct SourceProgress {
    pub offsets: std::collections::HashMap<String, String>,
    pub watermark: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    epoch: u64,
    workers: usize,
    max_parallelism: usize,
    topology: serde_json::Value,
    processes: usize,
    participants: BTreeMap<usize, Prepared>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Prepared {
    sources: BTreeMap<String, SourceProgress>,
    source_layout: BTreeMap<usize, Option<usize>>,
    local_snapshots: BTreeMap<String, PathBuf>,
    slate_snapshot: Option<SlateDbSnapshot>,
}

enum Storage {
    Local(BTreeMap<String, Arc<LocalBackend>>),
    Slate(Arc<SlateDbBackend>),
}

struct Progress {
    epoch: u64,
    batches: u64,
    draining: bool,
    done: bool,
    error: Option<String>,
    sources: BTreeMap<String, SourceProgress>,
    registered: HashSet<String>,
    expected_sources: Option<HashSet<String>>,
    source_layout: BTreeMap<usize, Option<usize>>,
    expected_layout: Option<BTreeMap<usize, Option<usize>>>,
    paused: HashSet<String>,
    ended: HashSet<String>,
    ready: HashSet<usize>,
    global_watermark: Option<u64>,
}

pub(crate) enum SourceAction {
    Batch(u64, ErasedBuffer),
    Advance(u64),
    Wait,
    Done,
}

pub(crate) struct Recovery {
    progress: Mutex<Progress>,
    changed: tokio::sync::Notify,
    interval: u64,
    memtable_config: rhei_core::state::memtable::MemTableConfig,
    workers: usize,
    process_id: usize,
    processes: usize,
    session: tokio::sync::Mutex<coordination::Session>,
    max_parallelism: usize,
    topology: serde_json::Value,
    directory: PathBuf,
    attempt: PathBuf,
    storage: Storage,
    // OS lock fences concurrent attempts sharing the publication directory.
    _locks: Vec<File>,
}

impl Recovery {
    #[allow(clippy::too_many_lines)] // Fencing, validation, handshake, then restore must run in order.
    async fn prepare(
        controller: &PipelineController,
        graph: &CompiledGraph,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(
            matches!(controller.error_policy, rhei_core::dlq::ErrorPolicy::Skip),
            "coordinated recovery fails on operator errors; durable DLQ coordination is not yet supported"
        );
        #[cfg(feature = "remote-state")]
        anyhow::ensure!(
            controller.from_checkpoint.is_none(),
            "legacy fork checkpoints cannot be used for coordinated recovery"
        );
        anyhow::ensure!(
            controller.cluster_member_ids().is_empty(),
            "coordinated recovery requires static topology"
        );
        let use_slate = controller.tiered.is_some();
        #[cfg(feature = "remote-state")]
        let use_slate = use_slate || controller.remote_state.is_some();
        let names: HashSet<_> = graph.operator_names.iter().collect();
        anyhow::ensure!(
            names.len() == graph.operator_names.len(),
            "duplicate operator state identity"
        );
        let directory = controller.checkpoint_dir.clone();
        std::fs::create_dir_all(&directory)?;
        let process_id = controller.process_id().unwrap_or(0);
        let processes = controller.total_workers() / controller.workers;
        let mut locks = Vec::new();
        let mut lock_names = vec![format!("recovery-process-{process_id}.lock")];
        if process_id == 0 {
            lock_names.push("recovery.lock".into());
        }
        for name in lock_names {
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(directory.join(name))?;
            lock.try_lock().map_err(|e| {
                anyhow::anyhow!("recovery directory is already owned by another attempt: {e}")
            })?;
            locks.push(lock);
        }
        anyhow::ensure!(
            !directory.join("manifest.json").exists(),
            "legacy checkpoints have no consistent snapshot; use a new recovery directory"
        );
        let prior: Option<Manifest> = match std::fs::read(directory.join("recovery.json")) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let topology = serde_json::to_value(&graph.topology)?;
        if let Some(ref prior) = prior {
            anyhow::ensure!(prior.version == 1, "unsupported recovery manifest version");
            anyhow::ensure!(
                prior.workers == controller.workers
                    && prior.processes == processes
                    && prior.participants.len() == processes
                    && prior.max_parallelism == controller.max_parallelism()
                    && prior.topology == topology,
                "recovery requires the same topology, state identities, and worker count"
            );
            let participant = prior
                .participants
                .get(&process_id)
                .ok_or_else(|| anyhow::anyhow!("missing process snapshot"))?;
            anyhow::ensure!(
                participant.slate_snapshot.is_some() == use_slate,
                "recovery backend changed"
            );
            if !use_slate {
                anyhow::ensure!(
                    participant.local_snapshots.keys().collect::<HashSet<_>>() == names,
                    "checkpoint does not contain exactly the expected operator snapshots"
                );
            }
        }
        let (session, suffix) =
            coordination::Session::connect(controller, &topology, prior.as_ref()).await?;
        let attempt = directory
            .join(&suffix)
            .join(format!("process-{process_id}"));
        std::fs::create_dir(&attempt)?;
        File::open(
            attempt
                .parent()
                .ok_or_else(|| anyhow::anyhow!("missing attempt parent"))?,
        )?
        .sync_all()?;
        let previous = prior.as_ref().and_then(|m| m.participants.get(&process_id));
        let root_backend = controller.tiered.as_ref().map(|t| t.l3.clone());
        #[cfg(feature = "remote-state")]
        let root_backend = if let Some(config) = &controller.remote_state {
            anyhow::ensure!(
                root_backend.is_none(),
                "choose either remote_state or with_tiered_storage"
            );
            Some(Arc::new(
                SlateDbBackend::open(
                    format!("{}/process-{process_id}", config.prefix),
                    config.build_object_store()?,
                )
                .await?,
            ))
        } else {
            root_backend
        };
        let storage = if let Some(root_backend) = root_backend {
            let attempt_backend = root_backend
                .recovery_attempt(
                    &format!("{suffix}-p{process_id}"),
                    previous.and_then(|m| m.slate_snapshot.as_ref()),
                )
                .await?;
            #[cfg(feature = "remote-state")]
            if controller.remote_state.is_some() {
                root_backend.close().await?;
            }
            Storage::Slate(Arc::new(attempt_backend))
        } else {
            let mut backends = BTreeMap::new();
            for (index, name) in graph.operator_names.iter().enumerate() {
                let path = attempt.join(format!("working-{index}.json"));
                let backend = if let Some(previous) = previous {
                    LocalBackend::from_snapshot(
                        &directory.join(&previous.local_snapshots[name]),
                        path,
                    )?
                } else {
                    LocalBackend::new(path, None)?
                };
                backends.insert(name.clone(), Arc::new(backend));
            }
            Storage::Local(backends)
        };
        let epoch = prior.as_ref().map_or(Ok(1), |m| {
            m.epoch
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("checkpoint epoch exhausted"))
        })?;
        anyhow::ensure!(epoch < u64::MAX, "checkpoint epoch exhausted");
        Ok(Arc::new(Self {
            progress: Mutex::new(Progress {
                epoch,
                batches: 0,
                draining: false,
                done: false,
                error: None,
                sources: previous.map_or_else(BTreeMap::new, |m| m.sources.clone()),
                registered: HashSet::new(),
                expected_sources: previous.map(|m| m.sources.keys().cloned().collect()),
                source_layout: BTreeMap::new(),
                expected_layout: previous.map(|m| m.source_layout.clone()),
                paused: HashSet::new(),
                ended: HashSet::new(),
                ready: HashSet::new(),
                global_watermark: None,
            }),
            changed: tokio::sync::Notify::new(),
            interval: controller.checkpoint_interval,
            memtable_config: controller.memtable_config.clone(),
            workers: controller.workers,
            max_parallelism: controller.max_parallelism(),
            topology,
            directory,
            attempt,
            storage,
            process_id,
            processes,
            session: tokio::sync::Mutex::new(session),
            _locks: locks,
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Progress> {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn state(&self, name: &str, worker: usize) -> anyhow::Result<OperatorContext> {
        let state = match &self.storage {
            Storage::Local(backends) => StateContext::new(Box::new(backends[name].clone())),
            Storage::Slate(backend) => StateContext::new(Box::new(backend.clone())),
        };
        Ok(OperatorContext::new(
            state
                .with_memtable_config(self.memtable_config.clone())
                .with_worker_label(worker.to_string())
                .with_addressing(StateAddressing::new(name, self.max_parallelism, worker)?),
        ))
    }

    pub fn register_source(&self, id: &str) -> SourceProgress {
        let mut p = self.lock();
        p.registered.insert(id.to_owned());
        p.sources.entry(id.to_owned()).or_default().clone()
    }

    pub fn validate_sources(&self) -> anyhow::Result<()> {
        let p = self.lock();
        anyhow::ensure!(
            !p.registered.is_empty()
                && p.expected_sources
                    .as_ref()
                    .is_none_or(|ids| *ids == p.registered)
                && p.expected_layout
                    .as_ref()
                    .is_none_or(|layout| *layout == p.source_layout),
            "source assignment changed since the committed checkpoint"
        );
        Ok(())
    }

    pub fn register_layout(&self, id: usize, partitions: Option<usize>) {
        self.lock().source_layout.insert(id, partitions);
    }

    pub fn fail(&self, error: impl std::fmt::Display) {
        let mut p = self.lock();
        p.error.get_or_insert_with(|| error.to_string());
        self.changed.notify_one();
    }

    pub fn result(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.lock().error {
            anyhow::bail!("recovery attempt failed: {error}");
        }
        Ok(())
    }

    /// Cancel async callbacks when another participant fails or a phase stalls.
    pub async fn guard<T>(
        &self,
        future: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let future = tokio::time::timeout(std::time::Duration::from_secs(60), future);
        tokio::pin!(future);
        loop {
            self.result()?;
            tokio::select! {
                result = &mut future => return result.map_err(|_| anyhow::anyhow!("recovery callback timed out"))?,
                () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
            }
        }
    }

    pub fn stopped(&self) -> bool {
        let p = self.lock();
        p.done || p.error.is_some()
    }

    pub fn poll_source(
        &self,
        id: &str,
        rx: &flume::Receiver<SourceBatch>,
        exhausted: bool,
    ) -> SourceAction {
        let mut p = self.lock();
        if p.done || p.error.is_some() {
            return SourceAction::Done;
        }
        if !p.draining {
            match rx.try_recv() {
                Ok((buffer, watermark, offsets)) => {
                    let source = p
                        .sources
                        .get_mut(id)
                        .unwrap_or_else(|| unreachable!("registered source"));
                    source.offsets = offsets;
                    if let Some(watermark) = watermark {
                        source.watermark = source.watermark.max(watermark);
                    }
                    p.batches += 1;
                    if p.batches >= self.interval {
                        p.draining = true;
                        self.changed.notify_one();
                    }
                    return SourceAction::Batch(p.epoch, buffer);
                }
                Err(flume::TryRecvError::Disconnected) => {
                    p.ended.insert(id.to_owned());
                    if exhausted {
                        p.sources
                            .get_mut(id)
                            .unwrap_or_else(|| unreachable!("registered source"))
                            .watermark = u64::MAX;
                    }
                    if p.ended.len() == p.registered.len() {
                        p.draining = true;
                        self.changed.notify_one();
                    }
                }
                Err(flume::TryRecvError::Empty) => {}
            }
        }
        if p.draining {
            if p.paused.insert(id.to_owned()) {
                self.changed.notify_one();
            }
            if p.global_watermark.is_some() {
                return SourceAction::Advance(p.epoch + 1);
            }
        }
        SourceAction::Wait
    }

    pub fn boundary(&self) -> Option<u64> {
        let p = self.lock();
        p.global_watermark.map(|_| p.epoch + 1)
    }

    pub fn watermark(&self, frontier: u64) -> Option<u64> {
        let p = self.lock();
        p.global_watermark.filter(|_| frontier > p.epoch)
    }

    pub fn worker_ready(&self, worker: usize) {
        self.lock().ready.insert(worker);
        self.changed.notify_one();
    }

    pub async fn coordinate(&self, sinks: Vec<flume::Sender<SinkMessage>>) -> anyhow::Result<u64> {
        let result = self.coordinate_inner(sinks).await;
        if let Err(ref error) = result {
            self.fail(error);
        }
        result
    }

    async fn coordinate_inner(
        &self,
        sinks: Vec<flume::Sender<SinkMessage>>,
    ) -> anyhow::Result<u64> {
        self.session.lock().await.drive(self, &sinks).await
    }

    async fn prepare_snapshot(
        &self,
        sinks: &[flume::Sender<SinkMessage>],
    ) -> anyhow::Result<Prepared> {
        while self.lock().ready.len() != self.workers {
            self.result()?;
            self.changed.notified().await;
        }
        // All graph branches and workers have drained before these FIFO flushes.
        for sink in sinks {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sink.send_async(SinkMessage::Flush(tx))
                .await
                .map_err(|_| anyhow::anyhow!("sink stopped before checkpoint flush"))?;
            rx.await
                .map_err(|_| anyhow::anyhow!("sink failed during checkpoint flush"))?
                .map_err(anyhow::Error::msg)?;
        }
        let (epoch, sources) = {
            let p = self.lock();
            (p.epoch, p.sources.clone())
        };
        let snapshot_dir = self.attempt.join(format!("checkpoint-{epoch}"));
        std::fs::create_dir(&snapshot_dir)?;
        File::open(&self.attempt)?.sync_all()?;
        let mut local_snapshots = BTreeMap::new();
        let slate_snapshot = match &self.storage {
            Storage::Local(backends) => {
                for (index, (name, backend)) in backends.iter().enumerate() {
                    let path = snapshot_dir.join(format!("state-{index}.json"));
                    backend.snapshot(&path)?;
                    local_snapshots
                        .insert(name.clone(), path.strip_prefix(&self.directory)?.to_owned());
                }
                None
            }
            Storage::Slate(backend) => Some(backend.snapshot().await?),
        };
        self.result()?;
        Ok(Prepared {
            sources,
            source_layout: self.lock().source_layout.clone(),
            local_snapshots,
            slate_snapshot,
        })
    }

    fn publish(&self, epoch: u64, participants: BTreeMap<usize, Prepared>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.process_id == 0
                && participants.len() == self.processes
                && (0..self.processes).all(|id| participants.contains_key(&id)),
            "incomplete checkpoint participants"
        );
        self.result()?;
        let manifest = Manifest {
            version: 1,
            epoch,
            workers: self.workers,
            max_parallelism: self.max_parallelism,
            topology: self.topology.clone(),
            processes: self.processes,
            participants,
        };
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        let snapshot_dir = self.attempt.join(format!("checkpoint-{epoch}"));
        let immutable = snapshot_dir.join("manifest.json");
        write_new(&immutable, &bytes)?;
        File::open(&snapshot_dir)?.sync_all()?;
        // Rename is the sole commit decision. Any error aborts this attempt;
        // a restart resolves an uncertain response by reading recovery.json.
        let pending = self.directory.join("recovery.pending");
        let mut file = File::create(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(pending, self.directory.join("recovery.json"))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn commit(&self, epoch: u64, final_epoch: bool) -> anyhow::Result<()> {
        let mut p = self.lock();
        anyhow::ensure!(p.epoch == epoch, "checkpoint decision for wrong epoch");
        if final_epoch {
            p.done = true;
            return Ok(());
        }
        p.epoch = epoch
            .checked_add(1)
            .filter(|next| *next < u64::MAX)
            .ok_or_else(|| anyhow::anyhow!("checkpoint epoch exhausted"))?;
        p.batches = 0;
        p.draining = false;
        p.paused.clear();
        p.ready.clear();
        p.global_watermark = None;
        Ok(())
    }
}

fn write_new(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(crate) async fn run(
    graph: CompiledGraph,
    controller: &PipelineController,
    shutdown: Option<ShutdownHandle>,
) -> anyhow::Result<()> {
    let recovery = Recovery::prepare(controller, &graph).await?;
    let manager = Arc::new(
        TaskManager::build(
            graph,
            controller,
            shutdown.as_ref(),
            std::collections::HashMap::new(),
            0,
            Some(recovery.clone()),
        )
        .await?,
    );
    recovery.validate_sources()?;
    controller
        .health
        .set_status(crate::health::PipelineStatus::Running);
    let result = manager.clone().run(controller).await;
    let manager = Arc::try_unwrap(manager)
        .map_err(|_| anyhow::anyhow!("recovery tasks still own task manager"))?;
    let drain = manager.drain().await;
    if let Storage::Slate(backend) = &recovery.storage {
        backend.close().await?;
    }
    result?;
    drain?;
    recovery.result()?;
    controller
        .health
        .set_status(crate::health::PipelineStatus::Stopped);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publication_survives_lost_commit_response_and_fences_other_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let controller = PipelineController::builder()
            .checkpoint_dir(directory.path())
            .coordinated_recovery(true)
            .build()
            .unwrap();
        let graph = CompiledGraph {
            nodes: vec![],
            topo_order: vec![],
            source_ids: vec![],
            sink_ids: vec![],
            operator_names: vec!["operator".into()],
            topology: crate::compiler::ApiTopology {
                nodes: vec![],
                edges: vec![],
            },
        };
        let first = Recovery::prepare(&controller, &graph).await.unwrap();
        assert!(
            Recovery::prepare(&controller, &graph).await.is_err(),
            "overlapping attempt must be fenced"
        );
        let mut context = first.state("operator", 0).unwrap();
        context.state.put(b"key", &42u64).unwrap();
        context.state.checkpoint().await.unwrap();
        first.register_source("0:0");
        first.register_layout(0, None);
        first.worker_ready(0);
        let snapshot = first.prepare_snapshot(&[]).await.unwrap();
        assert!(first.publish(1, BTreeMap::new()).is_err());
        assert!(!directory.path().join("recovery.json").exists());
        first.publish(1, BTreeMap::from([(0, snapshot)])).unwrap();
        // Crash after the durable decision, before any Commit response, then
        // leave newer writes in the abandoned attempt namespace.
        context.state.put(b"key", &99u64).unwrap();
        context.state.checkpoint().await.unwrap();
        drop(context);
        drop(first);
        let second = Recovery::prepare(&controller, &graph).await.unwrap();
        assert_eq!(second.lock().epoch, 2);
        let mut context = second.state("operator", 0).unwrap();
        assert_eq!(context.state.get::<u64>(b"key").await.unwrap(), Some(42));
        second.register_source("0:0");
        second.register_layout(0, None);
        second.validate_sources().unwrap();
        second.register_layout(0, Some(2));
        assert!(
            second.validate_sources().is_err(),
            "partition changes must be rejected"
        );
    }
}
