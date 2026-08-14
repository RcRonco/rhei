//! Checkpoint manifest for recording pipeline checkpoint metadata.
//!
//! After each checkpoint cycle the executor writes a [`CheckpointManifest`]
//! atomically to disk.  On restart the manifest is loaded to detect and
//! validate existing checkpoints.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Persistent record of a completed checkpoint.
///
/// Written durably after every checkpoint cycle (temp file → fsync → rename →
/// directory fsync), so neither a concurrent reader nor a crash mid-write can
/// observe a partial manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointManifest {
    /// Schema version (currently `1`).
    pub version: u32,
    /// Monotonically increasing checkpoint identifier.
    pub checkpoint_id: u64,
    /// Unix timestamp in milliseconds when the checkpoint was taken.
    pub timestamp_ms: u64,
    /// Sorted list of operator names present in the pipeline.
    pub operators: Vec<String>,
    /// Source-specific offset snapshot.  Key format is source-defined
    /// (e.g. `"topic/partition"` for Kafka).
    pub source_offsets: HashMap<String, String>,
    /// Number of processes in the cluster (None for v1 manifests).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n_processes: Option<usize>,
    /// Workers per process (None for v1 manifests).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers_per_process: Option<usize>,
    /// Number of key groups the state in this checkpoint is partitioned into.
    ///
    /// Fixed for a pipeline's lifetime. Every key's group is
    /// `hash(key) % max_parallelism`, so resuming with a different value would
    /// send every key looking under the wrong prefix — silently reading empty
    /// state rather than failing. [`Self::validate_compatible`] rejects that.
    ///
    /// `None` on manifests written before key groups existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallelism: Option<usize>,
    /// Total worker count of the topology that wrote this checkpoint.
    ///
    /// Purely informational — unlike `max_parallelism`, resuming at a different
    /// worker count is exactly what rescaling is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_workers: Option<usize>,
    /// Node IDs participating in the topology that wrote this checkpoint.
    ///
    /// Recorded for operational forensics: it makes the membership history of a
    /// pipeline visible in its checkpoint trail.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_members: Vec<String>,
}

/// Why a checkpoint cannot be resumed with the current configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointIncompatibility {
    /// The manifest's key group count differs from the configured one.
    MaxParallelismChanged {
        /// Value recorded in the manifest.
        manifest: usize,
        /// Value configured for this run.
        configured: usize,
    },
}

impl std::fmt::Display for CheckpointIncompatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxParallelismChanged {
                manifest,
                configured,
            } => write!(
                f,
                "checkpoint was written with max_parallelism={manifest} but this run is \
                 configured for {configured}. Key groups are computed as \
                 `hash(key) % max_parallelism`, so changing it re-partitions the entire \
                 key space and orphans all existing state. Either restore the original \
                 value, or start from an empty checkpoint directory."
            ),
        }
    }
}

impl std::error::Error for CheckpointIncompatibility {}

const MANIFEST_FILE: &str = "manifest.json";

/// Highest manifest schema version this build knows how to read.
///
/// A manifest carrying a higher version was written by a newer Rhei and may use
/// fields this build would silently ignore, so [`CheckpointManifest::load_checked`]
/// refuses it rather than resuming against a half-understood checkpoint.
pub const MANIFEST_VERSION: u32 = 1;

/// Write `bytes` to `path` durably: fsync the file, rename it over `target`,
/// then fsync the containing directory so the rename itself survives a crash.
///
/// `std::fs::rename` alone makes the swap atomic *for a concurrent reader*, but
/// says nothing about what survives power loss: without the fsyncs the rename
/// can reach disk before the data it points at, leaving a manifest that is
/// present, valid-looking, and empty.
fn write_file_durably(dir: &Path, filename: &str, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;

    let target = dir.join(filename);
    let tmp = dir.join(format!(".{filename}.tmp"));

    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.flush()?;
        // Force the data out before the rename can publish it.
        file.sync_all()?;
    }

    std::fs::rename(&tmp, &target)?;

    // Persist the directory entry created by the rename. Without this the file
    // contents are durable but the name pointing at them may not be.
    // Directory fsync is not supported on every platform/filesystem, so a
    // failure here is logged rather than fatal — the data write above already
    // succeeded.
    match std::fs::File::open(dir) {
        Ok(dir_handle) => {
            if let Err(e) = dir_handle.sync_all() {
                tracing::debug!(error = %e, dir = %dir.display(), "directory fsync unsupported");
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, dir = %dir.display(), "could not open directory for fsync");
        }
    }

    Ok(())
}

/// Read and parse a manifest file, distinguishing "absent" from "unreadable".
///
/// Returns `Ok(None)` only when the file genuinely does not exist. Every other
/// failure — I/O error, malformed JSON, unknown schema version — is an `Err`,
/// because a checkpoint that cannot be read is never the same thing as a
/// pipeline that has never checkpointed.
fn read_manifest_file(path: &Path) -> anyhow::Result<Option<CheckpointManifest>> {
    let data = match std::fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow::Error::from(e)
                .context(format!("reading checkpoint manifest {}", path.display())));
        }
    };

    let manifest: CheckpointManifest = serde_json::from_str(&data).map_err(|e| {
        anyhow::anyhow!(
            "checkpoint manifest {} is corrupt and cannot be parsed: {e}. Refusing to start: \
             resuming would silently discard all checkpointed state and reprocess from the \
             beginning. Restore the file from backup, or delete the checkpoint directory to \
             deliberately start fresh.",
            path.display()
        )
    })?;

    anyhow::ensure!(
        manifest.version <= MANIFEST_VERSION,
        "checkpoint manifest {} has schema version {} but this build understands at most {}. \
         It was written by a newer Rhei; upgrade this binary rather than resuming against a \
         manifest whose fields it would ignore.",
        path.display(),
        manifest.version,
        MANIFEST_VERSION
    );

    Ok(Some(manifest))
}

impl CheckpointManifest {
    /// Check that this checkpoint can be resumed at `max_parallelism`.
    ///
    /// Manifests written before key groups existed carry no `max_parallelism`
    /// and are accepted: their state predates key-group addressing, so there is
    /// no recorded value to contradict.
    ///
    /// # Errors
    /// Returns [`CheckpointIncompatibility`] if the key group count differs.
    pub fn validate_compatible(
        &self,
        max_parallelism: usize,
    ) -> Result<(), CheckpointIncompatibility> {
        match self.max_parallelism {
            Some(recorded) if recorded != max_parallelism => {
                Err(CheckpointIncompatibility::MaxParallelismChanged {
                    manifest: recorded,
                    configured: max_parallelism,
                })
            }
            _ => Ok(()),
        }
    }

    /// Durably persist the manifest to `{dir}/manifest.json`.
    ///
    /// Writes to a temporary file, fsyncs it, renames it into place, then
    /// fsyncs the directory. A reader never observes a partially-written file,
    /// and a crash at any point leaves either the previous manifest or the new
    /// one — never a truncated or empty one.
    pub fn save(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        let json = serde_json::to_string_pretty(self)?;
        write_file_durably(dir, MANIFEST_FILE, json.as_bytes())
    }

    /// Load a manifest from `{dir}/manifest.json`.
    ///
    /// Returns `Ok(None)` only when no manifest exists — a pipeline that has
    /// never checkpointed. A manifest that exists but cannot be read is an
    /// `Err`, so a corrupt file fails the startup instead of silently
    /// reprocessing the stream from offset zero.
    ///
    /// # Errors
    /// Returns an error if the file exists but cannot be read, does not parse,
    /// or carries a schema version newer than [`MANIFEST_VERSION`].
    pub fn load_checked(dir: &Path) -> anyhow::Result<Option<Self>> {
        read_manifest_file(&dir.join(MANIFEST_FILE))
    }

    /// Best-effort load that maps every failure to `None`.
    ///
    /// Intended for read-only inspection surfaces (dashboards, the state
    /// explorer) where "no manifest" and "unreadable manifest" are both just
    /// "nothing to show". **Recovery paths must use [`Self::load_checked`]**:
    /// treating a corrupt manifest as absent there restarts the pipeline from
    /// scratch without telling anyone.
    pub fn load(dir: &Path) -> Option<Self> {
        match Self::load_checked(dir) {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    dir = %dir.display(),
                    "checkpoint manifest could not be read; reporting as absent"
                );
                None
            }
        }
    }

    /// Save a per-process partial manifest as `{dir}/manifest_p{process_id}.json`.
    ///
    /// Used in cluster mode: each process writes its own partial manifest,
    /// and process 0 merges them into the final `manifest.json`. Durability
    /// matches [`Self::save`].
    pub fn save_partial(&self, dir: &Path, process_id: usize) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        let json = serde_json::to_string_pretty(self)?;
        write_file_durably(dir, &partial_filename(process_id), json.as_bytes())
    }

    /// Load a per-process partial manifest, distinguishing absent from corrupt.
    ///
    /// # Errors
    /// Returns an error if the partial exists but cannot be read or parsed.
    pub fn load_partial_checked(dir: &Path, process_id: usize) -> anyhow::Result<Option<Self>> {
        read_manifest_file(&dir.join(partial_filename(process_id)))
    }

    /// Best-effort partial load that maps every failure to `None`.
    ///
    /// See [`Self::load`] for why recovery paths should prefer
    /// [`Self::load_partial_checked`].
    pub fn load_partial(dir: &Path, process_id: usize) -> Option<Self> {
        match Self::load_partial_checked(dir, process_id) {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    process_id,
                    "partial checkpoint manifest could not be read; reporting as absent"
                );
                None
            }
        }
    }

    /// Persist the manifest to an object store at the given path.
    ///
    /// Used for remote checkpoint storage (S3, Azure Blob, GCS) so all
    /// processes can load the latest manifest on recovery.
    pub async fn save_to_object_store(
        &self,
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        store
            .put(path, object_store::PutPayload::from(json.into_bytes()))
            .await?;
        Ok(())
    }

    /// Load a manifest from an object store.
    ///
    /// Returns `Ok(None)` only when the object does not exist. A present but
    /// unreadable object is an `Err`, for the same reason as
    /// [`Self::load_checked`].
    ///
    /// # Errors
    /// Returns an error if the object exists but cannot be fetched, does not
    /// parse, or carries a schema version newer than [`MANIFEST_VERSION`].
    pub async fn load_from_object_store_checked(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Option<Self>> {
        let result = match store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(
                    anyhow::Error::from(e).context(format!("fetching checkpoint manifest {path}"))
                );
            }
        };

        let bytes = result
            .bytes()
            .await
            .map_err(|e| anyhow::Error::from(e).context(format!("reading manifest body {path}")))?;

        let manifest: Self = serde_json::from_slice(&bytes).map_err(|e| {
            anyhow::anyhow!(
                "checkpoint manifest {path} is corrupt and cannot be parsed: {e}. Refusing to \
                 start: resuming would silently discard all checkpointed state and reprocess \
                 from the beginning."
            )
        })?;

        anyhow::ensure!(
            manifest.version <= MANIFEST_VERSION,
            "checkpoint manifest {path} has schema version {} but this build understands at \
             most {MANIFEST_VERSION}. It was written by a newer Rhei; upgrade this binary.",
            manifest.version
        );

        Ok(Some(manifest))
    }

    /// Best-effort object-store load that maps every failure to `None`.
    ///
    /// See [`Self::load`] for why recovery paths should prefer
    /// [`Self::load_from_object_store_checked`].
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> Option<Self> {
        match Self::load_from_object_store_checked(store, path).await {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    %path,
                    "remote checkpoint manifest could not be read; reporting as absent"
                );
                None
            }
        }
    }

    /// Merge partial manifests from all processes into a single manifest.
    ///
    /// Returns `Ok(None)` if any partial manifest has not been written yet —
    /// the normal race during a checkpoint cycle. Source offsets from all
    /// partials are combined, and the cluster shape fields (`max_parallelism`,
    /// `n_processes`, `workers_per_process`, `total_workers`, `cluster_members`)
    /// are carried through to the merged manifest.
    ///
    /// Carrying `max_parallelism` through is what keeps
    /// [`Self::validate_compatible`] meaningful in cluster mode: the merged
    /// manifest is the one a restart reads, so dropping the field there would
    /// disable the key-group safety check on exactly the deployments that need
    /// it most.
    ///
    /// # Errors
    /// Returns an error if a partial cannot be read, or if the partials
    /// disagree on `max_parallelism` — processes in one cluster must share a
    /// key-group count, and disagreement means their state is partitioned
    /// incompatibly.
    pub fn merge_partials(dir: &Path, n_processes: usize) -> anyhow::Result<Option<Self>> {
        let mut partials = Vec::with_capacity(n_processes);
        for pid in 0..n_processes {
            match Self::load_partial_checked(dir, pid)? {
                Some(partial) => partials.push(partial),
                // Not written yet — the caller retries on the next cycle.
                None => return Ok(None),
            }
        }

        let mut merged_offsets = HashMap::new();
        let mut max_checkpoint_id = 0u64;
        let mut max_timestamp_ms = 0u64;
        let mut operators: Vec<String> = Vec::new();
        let mut members: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut max_parallelism: Option<usize> = None;
        let mut workers_per_process: Option<usize> = None;
        let mut total_workers = 0usize;
        let mut saw_worker_counts = false;

        for (pid, partial) in partials.iter().enumerate() {
            merged_offsets.extend(partial.source_offsets.clone());
            max_checkpoint_id = max_checkpoint_id.max(partial.checkpoint_id);
            max_timestamp_ms = max_timestamp_ms.max(partial.timestamp_ms);
            if operators.is_empty() {
                operators.clone_from(&partial.operators);
            }
            members.extend(partial.cluster_members.iter().cloned());

            // Every process must agree on the key group count. A mismatch means
            // the processes partition the key space differently, so their state
            // cannot be merged into one checkpoint.
            match (max_parallelism, partial.max_parallelism) {
                (Some(existing), Some(found)) => anyhow::ensure!(
                    existing == found,
                    "partial checkpoint manifests disagree on max_parallelism: process {pid} \
                     recorded {found}, an earlier process recorded {existing}. All processes in \
                     a cluster must run with the same key group count."
                ),
                (None, found @ Some(_)) => max_parallelism = found,
                _ => {}
            }

            if let Some(wpp) = partial.workers_per_process {
                workers_per_process = Some(workers_per_process.map_or(wpp, |w: usize| w.max(wpp)));
            }
            if let Some(tw) = partial.total_workers {
                // Prefer the recorded topology-wide total when present.
                total_workers = total_workers.max(tw);
                saw_worker_counts = true;
            }
        }

        Ok(Some(Self {
            version: MANIFEST_VERSION,
            checkpoint_id: max_checkpoint_id,
            timestamp_ms: max_timestamp_ms,
            operators,
            source_offsets: merged_offsets,
            n_processes: Some(n_processes),
            workers_per_process,
            max_parallelism,
            total_workers: saw_worker_counts.then_some(total_workers),
            cluster_members: members.into_iter().collect(),
        }))
    }
}

/// Filename for a process's partial manifest.
fn partial_filename(process_id: usize) -> String {
    format!("manifest_p{process_id}.json")
}

/// Load operator state from a checkpoint file.
///
/// Reads `{dir}/{operator_name}.checkpoint.json` (`LocalBackend` format),
/// strips the `{operator_name}/` prefix from keys, and returns
/// `(user_key_string, raw_value_bytes)` pairs.
///
/// Returns an empty vec if the file does not exist.
pub fn load_operator_state(
    dir: &Path,
    operator_name: &str,
) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let path = dir.join(format!("{operator_name}.checkpoint.json"));
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = std::fs::read_to_string(&path)?;
    let raw_entries: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_str(&contents)?;

    let prefix = format!("{operator_name}/");
    let entries = raw_entries
        .into_iter()
        .map(|(key_bytes, value_bytes)| {
            let key_str = String::from_utf8_lossy(&key_bytes);
            let user_key = key_str
                .strip_prefix(&prefix)
                .unwrap_or(&key_str)
                .to_string();
            (user_key, value_bytes)
        })
        .collect();

    Ok(entries)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = std::env::temp_dir().join(format!("rhei_manifest_rt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id: 42,
            timestamp_ms: 1_700_000_000_000,
            operators: vec!["op_a".into(), "op_b".into()],
            source_offsets: HashMap::from([("t/0".into(), "99".into())]),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };

        manifest.save(&dir).unwrap();
        let loaded = CheckpointManifest::load(&dir).expect("manifest should exist");

        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.checkpoint_id, 42);
        assert_eq!(loaded.timestamp_ms, 1_700_000_000_000);
        assert_eq!(loaded.operators, vec!["op_a", "op_b"]);
        assert_eq!(loaded.source_offsets.get("t/0").unwrap(), "99");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = std::env::temp_dir().join("rhei_manifest_missing");
        assert!(CheckpointManifest::load(&dir).is_none());
    }

    #[test]
    fn partial_save_load_round_trip() {
        let dir =
            std::env::temp_dir().join(format!("rhei_manifest_partial_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id: 10,
            timestamp_ms: 1_700_000_000_000,
            operators: vec!["op_a".into()],
            source_offsets: HashMap::from([("t/0".into(), "50".into())]),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };

        manifest.save_partial(&dir, 0).unwrap();
        let loaded = CheckpointManifest::load_partial(&dir, 0).expect("partial should exist");
        assert_eq!(loaded.checkpoint_id, 10);
        assert_eq!(loaded.source_offsets.get("t/0").unwrap(), "50");

        // Partial for a different process should not exist.
        assert!(CheckpointManifest::load_partial(&dir, 1).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_partials_combines_offsets() {
        let dir = std::env::temp_dir().join(format!("rhei_manifest_merge_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let p0 = CheckpointManifest {
            version: 1,
            checkpoint_id: 5,
            timestamp_ms: 1_000,
            operators: vec!["op_a".into(), "op_b".into()],
            source_offsets: HashMap::from([
                ("topic/0".into(), "100".into()),
                ("topic/1".into(), "200".into()),
            ]),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };
        let p1 = CheckpointManifest {
            version: 1,
            checkpoint_id: 5,
            timestamp_ms: 1_100,
            operators: vec!["op_a".into(), "op_b".into()],
            source_offsets: HashMap::from([
                ("topic/2".into(), "300".into()),
                ("topic/3".into(), "400".into()),
            ]),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };

        p0.save_partial(&dir, 0).unwrap();
        p1.save_partial(&dir, 1).unwrap();

        let merged = CheckpointManifest::merge_partials(&dir, 2)
            .expect("merge should not error")
            .expect("all partials present");
        assert_eq!(merged.checkpoint_id, 5);
        assert_eq!(merged.timestamp_ms, 1_100); // max
        assert_eq!(merged.source_offsets.len(), 4);
        assert_eq!(merged.source_offsets.get("topic/0").unwrap(), "100");
        assert_eq!(merged.source_offsets.get("topic/2").unwrap(), "300");
        assert_eq!(merged.source_offsets.get("topic/3").unwrap(), "400");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_partials_returns_none_when_incomplete() {
        let dir =
            std::env::temp_dir().join(format!("rhei_manifest_merge_inc_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let p0 = CheckpointManifest {
            version: 1,
            checkpoint_id: 1,
            timestamp_ms: 1_000,
            operators: vec![],
            source_offsets: HashMap::new(),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };
        p0.save_partial(&dir, 0).unwrap();

        // Only 1 of 3 partial manifests exist.
        assert!(
            CheckpointManifest::merge_partials(&dir, 3)
                .expect("missing partials are not an error")
                .is_none()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn object_store_round_trip() {
        let store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("checkpoints/manifest.json");

        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id: 7,
            timestamp_ms: 2_000_000_000_000,
            operators: vec!["join".into(), "window".into()],
            source_offsets: HashMap::from([
                ("topic/0".into(), "42".into()),
                ("topic/1".into(), "99".into()),
            ]),
            n_processes: None,
            workers_per_process: None,
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };

        manifest.save_to_object_store(&store, &path).await.unwrap();

        let loaded = CheckpointManifest::load_from_object_store(&store, &path)
            .await
            .expect("manifest should exist in object store");

        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.checkpoint_id, 7);
        assert_eq!(loaded.timestamp_ms, 2_000_000_000_000);
        assert_eq!(loaded.operators, vec!["join", "window"]);
        assert_eq!(loaded.source_offsets.get("topic/0").unwrap(), "42");
        assert_eq!(loaded.source_offsets.get("topic/1").unwrap(), "99");
    }

    #[tokio::test]
    async fn object_store_load_missing_returns_none() {
        let store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("nonexistent/manifest.json");
        assert!(
            CheckpointManifest::load_from_object_store(&store, &path)
                .await
                .is_none()
        );
    }

    #[test]
    fn load_operator_state_parses_checkpoint_file() {
        let dir = std::env::temp_dir().join(format!("rhei_state_load_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"my_op/key1".to_vec(), b"\"value1\"".to_vec()),
            (b"my_op/key2".to_vec(), b"\"value2\"".to_vec()),
        ];
        let json = serde_json::to_string(&entries).unwrap();
        std::fs::write(dir.join("my_op.checkpoint.json"), &json).unwrap();

        let result = load_operator_state(&dir, "my_op").unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|(k, _)| k == "key1"));
        assert!(result.iter().any(|(k, _)| k == "key2"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_operator_state_returns_empty_for_missing_file() {
        let dir = std::env::temp_dir().join("rhei_state_missing");
        let result = load_operator_state(&dir, "nonexistent");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn v1_manifest_deserializes_with_default_topology() {
        let json = r#"{
        "version": 1,
        "checkpoint_id": 5,
        "timestamp_ms": 1000,
        "operators": ["op_a"],
        "source_offsets": {"t/0": "42"}
    }"#;
        let manifest: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.n_processes, None);
        assert_eq!(manifest.workers_per_process, None);
    }

    #[test]
    fn topology_metadata_round_trips() {
        let dir = std::env::temp_dir().join(format!("rhei_manifest_topo_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id: 1,
            timestamp_ms: 1000,
            operators: vec!["op".into()],
            source_offsets: HashMap::new(),
            n_processes: Some(2),
            workers_per_process: Some(4),
            max_parallelism: None,
            total_workers: None,
            cluster_members: Vec::new(),
        };

        manifest.save(&dir).unwrap();
        let loaded = CheckpointManifest::load(&dir).expect("manifest should exist");
        assert_eq!(loaded.n_processes, Some(2));
        assert_eq!(loaded.workers_per_process, Some(4));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Key group compatibility ─────────────────────────────────────

    fn manifest_with_max_parallelism(max_parallelism: Option<usize>) -> CheckpointManifest {
        CheckpointManifest {
            version: 1,
            checkpoint_id: 1,
            timestamp_ms: 0,
            operators: vec!["op".to_string()],
            source_offsets: HashMap::new(),
            n_processes: Some(1),
            workers_per_process: Some(2),
            max_parallelism,
            total_workers: Some(2),
            cluster_members: vec!["node-a".to_string()],
        }
    }

    #[test]
    fn matching_max_parallelism_is_compatible() {
        assert!(
            manifest_with_max_parallelism(Some(128))
                .validate_compatible(128)
                .is_ok()
        );
    }

    #[test]
    fn changed_max_parallelism_is_rejected() {
        // Silently accepting this would resume against state that every key
        // now hashes away from — reading empty values instead of failing.
        let err = manifest_with_max_parallelism(Some(128))
            .validate_compatible(256)
            .unwrap_err();
        assert_eq!(
            err,
            CheckpointIncompatibility::MaxParallelismChanged {
                manifest: 128,
                configured: 256,
            }
        );
        let msg = err.to_string();
        assert!(
            msg.contains("128") && msg.contains("256"),
            "unhelpful message: {msg}"
        );
    }

    #[test]
    fn manifests_without_max_parallelism_are_accepted() {
        // Written before key groups existed: no recorded value to contradict.
        assert!(
            manifest_with_max_parallelism(None)
                .validate_compatible(128)
                .is_ok()
        );
    }

    #[test]
    fn worker_count_may_change_freely() {
        // Resuming at a different parallelism is exactly what rescaling is.
        let mut manifest = manifest_with_max_parallelism(Some(128));
        manifest.total_workers = Some(4);
        manifest.workers_per_process = Some(4);
        assert!(manifest.validate_compatible(128).is_ok());
    }

    #[test]
    fn topology_fields_survive_a_serde_round_trip() {
        let manifest = manifest_with_max_parallelism(Some(256));
        let json = serde_json::to_string(&manifest).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.max_parallelism, Some(256));
        assert_eq!(restored.total_workers, Some(2));
        assert_eq!(restored.cluster_members, vec!["node-a".to_string()]);
    }

    #[test]
    fn old_manifests_still_deserialize() {
        // Backward compatibility: a manifest written before these fields
        // existed must still load, with the new fields defaulted.
        let json = r#"{
            "version": 1,
            "checkpoint_id": 7,
            "timestamp_ms": 100,
            "operators": ["op"],
            "source_offsets": {}
        }"#;
        let manifest: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.checkpoint_id, 7);
        assert_eq!(manifest.max_parallelism, None);
        assert_eq!(manifest.total_workers, None);
        assert!(manifest.cluster_members.is_empty());
        assert!(manifest.validate_compatible(128).is_ok());
    }

    // ── Corruption is never mistaken for "no checkpoint" ─────────────────

    /// Build a manifest with the given key group count.
    fn manifest_with(max_parallelism: Option<usize>) -> CheckpointManifest {
        CheckpointManifest {
            version: MANIFEST_VERSION,
            checkpoint_id: 7,
            timestamp_ms: 1_234,
            operators: vec!["counter".to_string()],
            source_offsets: HashMap::new(),
            n_processes: Some(2),
            workers_per_process: Some(4),
            max_parallelism,
            total_workers: Some(8),
            cluster_members: vec!["node-a".to_string()],
        }
    }

    #[test]
    fn load_checked_reports_absent_manifest_as_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            CheckpointManifest::load_checked(dir.path())
                .expect("absent manifest is not an error")
                .is_none()
        );
    }

    #[test]
    fn load_checked_errors_on_corrupt_manifest() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"{not valid json").unwrap();

        // The whole point: a corrupt manifest must not look like a fresh start,
        // or the pipeline silently reprocesses the stream from the beginning.
        let err = CheckpointManifest::load_checked(dir.path())
            .expect_err("corrupt manifest must be an error");
        assert!(err.to_string().contains("corrupt"), "got: {err}");
    }

    #[test]
    fn load_checked_errors_on_truncated_manifest() {
        let dir = tempfile::tempdir().unwrap();
        // The exact shape a non-durable write leaves behind after power loss:
        // the file exists, but its contents never reached the disk.
        std::fs::write(dir.path().join("manifest.json"), b"").unwrap();

        assert!(CheckpointManifest::load_checked(dir.path()).is_err());
    }

    #[test]
    fn load_checked_rejects_future_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = manifest_with(Some(128));
        manifest.version = MANIFEST_VERSION + 1;
        manifest.save(dir.path()).unwrap();

        let err = CheckpointManifest::load_checked(dir.path())
            .expect_err("a newer schema version must be rejected");
        assert!(err.to_string().contains("schema version"), "got: {err}");
    }

    #[test]
    fn lossy_load_maps_corruption_to_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"garbage").unwrap();

        // `load` stays lossy for read-only surfaces, but recovery paths use
        // `load_checked` — see the doc comment on `load`.
        assert!(CheckpointManifest::load(dir.path()).is_none());
    }

    #[test]
    fn saved_manifest_round_trips_through_load_checked() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(256)).save(dir.path()).unwrap();

        let loaded = CheckpointManifest::load_checked(dir.path())
            .unwrap()
            .expect("manifest should exist");
        assert_eq!(loaded.checkpoint_id, 7);
        assert_eq!(loaded.max_parallelism, Some(256));
    }

    #[test]
    fn save_leaves_no_temp_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(128)).save(dir.path()).unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|e| e == "tmp")
            })
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
    }

    // ── Merged cluster manifests keep their key group count ──────────────

    #[test]
    fn merge_partials_preserves_max_parallelism() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 0)
            .unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 1)
            .unwrap();

        let merged = CheckpointManifest::merge_partials(dir.path(), 2)
            .unwrap()
            .expect("all partials present");

        // Dropping this field is what silently disabled the key-group safety
        // check in cluster mode.
        assert_eq!(merged.max_parallelism, Some(128));
        assert_eq!(merged.n_processes, Some(2));
        assert_eq!(merged.workers_per_process, Some(4));
        assert_eq!(merged.total_workers, Some(8));
        assert_eq!(merged.cluster_members, vec!["node-a".to_string()]);
    }

    #[test]
    fn merged_manifest_still_rejects_changed_key_group_count() {
        // End-to-end regression for the cluster-mode gap: write partials,
        // merge them the way process 0 does, reload, and confirm the restart
        // check still fires.
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 0)
            .unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 1)
            .unwrap();
        CheckpointManifest::merge_partials(dir.path(), 2)
            .unwrap()
            .unwrap()
            .save(dir.path())
            .unwrap();

        let reloaded = CheckpointManifest::load_checked(dir.path())
            .unwrap()
            .expect("merged manifest should exist");

        assert!(reloaded.validate_compatible(128).is_ok());
        assert_eq!(
            reloaded.validate_compatible(256),
            Err(CheckpointIncompatibility::MaxParallelismChanged {
                manifest: 128,
                configured: 256,
            })
        );
    }

    #[test]
    fn merge_partials_rejects_disagreeing_max_parallelism() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 0)
            .unwrap();
        manifest_with(Some(256))
            .save_partial(dir.path(), 1)
            .unwrap();

        let err = CheckpointManifest::merge_partials(dir.path(), 2)
            .expect_err("processes must agree on the key group count");
        assert!(err.to_string().contains("max_parallelism"), "got: {err}");
    }

    #[test]
    fn merge_partials_errors_on_corrupt_partial() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(Some(128))
            .save_partial(dir.path(), 0)
            .unwrap();
        std::fs::write(dir.path().join("manifest_p1.json"), b"{{{").unwrap();

        // A corrupt partial must not be silently treated as "not written yet",
        // which would let the merge quietly skip a process's offsets.
        assert!(CheckpointManifest::merge_partials(dir.path(), 2).is_err());
    }

    #[test]
    fn merge_partials_tolerates_legacy_partials_without_key_groups() {
        let dir = tempfile::tempdir().unwrap();
        manifest_with(None).save_partial(dir.path(), 0).unwrap();
        manifest_with(None).save_partial(dir.path(), 1).unwrap();

        let merged = CheckpointManifest::merge_partials(dir.path(), 2)
            .unwrap()
            .expect("all partials present");
        assert_eq!(merged.max_parallelism, None);
        assert!(merged.validate_compatible(64).is_ok());
    }
}
