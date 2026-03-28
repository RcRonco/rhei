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
/// Written atomically (temp-file + rename) after every checkpoint cycle so
/// that a crash mid-write never leaves a corrupt manifest on disk.
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
}

const MANIFEST_FILE: &str = "manifest.json";

impl CheckpointManifest {
    /// Atomically persist the manifest to `{dir}/manifest.json`.
    ///
    /// Writes to a temporary file first, then renames — so readers never
    /// observe a partially-written file.
    pub fn save(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        let target = dir.join(MANIFEST_FILE);
        let tmp = dir.join(format!(".{MANIFEST_FILE}.tmp"));
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &target)?;
        Ok(())
    }

    /// Load a manifest from `{dir}/manifest.json`, returning `None` if the
    /// file does not exist.
    pub fn load(dir: &Path) -> Option<Self> {
        let path = dir.join(MANIFEST_FILE);
        let data = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Save a per-process partial manifest as `{dir}/manifest_p{process_id}.json`.
    ///
    /// Used in cluster mode: each process writes its own partial manifest,
    /// and process 0 merges them into the final `manifest.json`.
    pub fn save_partial(&self, dir: &Path, process_id: usize) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        let filename = format!("manifest_p{process_id}.json");
        let target = dir.join(&filename);
        let tmp = dir.join(format!(".{filename}.tmp"));
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &target)?;
        Ok(())
    }

    /// Load a per-process partial manifest from `{dir}/manifest_p{process_id}.json`.
    pub fn load_partial(dir: &Path, process_id: usize) -> Option<Self> {
        let path = dir.join(format!("manifest_p{process_id}.json"));
        let data = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&data).ok()
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

    /// Load a manifest from an object store, returning `None` if the object
    /// does not exist.
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> Option<Self> {
        let result = store.get(path).await.ok()?;
        let bytes = result.bytes().await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Merge partial manifests from all processes into a single manifest.
    ///
    /// Returns `None` if any partial manifest is missing. Source offsets
    /// from all partials are combined (later processes overwrite duplicate keys).
    pub fn merge_partials(dir: &Path, n_processes: usize) -> Option<Self> {
        let mut partials = Vec::with_capacity(n_processes);
        for pid in 0..n_processes {
            partials.push(Self::load_partial(dir, pid)?);
        }

        let mut merged_offsets = HashMap::new();
        let mut max_checkpoint_id = 0u64;
        let mut max_timestamp_ms = 0u64;
        let mut operators = Vec::new();

        for partial in &partials {
            merged_offsets.extend(partial.source_offsets.clone());
            max_checkpoint_id = max_checkpoint_id.max(partial.checkpoint_id);
            max_timestamp_ms = max_timestamp_ms.max(partial.timestamp_ms);
            if operators.is_empty() {
                operators.clone_from(&partial.operators);
            }
        }

        Some(Self {
            version: 1,
            checkpoint_id: max_checkpoint_id,
            timestamp_ms: max_timestamp_ms,
            operators,
            source_offsets: merged_offsets,
            n_processes: None,
            workers_per_process: None,
        })
    }
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
        };

        p0.save_partial(&dir, 0).unwrap();
        p1.save_partial(&dir, 1).unwrap();

        let merged = CheckpointManifest::merge_partials(&dir, 2).expect("merge should succeed");
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
        };
        p0.save_partial(&dir, 0).unwrap();

        // Only 1 of 3 partial manifests exist.
        assert!(CheckpointManifest::merge_partials(&dir, 3).is_none());

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
        };

        manifest.save(&dir).unwrap();
        let loaded = CheckpointManifest::load(&dir).expect("manifest should exist");
        assert_eq!(loaded.n_processes, Some(2));
        assert_eq!(loaded.workers_per_process, Some(4));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
