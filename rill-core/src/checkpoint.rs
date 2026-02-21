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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = std::env::temp_dir().join(format!("rill_manifest_rt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let manifest = CheckpointManifest {
            version: 1,
            checkpoint_id: 42,
            timestamp_ms: 1_700_000_000_000,
            operators: vec!["op_a".into(), "op_b".into()],
            source_offsets: HashMap::from([("t/0".into(), "99".into())]),
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
        let dir = std::env::temp_dir().join("rill_manifest_missing");
        assert!(CheckpointManifest::load(&dir).is_none());
    }
}
