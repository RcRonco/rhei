use std::collections::HashMap;

use bytes::Bytes;
use moka::sync::Cache;

/// Configuration for bounding the L1 `MemTable` size.
#[derive(Debug, Clone)]
pub struct MemTableConfig {
    /// Maximum approximate bytes for the clean entry cache.
    /// Default: 32 MiB.
    pub max_bytes: usize,
    /// Maximum number of entries in the clean entry cache.
    /// Default: 500,000.
    pub max_entries: usize,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_bytes: 32 * 1024 * 1024,
            max_entries: 500_000,
        }
    }
}

/// L1 in-memory write buffer with dirty tracking and W-TinyLFU eviction.
///
/// Dirty entries (written since last flush) live in a `HashMap` and are never
/// evicted. Clean entries (already persisted to L2/L3) live in a [`moka`] cache
/// with W-TinyLFU admission and eviction to bound memory usage per
/// [`MemTableConfig`].
pub struct MemTable {
    /// Dirty entries not yet flushed to the backend. Never evicted.
    dirty: HashMap<Vec<u8>, Option<Bytes>>,
    /// Clean entry cache with W-TinyLFU eviction policy.
    clean: Cache<Vec<u8>, Bytes>,
}

impl std::fmt::Debug for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTable")
            .field("dirty_count", &self.dirty.len())
            .field("clean_count", &self.clean.entry_count())
            .finish()
    }
}

/// Build the moka clean cache from config.
///
/// When `max_bytes` is `usize::MAX` (i.e. byte-bounding is disabled), the cache
/// uses entry-count based eviction via `max_entries`. Otherwise byte-weighted
/// eviction is used, with a minimum weight per entry derived from
/// `max_bytes / max_entries` to enforce the entry count limit as well.
fn build_clean_cache(config: &MemTableConfig) -> Cache<Vec<u8>, Bytes> {
    if config.max_bytes == usize::MAX {
        // Entry count-based eviction only (used in tests with unbounded bytes).
        Cache::builder()
            .max_capacity(config.max_entries as u64)
            .build()
    } else {
        // Byte-weighted eviction. A minimum weight per entry also enforces
        // the entry count limit: `max_entries` entries × `min_weight` ≈ `max_bytes`.
        let max_bytes = config.max_bytes;
        let max_entries = config.max_entries;
        let min_weight = if max_entries > 0 {
            (max_bytes / max_entries).max(1)
        } else {
            1
        };

        Cache::builder()
            .max_capacity(max_bytes as u64)
            .weigher(move |key: &Vec<u8>, value: &Bytes| -> u32 {
                let actual = key.len() + value.len();
                u32::try_from(actual.max(min_weight)).unwrap_or(u32::MAX)
            })
            .build()
    }
}

impl MemTable {
    /// Creates a memtable with the default [`MemTableConfig`].
    pub fn new() -> Self {
        Self::with_config(MemTableConfig::default())
    }

    /// Creates a memtable with the given configuration.
    pub fn with_config(config: MemTableConfig) -> Self {
        let clean = build_clean_cache(&config);
        Self {
            dirty: HashMap::new(),
            clean,
        }
    }

    /// Looks up a key. Returns `None` on cache miss, `Some(None)` for a
    /// tombstone, or `Some(Some(v))` for a value.
    pub fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        // Check dirty map first (read-your-own-writes).
        if let Some(value) = self.dirty.get(key) {
            return Some(value.clone());
        }

        // Check clean cache.
        self.clean.get(&key.to_vec()).map(Some)
    }

    /// Inserts a key-value pair and marks the key as dirty.
    pub fn put(&mut self, key: Vec<u8>, value: Bytes) {
        // Remove from clean cache — this key is now dirty.
        self.clean.invalidate(&key);
        self.dirty.insert(key, Some(value));
    }

    /// Records a tombstone for the key and marks it as dirty.
    pub fn delete(&mut self, key: Vec<u8>) {
        // Remove from clean cache — this key is now dirty.
        self.clean.invalidate(&key);
        self.dirty.insert(key, None);
    }

    /// Returns dirty entries and clears the dirty set. Flushed values
    /// (non-tombstones) are moved into the clean cache so they remain
    /// available for reads.
    pub fn flush(&mut self) -> Vec<(Vec<u8>, Option<Bytes>)> {
        let entries: Vec<(Vec<u8>, Option<Bytes>)> = self.dirty.drain().collect();

        // Move flushed values into the clean cache. Tombstones are not cached —
        // after the backend processes the delete, a cache miss will correctly
        // fall through to the backend which returns None.
        for (key, value) in &entries {
            if let Some(v) = value {
                self.clean.insert(key.clone(), v.clone());
            }
        }

        entries
    }

    /// Returns all keys in the memtable (dirty + clean) that start with the given prefix.
    ///
    /// Dirty tombstones are excluded (value is `None`). Keys present in both dirty
    /// and clean are deduplicated.
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        use std::collections::HashSet;

        let mut keys = HashSet::new();

        // Scan dirty entries — skip tombstones (value = None).
        for (k, v) in &self.dirty {
            if k.starts_with(prefix) && v.is_some() {
                keys.insert(k.clone());
            }
        }

        // Scan clean cache entries.
        for (k, _v) in &self.clean {
            if k.starts_with(prefix) && !self.dirty.contains_key(&*k) {
                keys.insert(k.to_vec());
            }
        }

        keys.into_iter().collect()
    }

    /// Load data from the backend into the clean cache (only for keys not
    /// already present).
    pub fn merge(&mut self, key: Vec<u8>, value: Bytes) {
        // Don't overwrite dirty entries (read-your-own-writes).
        if self.dirty.contains_key(&key) {
            return;
        }

        // Don't re-insert if already in clean cache.
        if self.clean.contains_key(&key) {
            return;
        }

        self.clean.insert(key, value);
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), Bytes::from_static(b"val1"));
        assert_eq!(mt.get(b"key1"), Some(Some(Bytes::from_static(b"val1"))));
    }

    #[test]
    fn get_miss() {
        let mt = MemTable::new();
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn delete_marks_tombstone() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), Bytes::from_static(b"val1"));
        mt.delete(b"key1".to_vec());
        // Should return Some(None) indicating the key was explicitly deleted.
        assert_eq!(mt.get(b"key1"), Some(None));
    }

    #[test]
    fn flush_returns_dirty_entries() {
        let mut mt = MemTable::new();
        mt.put(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.put(b"b".to_vec(), Bytes::from_static(b"2"));
        mt.delete(b"c".to_vec());

        let mut flushed = mt.flush();
        flushed.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(flushed.len(), 3);
        assert_eq!(flushed[0], (b"a".to_vec(), Some(Bytes::from_static(b"1"))));
        assert_eq!(flushed[1], (b"b".to_vec(), Some(Bytes::from_static(b"2"))));
        assert_eq!(flushed[2], (b"c".to_vec(), None));

        // Second flush should be empty.
        assert!(mt.flush().is_empty());
    }

    #[test]
    fn merge_does_not_overwrite() {
        let mut mt = MemTable::new();
        mt.put(b"key".to_vec(), Bytes::from_static(b"local"));
        mt.merge(b"key".to_vec(), Bytes::from_static(b"backend"));
        assert_eq!(mt.get(b"key"), Some(Some(Bytes::from_static(b"local"))));
    }

    #[test]
    fn merge_fills_missing() {
        let mut mt = MemTable::new();
        mt.merge(b"key".to_vec(), Bytes::from_static(b"backend"));
        assert_eq!(mt.get(b"key"), Some(Some(Bytes::from_static(b"backend"))));
    }

    #[test]
    fn eviction_bounds_clean_entries() {
        let config = MemTableConfig {
            max_bytes: usize::MAX,
            max_entries: 3,
        };
        let mut mt = MemTable::with_config(config);

        // Merge 4 clean entries — should evict to stay within max_entries.
        mt.merge(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.merge(b"b".to_vec(), Bytes::from_static(b"2"));
        mt.merge(b"c".to_vec(), Bytes::from_static(b"3"));
        mt.clean.run_pending_tasks();
        assert_eq!(mt.clean.entry_count(), 3);

        mt.merge(b"d".to_vec(), Bytes::from_static(b"4"));
        mt.clean.run_pending_tasks();
        assert!(
            mt.clean.entry_count() <= 3,
            "clean cache should not exceed max_entries, got {}",
            mt.clean.entry_count()
        );
    }

    #[test]
    fn dirty_entries_never_evicted() {
        let config = MemTableConfig {
            max_bytes: usize::MAX,
            max_entries: 2,
        };
        let mut mt = MemTable::with_config(config);

        // Put 3 entries (all dirty). None should be evicted.
        mt.put(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.put(b"b".to_vec(), Bytes::from_static(b"2"));
        mt.put(b"c".to_vec(), Bytes::from_static(b"3"));

        // All 3 dirty entries survive even though max_entries=2.
        assert!(mt.get(b"a").is_some());
        assert!(mt.get(b"b").is_some());
        assert!(mt.get(b"c").is_some());
        assert_eq!(mt.dirty.len(), 3);
    }

    #[test]
    fn flush_makes_entries_evictable() {
        let config = MemTableConfig {
            max_bytes: usize::MAX,
            max_entries: 2,
        };
        let mut mt = MemTable::with_config(config);

        // Put 2 dirty entries.
        mt.put(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.put(b"b".to_vec(), Bytes::from_static(b"2"));

        // Flush — now both are clean and in moka cache.
        let _ = mt.flush();
        mt.clean.run_pending_tasks();
        assert!(mt.dirty.is_empty());
        assert_eq!(mt.clean.entry_count(), 2);

        // Merge a 3rd entry — the cache should stay bounded. With W-TinyLFU
        // admission, the new entry may itself be rejected if existing entries
        // have higher estimated frequency, which is fine — the key invariant
        // is that flushed entries became evictable.
        mt.merge(b"c".to_vec(), Bytes::from_static(b"3"));
        mt.clean.run_pending_tasks();
        assert!(
            mt.clean.entry_count() <= 2,
            "should have evicted to stay at max_entries, got {}",
            mt.clean.entry_count()
        );
    }

    #[test]
    fn keys_with_prefix_filters_correctly() {
        let mut mt = MemTable::new();
        mt.put(b"scores:alice".to_vec(), Bytes::from_static(b"100"));
        mt.put(b"scores:bob".to_vec(), Bytes::from_static(b"200"));
        mt.put(b"timers:t1".to_vec(), Bytes::from_static(b"999"));

        let mut keys = mt.keys_with_prefix(b"scores:");
        keys.sort();
        assert_eq!(keys, vec![b"scores:alice".to_vec(), b"scores:bob".to_vec()]);
    }

    #[test]
    fn keys_with_prefix_excludes_tombstones() {
        let mut mt = MemTable::new();
        mt.put(b"map:a".to_vec(), Bytes::from_static(b"1"));
        mt.put(b"map:b".to_vec(), Bytes::from_static(b"2"));
        mt.delete(b"map:b".to_vec());

        let keys = mt.keys_with_prefix(b"map:");
        assert_eq!(keys, vec![b"map:a".to_vec()]);
    }

    #[test]
    fn keys_with_prefix_includes_clean_entries() {
        let mut mt = MemTable::new();
        mt.put(b"map:a".to_vec(), Bytes::from_static(b"1"));
        mt.put(b"map:b".to_vec(), Bytes::from_static(b"2"));
        mt.flush();
        mt.clean.run_pending_tasks();

        mt.put(b"map:c".to_vec(), Bytes::from_static(b"3"));

        let mut keys = mt.keys_with_prefix(b"map:");
        keys.sort();
        assert_eq!(
            keys,
            vec![b"map:a".to_vec(), b"map:b".to_vec(), b"map:c".to_vec()]
        );
    }

    #[test]
    fn byte_weighted_eviction() {
        // Use a small byte budget — entries are evicted by total weight.
        let config = MemTableConfig {
            max_bytes: 100,
            max_entries: 1_000,
        };
        let mut mt = MemTable::with_config(config);

        // Insert entries that total ~80 bytes (key 4 + value 16 = 20 each).
        for i in 0u32..4 {
            let key = format!("k{i:>3}").into_bytes();
            mt.merge(key, Bytes::from(vec![0u8; 16]));
        }
        mt.clean.run_pending_tasks();

        // All 4 should fit (~80 bytes < 100 byte budget).
        assert_eq!(mt.clean.entry_count(), 4);

        // Add more entries to exceed the budget.
        for i in 4u32..10 {
            let key = format!("k{i:>3}").into_bytes();
            mt.merge(key, Bytes::from(vec![0u8; 16]));
        }
        mt.clean.run_pending_tasks();

        // Cache should be bounded around the byte budget.
        let weighted = mt.clean.weighted_size();
        assert!(
            weighted <= 120, // allow some slack for async eviction
            "weighted size {weighted} should be near max_bytes=100"
        );
    }
}
