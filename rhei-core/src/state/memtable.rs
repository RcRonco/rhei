use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;

/// Configuration for bounding the L1 `MemTable` size.
#[derive(Debug, Clone)]
pub struct MemTableConfig {
    /// Maximum approximate bytes (key + value sizes) before evicting clean entries.
    /// Default: 32 MiB.
    pub max_bytes: usize,
    /// Maximum number of entries before evicting clean entries.
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

/// L1 in-memory write buffer with dirty tracking and LRU eviction.
///
/// All writes go here first (synchronous, fast). Dirty keys are flushed to the
/// backend during checkpoint. Clean entries (already persisted to L2/L3) are
/// evicted in LRU order to bound memory usage per [`MemTableConfig`].
#[derive(Debug)]
pub struct MemTable {
    data: HashMap<Vec<u8>, Option<Bytes>>,
    dirty: HashSet<Vec<u8>>,
    config: MemTableConfig,
    /// LRU queue tracking access order for clean entries only.
    /// Front = oldest (evict first), back = most recently accessed.
    lru: VecDeque<Vec<u8>>,
    /// Running tally of `key.len() + value.len()` for all entries in `data`.
    approx_bytes: usize,
}

impl MemTable {
    /// Creates a memtable with the default [`MemTableConfig`].
    pub fn new() -> Self {
        Self::with_config(MemTableConfig::default())
    }

    /// Creates a memtable with the given configuration.
    pub fn with_config(config: MemTableConfig) -> Self {
        Self {
            data: HashMap::new(),
            dirty: HashSet::new(),
            config,
            lru: VecDeque::new(),
            approx_bytes: 0,
        }
    }

    /// Looks up a key. Returns `None` on cache miss, `Some(None)` for a
    /// tombstone, or `Some(Some(v))` for a value.
    ///
    /// On hit of a clean entry, promotes it in the LRU queue.
    pub fn get(&mut self, key: &[u8]) -> Option<Option<&Bytes>> {
        // Check existence first to avoid borrow issues.
        if !self.data.contains_key(key) {
            return None;
        }

        // Promote clean entries in LRU (dirty entries aren't tracked in LRU).
        if !self.dirty.contains(key) {
            self.lru_promote(key);
        }

        self.data.get(key).map(|v| v.as_ref())
    }

    /// Inserts a key-value pair and marks the key as dirty.
    pub fn put(&mut self, key: Vec<u8>, value: Bytes) {
        let new_bytes = key.len() + value.len();

        // Subtract old entry size if overwriting.
        if let Some(old_value) = self.data.get(&key) {
            let old_bytes = key.len() + old_value.as_ref().map_or(0, Bytes::len);
            self.approx_bytes = self.approx_bytes.saturating_sub(old_bytes);
        }

        // If this key was clean (in LRU), remove it — it's now dirty.
        if !self.dirty.contains(&key) {
            self.lru_remove(&key);
        }

        self.dirty.insert(key.clone());
        self.data.insert(key, Some(value));
        self.approx_bytes += new_bytes;
    }

    /// Records a tombstone for the key and marks it as dirty.
    pub fn delete(&mut self, key: Vec<u8>) {
        let new_bytes = key.len(); // tombstone: key only, no value

        // Subtract old entry size if overwriting.
        if let Some(old_value) = self.data.get(&key) {
            let old_bytes = key.len() + old_value.as_ref().map_or(0, Bytes::len);
            self.approx_bytes = self.approx_bytes.saturating_sub(old_bytes);
        }

        // If this key was clean (in LRU), remove it — it's now dirty.
        if !self.dirty.contains(&key) {
            self.lru_remove(&key);
        }

        self.dirty.insert(key.clone());
        self.data.insert(key, None);
        self.approx_bytes += new_bytes;
    }

    /// Returns dirty entries and clears the dirty set.
    /// Each entry is `(key, Option<value>)` — `None` means the key was deleted.
    ///
    /// After flush, all remaining entries are clean. The LRU queue is rebuilt
    /// from `data.keys()` so that previously-dirty entries become evictable.
    pub fn flush(&mut self) -> Vec<(Vec<u8>, Option<Bytes>)> {
        let entries: Vec<(Vec<u8>, Option<Bytes>)> = self
            .dirty
            .drain()
            .filter_map(|key| {
                let value = self.data.get(&key)?.clone();
                Some((key, value))
            })
            .collect();

        // After flush, all remaining entries are clean — rebuild LRU.
        self.lru.clear();
        for key in self.data.keys() {
            self.lru.push_back(key.clone());
        }

        entries
    }

    /// Load data from the backend into the memtable (only for keys not already present).
    pub fn merge(&mut self, key: Vec<u8>, value: Bytes) {
        if self.data.contains_key(&key) {
            return;
        }

        let entry_bytes = key.len() + value.len();
        self.approx_bytes += entry_bytes;

        // New merged entry is clean — add to LRU.
        self.lru.push_back(key.clone());

        self.data.insert(key, Some(value));
        self.maybe_evict();
    }

    /// Evict the oldest clean entries until we're within configured limits.
    fn maybe_evict(&mut self) {
        while (self.approx_bytes > self.config.max_bytes
            || self.data.len() > self.config.max_entries)
            && !self.lru.is_empty()
        {
            let key = self.lru.pop_front().unwrap();
            if self.dirty.contains(&key) {
                // Dirty entry shouldn't be in LRU, but be safe — skip.
                continue;
            }
            if let Some(entry) = self.data.remove(&key) {
                let entry_bytes = key.len() + entry.as_ref().map_or(0, Bytes::len);
                self.approx_bytes = self.approx_bytes.saturating_sub(entry_bytes);
            }
        }
    }

    /// Promote a key to the back of the LRU queue (most recently used).
    fn lru_promote(&mut self, key: &[u8]) {
        if let Some(pos) = self.lru.iter().position(|k| k.as_slice() == key) {
            self.lru.remove(pos);
            self.lru.push_back(key.to_vec());
        }
    }

    /// Remove a key from the LRU queue.
    fn lru_remove(&mut self, key: &[u8]) {
        if let Some(pos) = self.lru.iter().position(|k| k.as_slice() == key) {
            self.lru.remove(pos);
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), Bytes::from_static(b"val1"));
        assert_eq!(mt.get(b"key1"), Some(Some(&Bytes::from_static(b"val1"))));
    }

    #[test]
    fn get_miss() {
        let mut mt = MemTable::new();
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn delete_marks_tombstone() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), Bytes::from_static(b"val1"));
        mt.delete(b"key1".to_vec());
        // Should return Some(None) indicating the key was explicitly deleted
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

        // Second flush should be empty
        assert!(mt.flush().is_empty());
    }

    #[test]
    fn merge_does_not_overwrite() {
        let mut mt = MemTable::new();
        mt.put(b"key".to_vec(), Bytes::from_static(b"local"));
        mt.merge(b"key".to_vec(), Bytes::from_static(b"backend"));
        assert_eq!(mt.get(b"key"), Some(Some(&Bytes::from_static(b"local"))));
    }

    #[test]
    fn merge_fills_missing() {
        let mut mt = MemTable::new();
        mt.merge(b"key".to_vec(), Bytes::from_static(b"backend"));
        assert_eq!(mt.get(b"key"), Some(Some(&Bytes::from_static(b"backend"))));
    }

    #[test]
    fn eviction_when_over_max_entries() {
        let config = MemTableConfig {
            max_bytes: usize::MAX,
            max_entries: 3,
        };
        let mut mt = MemTable::with_config(config);

        // Merge 4 clean entries — should evict the oldest one.
        mt.merge(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.merge(b"b".to_vec(), Bytes::from_static(b"2"));
        mt.merge(b"c".to_vec(), Bytes::from_static(b"3"));
        // At this point we have 3 entries = max_entries, no eviction yet.
        assert_eq!(mt.data.len(), 3);

        mt.merge(b"d".to_vec(), Bytes::from_static(b"4"));
        // Now we exceeded max_entries — "a" (oldest LRU entry) should be evicted.
        assert_eq!(mt.data.len(), 3);
        assert!(mt.get(b"a").is_none());
        assert!(mt.get(b"b").is_some());
        assert!(mt.get(b"c").is_some());
        assert!(mt.get(b"d").is_some());
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
        assert_eq!(mt.data.len(), 3);
    }

    #[test]
    fn lru_evicts_oldest_first() {
        let config = MemTableConfig {
            max_bytes: usize::MAX,
            max_entries: 3,
        };
        let mut mt = MemTable::with_config(config);

        mt.merge(b"a".to_vec(), Bytes::from_static(b"1"));
        mt.merge(b"b".to_vec(), Bytes::from_static(b"2"));
        mt.merge(b"c".to_vec(), Bytes::from_static(b"3"));

        // Access "a" to promote it — now LRU order is b, c, a.
        let _ = mt.get(b"a");

        // Insert a 4th entry — "b" (oldest untouched) should be evicted.
        mt.merge(b"d".to_vec(), Bytes::from_static(b"4"));
        assert!(
            mt.get(b"b").is_none(),
            "b should have been evicted (oldest)"
        );
        assert!(
            mt.get(b"a").is_some(),
            "a should survive (recently accessed)"
        );
        assert!(mt.get(b"c").is_some());
        assert!(mt.get(b"d").is_some());
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

        // Flush — now both are clean and in LRU.
        let _ = mt.flush();
        assert_eq!(mt.data.len(), 2);

        // Merge a 3rd entry — should evict one of the now-clean entries.
        mt.merge(b"c".to_vec(), Bytes::from_static(b"3"));
        assert_eq!(
            mt.data.len(),
            2,
            "should have evicted to stay at max_entries"
        );
        assert!(mt.get(b"c").is_some(), "newly merged entry should exist");
    }
}
