use std::collections::{HashMap, HashSet};

/// L1 in-memory write buffer with dirty tracking.
///
/// All writes go here first (synchronous, fast). Dirty keys are flushed to the
/// backend during checkpoint.
#[derive(Debug)]
pub struct MemTable {
    data: HashMap<Vec<u8>, Option<Vec<u8>>>,
    dirty: HashSet<Vec<u8>>,
}

impl MemTable {
    /// Creates an empty memtable.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            dirty: HashSet::new(),
        }
    }

    /// Looks up a key. Returns `None` on cache miss, `Some(None)` for a tombstone, or `Some(Some(v))` for a value.
    pub fn get(&self, key: &[u8]) -> Option<Option<&Vec<u8>>> {
        // Returns:
        //   None           -> key not in memtable (cache miss)
        //   Some(None)     -> key was deleted in memtable
        //   Some(Some(v))  -> key has a value in memtable
        self.data.get(key).map(|v| v.as_ref())
    }

    /// Inserts a key-value pair and marks the key as dirty.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.dirty.insert(key.clone());
        self.data.insert(key, Some(value));
    }

    /// Records a tombstone for the key and marks it as dirty.
    pub fn delete(&mut self, key: Vec<u8>) {
        self.dirty.insert(key.clone());
        self.data.insert(key, None);
    }

    /// Returns dirty entries and clears the dirty set.
    /// Each entry is `(key, Option<value>)` — `None` means the key was deleted.
    pub fn flush(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .dirty
            .drain()
            .filter_map(|key| {
                let value = self.data.get(&key)?.clone();
                Some((key, value))
            })
            .collect();
        entries
    }

    /// Load data from the backend into the memtable (only for keys not already present).
    pub fn merge(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.entry(key).or_insert(Some(value));
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
        mt.put(b"key1".to_vec(), b"val1".to_vec());
        assert_eq!(mt.get(b"key1"), Some(Some(&b"val1".to_vec())));
    }

    #[test]
    fn get_miss() {
        let mt = MemTable::new();
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn delete_marks_tombstone() {
        let mut mt = MemTable::new();
        mt.put(b"key1".to_vec(), b"val1".to_vec());
        mt.delete(b"key1".to_vec());
        // Should return Some(None) indicating the key was explicitly deleted
        assert_eq!(mt.get(b"key1"), Some(None));
    }

    #[test]
    fn flush_returns_dirty_entries() {
        let mut mt = MemTable::new();
        mt.put(b"a".to_vec(), b"1".to_vec());
        mt.put(b"b".to_vec(), b"2".to_vec());
        mt.delete(b"c".to_vec());

        let mut flushed = mt.flush();
        flushed.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(flushed.len(), 3);
        assert_eq!(flushed[0], (b"a".to_vec(), Some(b"1".to_vec())));
        assert_eq!(flushed[1], (b"b".to_vec(), Some(b"2".to_vec())));
        assert_eq!(flushed[2], (b"c".to_vec(), None));

        // Second flush should be empty
        assert!(mt.flush().is_empty());
    }

    #[test]
    fn merge_does_not_overwrite() {
        let mut mt = MemTable::new();
        mt.put(b"key".to_vec(), b"local".to_vec());
        mt.merge(b"key".to_vec(), b"backend".to_vec());
        assert_eq!(mt.get(b"key"), Some(Some(&b"local".to_vec())));
    }

    #[test]
    fn merge_fills_missing() {
        let mut mt = MemTable::new();
        mt.merge(b"key".to_vec(), b"backend".to_vec());
        assert_eq!(mt.get(b"key"), Some(Some(&b"backend".to_vec())));
    }
}
