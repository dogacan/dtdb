use crate::{DbKey, DbValue};
use std::collections::BTreeMap;
use std::sync::RwLock;

/// MemTable is an in-memory sorted write buffer.
///
/// It uses a standard library `BTreeMap` to store key-value pairs sorted.
/// Deletions are stored as "tombstones" represented by `None`.
///
/// To allow thread safety, we wrap the `BTreeMap` in a `RwLock`.
/// - `RwLock` is a reader-writer lock that allows multiple concurrent readers,
///   but only a single writer. In Rust, `RwLock` ensures safe concurrent access
///   without requiring a complex lock-free data structure for educational purposes.
pub struct MemTable {
    map: RwLock<BTreeMap<DbKey, Option<DbValue>>>,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    /// Creates a new, empty MemTable.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }

    /// Inserts a key-value pair into the MemTable.
    pub fn put(&self, key: DbKey, value: DbValue) {
        // `.write()` acquires exclusive write access. If another thread is reading
        // or writing, this thread will block until the lock is released.
        // `.unwrap()` handles the case of lock poisoning (when a thread panics while holding the lock).
        let mut map = self.map.write().unwrap();
        map.insert(key, Some(value));
    }

    /// Deletes a key from the MemTable by inserting a tombstone (`None`).
    pub fn delete(&self, key: DbKey) {
        let mut map = self.map.write().unwrap();
        map.insert(key, None);
    }

    /// Fetches a value from the MemTable.
    ///
    /// Returns:
    /// - `Some(Some(value))` if the key exists with a value.
    /// - `Some(None)` if the key has a tombstone (deleted).
    /// - `None` if the key is not present in the MemTable.
    pub fn get(&self, key: &DbKey) -> Option<Option<DbValue>> {
        // `.read()` acquires shared read access, allowing multiple readers concurrently.
        let map = self.map.read().unwrap();
        map.get(key).cloned()
    }

    /// Returns a snapshot of all key-value entries in the MemTable, sorted by key.
    /// Used when flushing the MemTable to disk or performing compaction.
    pub fn entries(&self) -> Vec<(DbKey, Option<DbValue>)> {
        let map = self.map.read().unwrap();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Performs a range scan between `start` and `end` (both inclusive) matching the filter.
    ///
    /// This demonstrates Rust's powerful closure features (`Fn`).
    pub fn scan<F>(&self, start: &DbKey, end: &DbKey, filter: F) -> Vec<(DbKey, DbValue)>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let map = self.map.read().unwrap();
        // `map.range(...)` uses BTreeMap's efficient search to scan only the keys
        // within the given range, avoiding a full table scan.
        map.range(start..=end)
            .filter_map(|(k, v)| {
                // If it's a tombstone, skip it. If it's a value, run the filter.
                if let Some(val) = v {
                    if filter(k, val) {
                        Some((k.clone(), val.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Clears all entries from the MemTable.
    pub fn clear(&self) {
        let mut map = self.map.write().unwrap();
        map.clear();
    }

    /// Estimates the memory usage of the MemTable in bytes.
    /// Used to decide when to flush the MemTable to disk.
    pub fn byte_size(&self) -> usize {
        let map = self.map.read().unwrap();
        let mut total = 0;
        for (key, val) in map.iter() {
            total += key.byte_size();
            total += match val {
                Some(DbValue::Int(_)) => 8,
                Some(DbValue::Float(_)) => 8,
                Some(DbValue::String(s)) => s.len(),
                Some(DbValue::Bytes(b)) => b.len(),
                Some(DbValue::Bool(_)) => 1,
                Some(DbValue::Null) => 1,
                None => 1, // Tombstone overhead
            };
        }
        total
    }
}
