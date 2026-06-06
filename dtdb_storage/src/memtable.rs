use crate::{DbKey, DbValue};
use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    pub(crate) map: RwLock<BTreeMap<DbKey, Option<DbValue>>>,
    size_bytes: AtomicUsize,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Byte size of a stored slot, treating a tombstone (`None`) as 1 byte of
/// overhead. The value's own size comes from the shared [`DbValue::byte_size`].
fn option_value_byte_size(val: &Option<DbValue>) -> usize {
    val.as_ref().map_or(1, DbValue::byte_size)
}

impl MemTable {
    /// Creates a new, empty MemTable.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            size_bytes: AtomicUsize::new(0),
        }
    }

    /// Inserts a key-value pair into the MemTable.
    pub fn put(&self, key: DbKey, value: DbValue) {
        let key_size = key.byte_size();
        let new_val_size = value.byte_size();
        let mut map = self.map.write().unwrap();
        let old = map.insert(key, Some(value));
        match old {
            Some(old_val) => {
                let old_val_size = option_value_byte_size(&old_val);
                let delta = new_val_size as isize - old_val_size as isize;
                if delta >= 0 {
                    self.size_bytes.fetch_add(delta as usize, Ordering::Relaxed);
                } else {
                    self.size_bytes
                        .fetch_sub((-delta) as usize, Ordering::Relaxed);
                }
            }
            None => {
                self.size_bytes
                    .fetch_add(key_size + new_val_size, Ordering::Relaxed);
            }
        }
    }

    /// Deletes a key from the MemTable by inserting a tombstone (`None`).
    pub fn delete(&self, key: DbKey) {
        let key_size = key.byte_size();
        let mut map = self.map.write().unwrap();
        let old = map.insert(key, None);
        match old {
            Some(old_val) => {
                let old_val_size = option_value_byte_size(&old_val);
                let delta = 1isize - old_val_size as isize; // Tombstone overhead = 1
                if delta >= 0 {
                    self.size_bytes.fetch_add(delta as usize, Ordering::Relaxed);
                } else {
                    self.size_bytes
                        .fetch_sub((-delta) as usize, Ordering::Relaxed);
                }
            }
            None => {
                self.size_bytes.fetch_add(key_size + 1, Ordering::Relaxed);
            }
        }
    }

    /// Applies a batch of operations to the MemTable under a single write lock.
    pub fn apply_batch(&self, entries: Vec<crate::wal::WalEntry>) {
        let mut map = self.map.write().unwrap();
        let mut size_delta = 0isize;

        for ent in entries {
            match ent {
                crate::wal::WalEntry::Put { key, value } => {
                    let key_size = key.byte_size();
                    let new_val_size = value.byte_size();
                    let old = map.insert(key, Some(value));
                    match old {
                        Some(old_val) => {
                            let old_val_size = option_value_byte_size(&old_val);
                            size_delta += new_val_size as isize - old_val_size as isize;
                        }
                        None => {
                            size_delta += (key_size + new_val_size) as isize;
                        }
                    }
                }
                crate::wal::WalEntry::Delete { key } => {
                    let key_size = key.byte_size();
                    let old = map.insert(key, None);
                    match old {
                        Some(old_val) => {
                            let old_val_size = option_value_byte_size(&old_val);
                            size_delta += 1isize - old_val_size as isize;
                        }
                        None => {
                            size_delta += (key_size + 1) as isize;
                        }
                    }
                }
                crate::wal::WalEntry::Batch(_) => {}
            }
        }

        if size_delta >= 0 {
            self.size_bytes
                .fetch_add(size_delta as usize, Ordering::Relaxed);
        } else {
            self.size_bytes
                .fetch_sub((-size_delta) as usize, Ordering::Relaxed);
        }
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

    /// Performs a range scan returning all entries (including tombstones) in the range.
    pub fn scan_range_raw(&self, start: &DbKey, end: &DbKey) -> Vec<(DbKey, Option<DbValue>)> {
        let map = self.map.read().unwrap();
        map.range(start.clone()..=end.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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
        self.size_bytes.store(0, Ordering::Relaxed);
    }

    /// Returns the total number of entries and number of tombstones currently in the MemTable.
    pub fn entry_counts(&self) -> (usize, usize) {
        let map = self.map.read().unwrap();
        let total = map.len();
        let tombstones = map.values().filter(|v| v.is_none()).count();
        (total, tombstones)
    }

    /// Estimates the memory usage of the MemTable in bytes.
    /// Used to decide when to decide when to flush the MemTable to disk.
    pub fn byte_size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Returns the number of entries in the MemTable.
    pub fn len(&self) -> usize {
        let map = self.map.read().unwrap();
        map.len()
    }

    /// Returns true if the MemTable is empty.
    pub fn is_empty(&self) -> bool {
        let map = self.map.read().unwrap();
        map.is_empty()
    }
}
