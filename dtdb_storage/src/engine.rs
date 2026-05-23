use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::{DbKey, DbValue, Result, EngineOptions};
use crate::memtable::MemTable;
use crate::wal::{Wal, WalEntry};
use crate::sstable::{SstableReader, SstableWriter};

/// StorageEngine coordinates the LSM-tree storage components:
/// - An active in-memory MemTable.
/// - An active Write-Ahead Log (WAL) on disk.
/// - A list of read-only Sorted String Tables (SSTables) on disk.
///
/// Thread Safety Design in Rust:
/// 1. `Arc` (Atomically Reference Counted pointer): Allows shared ownership
///    of the MemTable and SSTables. We can safely clone the StorageEngine
///    and share it across threads.
/// 2. `RwLock` (Reader-Writer Lock): Wraps the memtable pointer and sstables list.
///    Multiple threads can read concurrently, but updating the list (during flush
///    or compaction) requires a write lock.
/// 3. `Mutex` (Mutual Exclusion Lock):
///    - Wraps `Wal` to serialize disk logging.
///    - Wraps individual `SstableReader` instances because reading/seeking on
///      a shared file descriptor alters the internal file cursor, making concurrent
///      reads unsafe without serialization.
///    - `write_mutex` is held during any mutating operation (`put`, `delete`, `compact`, `flush`)
///      to prevent parallel writes from interleaving.
pub struct StorageEngine {
    dir_path: PathBuf,
    memtable: RwLock<Arc<MemTable>>,
    wal: Mutex<Wal>,
    sstables: RwLock<Vec<Mutex<SstableReader>>>,
    pub options: EngineOptions,
    write_mutex: Mutex<()>,
}

impl StorageEngine {
    /// Opens a StorageEngine directory.
    ///
    /// If SSTables and/or a WAL exist, it replays the WAL to recover data,
    /// flushes recovered data to a new SSTable, cleans up the WAL, and returns.
    pub fn open(
        dir_path: impl AsRef<Path>,
        options: EngineOptions,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        // Load or save options.bin
        let options_path = dir_path.join("options.bin");
        let active_options = if options_path.exists() {
            let bytes = fs::read(&options_path)?;
            bincode::deserialize::<EngineOptions>(&bytes)?
        } else {
            let bytes = bincode::serialize(&options)?;
            fs::write(&options_path, bytes)?;
            options
        };

        // 1. Discover all SSTable files in the directory.
        let mut sst_files = Vec::new();
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = stem.parse::<u64>() {
                        sst_files.push((id, path));
                    }
                }
            }
        }
        // Sort SSTables by ID ascending (chronological order).
        sst_files.sort_by_key(|(id, _)| *id);

        let mut sstables = Vec::new();
        for (_, path) in &sst_files {
            let reader = SstableReader::open(path)?;
            sstables.push(Mutex::new(reader));
        }
        // For search paths, we want newest SSTables first, so we reverse the list.
        sstables.reverse();

        // 2. Perform recovery from WAL if it exists.
        let wal_path = dir_path.join("active.wal");
        let memtable = Arc::new(MemTable::new());

        if wal_path.exists() {
            let entries = Wal::recover(&wal_path)?;
            for entry in entries {
                match entry {
                    WalEntry::Put { key, value } => memtable.put(key, value),
                    WalEntry::Delete { key } => memtable.delete(key),
                }
            }

            // If we recovered data, flush it to disk immediately to start clean.
            if memtable.byte_size() > 0 {
                let next_id = sst_files.last().map(|(id, _)| id + 1).unwrap_or(1);
                let sst_path = dir_path.join(format!("{:05}.sst", next_id));
                let mut writer = SstableWriter::create(&sst_path, active_options.block_size_limit, active_options.compression)?;
                for (key, val) in memtable.entries() {
                    writer.append(key, val)?;
                }
                writer.finish()?;

                let reader = SstableReader::open(&sst_path)?;
                sstables.insert(0, Mutex::new(reader));
                memtable.clear();
            }

            // Remove the recovered WAL cleanly.
            fs::remove_file(&wal_path)?;
        }

        // 3. Open a fresh active WAL file.
        let wal = Wal::open(&wal_path)?;

        Ok(Self {
            dir_path,
            memtable: RwLock::new(memtable),
            wal: Mutex::new(wal),
            sstables: RwLock::new(sstables),
            options: active_options,
            write_mutex: Mutex::new(()),
        })
    }

    /// Writes a key-value pair to the database.
    pub fn put(&self, key: DbKey, value: DbValue) -> Result<()> {
        // Acquire the write mutex to serialize mutations.
        let _write_lock = self.write_mutex.lock().unwrap();

        // 1. Append to Write-Ahead Log first for durability.
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_put(&key, &value)?;
        }

        // 2. Insert into the in-memory MemTable.
        let mem = self.memtable.read().unwrap();
        mem.put(key, value);

        // 3. Check if memtable is full or WAL size exceeds limit. If so, trigger flush.
        let trigger_flush = {
            let mem_full = mem.byte_size() >= self.options.memtable_size_limit;
            let wal_full = {
                let wal = self.wal.lock().unwrap();
                if let Ok(size) = wal.size() {
                    size as usize >= self.options.wal_size_limit
                } else {
                    false
                }
            };
            mem_full || wal_full
        };

        if trigger_flush {
            self.flush_memtable_internal(&mem)?;
        }

        Ok(())
    }

    /// Deletes a key from the database by writing a tombstone.
    pub fn delete(&self, key: DbKey) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        // 1. Append delete operation to WAL.
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_delete(&key)?;
        }

        // 2. Insert tombstone into MemTable.
        let mem = self.memtable.read().unwrap();
        mem.delete(key);

        // 3. Check if memtable is full or WAL size exceeds limit. If so, trigger flush.
        let trigger_flush = {
            let mem_full = mem.byte_size() >= self.options.memtable_size_limit;
            let wal_full = {
                let wal = self.wal.lock().unwrap();
                if let Ok(size) = wal.size() {
                    size as usize >= self.options.wal_size_limit
                } else {
                    false
                }
            };
            mem_full || wal_full
        };

        if trigger_flush {
            self.flush_memtable_internal(&mem)?;
        }

        Ok(())
    }

    /// Explicitly flushes the active MemTable to disk.
    pub fn flush_memtable(&self) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();
        let mem = self.memtable.read().unwrap();
        self.flush_memtable_internal(&mem)?;
        Ok(())
    }

    /// Fetches a value by key.
    pub fn get(&self, key: &DbKey) -> Result<Option<DbValue>> {
        // 1. Search the active MemTable first (newest data).
        {
            let mem = self.memtable.read().unwrap();
            if let Some(res) = mem.get(key) {
                return Ok(res); // Returns Some(value) or None if deleted (tombstone).
            }
        }

        // 2. Search SSTables on disk from newest to oldest.
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter() {
            let mut reader = sstable.lock().unwrap();
            if let Some(res) = reader.get(key)? {
                return Ok(res); // Returns Some(value) or None if deleted (tombstone).
            }
        }

        Ok(None)
    }

    /// Performs a range scan between `start` and `end` (inclusive) matching the filter.
    pub fn filtered_scan<F>(&self, start: &DbKey, end: &DbKey, filter: F) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let mut seen = HashSet::new();
        let mut results = BTreeMap::new();

        // 1. Scan the MemTable first (newest).
        {
            let mem = self.memtable.read().unwrap();
            for (k, v) in mem.entries() {
                if k >= *start && k <= *end {
                    seen.insert(k.clone());
                    if let Some(val) = v {
                        if filter(&k, &val) {
                            results.insert(k, val);
                        }
                    }
                }
            }
        }

        // 2. Scan SSTables from newest to oldest.
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter() {
            let mut reader = sstable.lock().unwrap();
            let entries = reader.scan_raw(start, end)?;
            for (k, v) in entries {
                // If we have already seen this key in a newer layer (memtable or newer SST), skip it.
                if seen.insert(k.clone()) {
                    if let Some(val) = v {
                        if filter(&k, &val) {
                            results.insert(k, val);
                        }
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    /// Triggers manual compaction.
    ///
    /// Merges all SSTables and the active MemTable into a single optimized SSTable,
    /// removing duplicate keys and discarding tombstones.
    pub fn compact(&self) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        // 1. Flush active memtable to disk first so all data is in SSTables.
        let mem = self.memtable.read().unwrap();
        self.flush_memtable_internal(&mem)?;

        // 2. Read all entries from all SSTables.
        // We acquire a write lock on the SSTables list to freeze it.
        let mut sstables_guard = self.sstables.write().unwrap();
        if sstables_guard.is_empty() {
            return Ok(());
        }

        // We load all entries in chronological order (oldest to newest).
        // This is done by reversing the sstables list (which was newest first).
        let mut merged_data = BTreeMap::new();
        for sstable in sstables_guard.iter().rev() {
            let mut reader = sstable.lock().unwrap();
            let entries = reader.read_all()?;
            for (k, v) in entries {
                // Newer SSTable values naturally overwrite older ones in the BTreeMap.
                merged_data.insert(k, v);
            }
        }

        // 3. Write merged data to a new temporary SSTable.
        // Since we are merging all SSTables in the database, we can safely discard tombstones (None),
        // because there are no older records that they need to shadow.
        let temp_sst_path = self.dir_path.join("compacted.tmp");
        let mut writer = SstableWriter::create(&temp_sst_path, self.options.block_size_limit, self.options.compression)?;
        for (k, v) in merged_data {
            if let Some(val) = v {
                writer.append(k, Some(val))?;
            }
        }
        writer.finish()?;

        // 4. Close all open readers so files are not locked.
        // In Rust, dropping the `SstableReader` instances closes the file descriptors.
        sstables_guard.clear();

        // 5. Delete all old SSTable files from disk.
        for entry in fs::read_dir(&self.dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                fs::remove_file(path)?;
            }
        }

        // 6. Rename the compacted temporary file to "00001.sst".
        let final_sst_path = self.dir_path.join("00001.sst");
        fs::rename(&temp_sst_path, &final_sst_path)?;

        // 7. Re-open the single compacted SSTable.
        let reader = SstableReader::open(&final_sst_path)?;
        sstables_guard.push(Mutex::new(reader));

        Ok(())
    }

    /// Internal method to flush the current MemTable to disk.
    ///
    /// Must be called under the protection of `write_mutex`.
    fn flush_memtable_internal(&self, mem: &MemTable) -> Result<()> {
        let entries = mem.entries();
        if entries.is_empty() {
            return Ok(());
        }

        // 1. Determine next SSTable ID.
        let mut max_id = 0;
        for entry in fs::read_dir(&self.dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = stem.parse::<u64>() {
                        max_id = max_id.max(id);
                    }
                }
            }
        }
        let next_id = max_id + 1;
        let sst_path = self.dir_path.join(format!("{:05}.sst", next_id));

        // 2. Write MemTable entries to the new SSTable.
        let mut writer = SstableWriter::create(&sst_path, self.options.block_size_limit, self.options.compression)?;
        for (key, val) in entries {
            writer.append(key, val)?;
        }
        writer.finish()?;

        // 3. Register the new SSTable Reader (inserted at the beginning of sstables).
        let reader = SstableReader::open(&sst_path)?;
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.insert(0, Mutex::new(reader));
        }

        // 4. Rotate WAL file: create a new temporary WAL, swap it with active, and delete old.
        let temp_wal_path = self.dir_path.join("active.wal.tmp");
        let new_wal = Wal::open(&temp_wal_path)?;

        // Swap the wal mutex inner value.
        let wal_path = self.dir_path.join("active.wal");
        {
            let mut wal_guard = self.wal.lock().unwrap();
            *wal_guard = new_wal;
        }

        // Swap file names and delete the old log.
        fs::rename(&temp_wal_path, &wal_path)?;

        // 5. Clear the MemTable contents.
        mem.clear();

        Ok(())
    }
}

