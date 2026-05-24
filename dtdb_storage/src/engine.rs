use crate::memtable::MemTable;
use crate::sstable::{SstableReader, SstableWriter};
use crate::wal::{Wal, WalEntry};
use crate::{DbKey, DbValue, EngineOptions, Result};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

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
    sstables: RwLock<BTreeMap<usize, Vec<Mutex<SstableReader>>>>,
    pub options: EngineOptions,
    write_mutex: Mutex<()>,
}

impl StorageEngine {
    /// Opens a StorageEngine directory.
    ///
    /// If SSTables and/or a WAL exist, it replays the WAL to recover data,
    /// flushes recovered data to a new SSTable, cleans up the WAL, and returns.
    pub fn open(dir_path: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
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
        let mut max_id = 0;
        let mut discovered_ssts = Vec::new();
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if stem.starts_with('L') {
                        let parts: Vec<&str> = stem[1..].split('_').collect();
                        if parts.len() == 2 {
                            if let (Ok(level), Ok(id)) =
                                (parts[0].parse::<usize>(), parts[1].parse::<u64>())
                            {
                                max_id = max_id.max(id);
                                discovered_ssts.push((level, id, path));
                            }
                        }
                    }
                }
            }
        }

        let mut sstables_map: BTreeMap<usize, Vec<Mutex<SstableReader>>> = BTreeMap::new();
        for (level, id, path) in discovered_ssts {
            let reader = SstableReader::open(&path, id, level)?;
            sstables_map
                .entry(level)
                .or_default()
                .push(Mutex::new(reader));
        }

        // Sort the files in each level:
        for (level, list) in sstables_map.iter_mut() {
            if *level == 0 {
                // L0: Sort by ID descending (newest first)
                list.sort_by(|a, b| {
                    let id_a = a.lock().unwrap().id;
                    let id_b = b.lock().unwrap().id;
                    id_b.cmp(&id_a)
                });
            } else {
                // L1+: Sort by their first key ascending (key range order)
                list.sort_by(|a, b| {
                    let r_a = a.lock().unwrap();
                    let r_b = b.lock().unwrap();
                    match (r_a.first_key(), r_b.first_key()) {
                        (Some(k_a), Some(k_b)) => k_a.cmp(k_b),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                });
            }
        }

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
                let next_id = max_id + 1;
                let sst_path = dir_path.join(format!("L0_{:05}.sst", next_id));
                let mut writer = SstableWriter::create(
                    &sst_path,
                    active_options.block_size_limit,
                    active_options.compression,
                )?;
                for (key, val) in memtable.entries() {
                    writer.append(key, val)?;
                }
                writer.finish()?;

                let reader = SstableReader::open(&sst_path, next_id, 0)?;
                sstables_map
                    .entry(0)
                    .or_default()
                    .insert(0, Mutex::new(reader));
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
            sstables: RwLock::new(sstables_map),
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

        // 2. Search SSTables on disk.
        let sstables_map = self.sstables.read().unwrap();

        // 2a. Search Level 0 SSTables (from newest to oldest).
        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let mut reader = sstable.lock().unwrap();
                if let Some(res) = reader.get(key)? {
                    return Ok(res); // Returns Some(value) or None if deleted (tombstone).
                }
            }
        }

        // 2b. Search Level 1, 2, ...
        // For each level >= 1, files are sorted and non-overlapping.
        // We can binary search the files in that level to find the single file that could contain `key`.
        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            if ssts.is_empty() {
                continue;
            }

            // Binary search the non-overlapping SSTables using key range
            let idx_res = ssts.binary_search_by(|sstable| {
                let r = sstable.lock().unwrap();
                let f_key = r.first_key().expect("Level 1+ SSTable must not be empty");
                let l_key = r.last_key();
                if key < f_key {
                    std::cmp::Ordering::Greater
                } else if key > l_key {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            });

            if let Ok(idx) = idx_res {
                let mut reader = ssts[idx].lock().unwrap();
                if let Some(res) = reader.get(key)? {
                    return Ok(res);
                }
            }
        }

        Ok(None)
    }

    /// Performs a range scan between `start` and `end` (inclusive) matching the filter.
    pub fn filtered_scan<F>(
        &self,
        start: &DbKey,
        end: &DbKey,
        filter: F,
    ) -> Result<Vec<(DbKey, DbValue)>>
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

        // 2. Scan SSTables.
        let sstables_map = self.sstables.read().unwrap();

        // 2a. Scan Level 0 files from newest to oldest (reverse chronological).
        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let mut reader = sstable.lock().unwrap();
                let entries = reader.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone()) {
                        if let Some(val) = v {
                            if filter(&k, &val) {
                                results.insert(k, val);
                            }
                        }
                    }
                }
            }
        }

        // 2b. Scan Level 1+ files.
        // For each Level >= 1:
        // We can scan them in order since they are sorted and non-overlapping.
        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            for sstable in ssts.iter() {
                let mut reader = sstable.lock().unwrap();
                // Check if the SSTable range overlaps with our scan range [start, end].
                let f_key = reader
                    .first_key()
                    .expect("Level 1+ SSTable must not be empty");
                let l_key = reader.last_key();
                if f_key > end || l_key < start {
                    continue; // No overlap
                }

                let entries = reader.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone()) {
                        if let Some(val) = v {
                            if filter(&k, &val) {
                                results.insert(k, val);
                            }
                        }
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    /// Triggers manual compaction.
    ///
    /// Runs leveled compaction rounds until all levels are within their limits.
    pub fn compact(&self) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        // 1. Flush active memtable to disk first so all data is in SSTables.
        let mem = self.memtable.read().unwrap();
        self.flush_memtable_internal(&mem)?;

        // 2. Force compact Level 0 if it contains files
        let has_l0 = {
            let sstables = self.sstables.read().unwrap();
            sstables.get(&0).map_or(false, |list| !list.is_empty())
        };
        if has_l0 {
            self.compact_level(0)?;
        }

        // 3. Run compaction rounds until settled
        self.compact_if_needed()?;

        Ok(())
    }

    /// Automatically checks and triggers compaction rounds until all levels are within their limits.
    pub fn compact_if_needed(&self) -> Result<()> {
        loop {
            if let Some(level) = self.find_level_to_compact() {
                self.compact_level(level)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Finds the first level that violates its capacity limit.
    fn find_level_to_compact(&self) -> Option<usize> {
        let sstables = self.sstables.read().unwrap();

        // 1. Check Level 0 file count
        if let Some(l0_ssts) = sstables.get(&0) {
            if l0_ssts.len() >= self.options.l0_compaction_threshold {
                return Some(0);
            }
        }

        // 2. Check Level 1+ total sizes
        for level in 1..self.options.max_level {
            if let Some(ssts) = sstables.get(&level) {
                let total_size: u64 = ssts.iter().map(|s| s.lock().unwrap().file_size()).sum();
                let limit = self.level_size_limit(level);
                if total_size > limit {
                    return Some(level);
                }
            }
        }

        None
    }

    /// Calculates the size limit for a given level in bytes.
    fn level_size_limit(&self, level: usize) -> u64 {
        if level == 0 {
            0
        } else {
            self.options.base_level_size_limit as u64
                * (self.options.level_size_multiplier.pow((level - 1) as u32) as u64)
        }
    }

    /// Helper to find the next available unique SSTable ID.
    fn get_next_id(&self) -> Result<u64> {
        let mut max_id = 0;
        for entry in fs::read_dir(&self.dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if stem.starts_with('L') {
                        let parts: Vec<&str> = stem[1..].split('_').collect();
                        if parts.len() == 2 {
                            if let Ok(id) = parts[1].parse::<u64>() {
                                max_id = max_id.max(id);
                            }
                        }
                    }
                }
            }
        }
        Ok(max_id + 1)
    }

    /// Performs compaction from `source_level` to `source_level + 1`.
    fn compact_level(&self, source_level: usize) -> Result<()> {
        let target_level = source_level + 1;
        if target_level > self.options.max_level {
            return Ok(()); // Already at max level
        }

        let mut source_files = Vec::new();
        let mut overlapping_target_files = Vec::new();
        let mut remaining_target_files = Vec::new();

        // 1. Select source files and compute overlapping target files
        {
            let mut sstables_guard = self.sstables.write().unwrap();

            if source_level == 0 {
                // Compact all L0 files
                source_files = sstables_guard.remove(&0).unwrap_or_default();
            } else if let Some(list) = sstables_guard.get_mut(&source_level) {
                // Compact the first file in Level L
                if !list.is_empty() {
                    source_files.push(list.remove(0));
                }
            }

            if source_files.is_empty() {
                return Ok(());
            }

            // Find key range of source files
            let mut min_key = None;
            let mut max_key = None;
            for sstable in &source_files {
                let reader = sstable.lock().unwrap();
                if let Some(fk) = reader.first_key() {
                    if min_key.as_ref().map_or(true, |k| fk < k) {
                        min_key = Some(fk.clone());
                    }
                }
                let lk = reader.last_key();
                if max_key.as_ref().map_or(true, |k| lk > k) {
                    max_key = Some(lk.clone());
                }
            }

            if let (Some(min_k), Some(max_k)) = (min_key, max_key) {
                // Select overlapping files in target level
                if let Some(target_list) = sstables_guard.remove(&target_level) {
                    for sstable in target_list {
                        let overlaps = {
                            let reader = sstable.lock().unwrap();
                            let fk = reader
                                .first_key()
                                .expect("Target SSTable must not be empty");
                            let lk = reader.last_key();
                            fk <= &max_k && lk >= &min_k
                        };
                        if overlaps {
                            overlapping_target_files.push(sstable);
                        } else {
                            remaining_target_files.push(sstable);
                        }
                    }
                }
                // Put non-overlapping target files back
                sstables_guard.insert(target_level, remaining_target_files);
            }
        } // Drop write lock on sstables map so reads can happen concurrently on unaffected levels

        // 2. Merge-sort all selected files
        let mut merged_data = BTreeMap::new();

        let mut l0_files_sorted = Vec::new();
        let mut other_files = Vec::new();
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f.lock().unwrap();
            if reader.level == 0 {
                l0_files_sorted.push(f);
            } else {
                other_files.push(f);
            }
        }
        l0_files_sorted.sort_by_key(|f| f.lock().unwrap().id);

        // Read Level 1+ files first (older data)
        for f in other_files {
            let mut reader = f.lock().unwrap();
            let entries = reader.read_all()?;
            for (k, v) in entries {
                merged_data.insert(k, v);
            }
        }
        // Read Level 0 files in chronological order (newer data)
        for f in l0_files_sorted {
            let mut reader = f.lock().unwrap();
            let entries = reader.read_all()?;
            for (k, v) in entries {
                merged_data.insert(k, v);
            }
        }

        // 3. Write merged data to new SSTables in target level, splitting by target size
        let mut new_sstables = Vec::new();
        let mut current_writer: Option<SstableWriter> = None;
        let mut current_path = None;
        let mut current_id = 0;
        let mut current_writer_uncompressed_bytes = 0;

        for (k, v) in merged_data {
            // Check if we should discard this tombstone.
            if v.is_none() {
                let mut exists_below = false;
                let sstables_guard = self.sstables.read().unwrap();
                for (level, ssts) in sstables_guard.iter() {
                    if *level > target_level {
                        for sst in ssts {
                            let (fk, lk) = {
                                let r = sst.lock().unwrap();
                                (r.first_key().cloned(), r.last_key().clone())
                            };
                            if let Some(fk_val) = fk {
                                if k >= fk_val && k <= lk {
                                    let mut r_mut = sst.lock().unwrap();
                                    if let Ok(Some(_)) = r_mut.get(&k) {
                                        exists_below = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if exists_below {
                        break;
                    }
                }
                if !exists_below {
                    // Purge tombstone!
                    continue;
                }
            }

            if current_writer.is_none() {
                current_id = self.get_next_id()?;
                let path = self
                    .dir_path
                    .join(format!("L{}_{:05}.sst", target_level, current_id));
                current_path = Some(path.clone());
                current_writer = Some(SstableWriter::create(
                    &path,
                    self.options.block_size_limit,
                    self.options.compression,
                )?);
                current_writer_uncompressed_bytes = 0;
            }

            let writer = current_writer.as_mut().unwrap();
            writer.append(k.clone(), v.clone())?;

            let entry_sz = match &k {
                DbKey::Int(_) => 8,
                DbKey::String(s) => s.len(),
            } + match &v {
                Some(DbValue::Int(_)) => 8,
                Some(DbValue::Float(_)) => 8,
                Some(DbValue::String(s)) => s.len(),
                Some(DbValue::Bytes(b)) => b.len(),
                None => 1,
            };
            current_writer_uncompressed_bytes += entry_sz;

            if current_writer_uncompressed_bytes >= self.options.sstable_target_size {
                let writer = current_writer.take().unwrap();
                writer.finish()?;
                let reader =
                    SstableReader::open(current_path.take().unwrap(), current_id, target_level)?;
                new_sstables.push(Mutex::new(reader));
            }
        }

        if let Some(writer) = current_writer.take() {
            writer.finish()?;
            let reader =
                SstableReader::open(current_path.take().unwrap(), current_id, target_level)?;
            new_sstables.push(Mutex::new(reader));
        }

        // 4. Update the active sstables map and delete old files
        {
            let mut sstables_guard = self.sstables.write().unwrap();

            // Add new sstables to target level
            let target_list = sstables_guard.entry(target_level).or_default();
            let old_target_ids: HashSet<u64> = overlapping_target_files
                .iter()
                .map(|f| f.lock().unwrap().id)
                .collect();
            target_list.retain(|f| !old_target_ids.contains(&f.lock().unwrap().id));
            target_list.extend(new_sstables);
            target_list.sort_by(|a, b| {
                let r_a = a.lock().unwrap();
                let r_b = b.lock().unwrap();
                match (r_a.first_key(), r_b.first_key()) {
                    (Some(k_a), Some(k_b)) => k_a.cmp(k_b),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }

        // Delete old files from disk
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f.lock().unwrap();
            let _ = fs::remove_file(&reader.path);
        }

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
        let next_id = self.get_next_id()?;
        let sst_path = self.dir_path.join(format!("L0_{:05}.sst", next_id));

        // 2. Write MemTable entries to the new SSTable.
        let mut writer = SstableWriter::create(
            &sst_path,
            self.options.block_size_limit,
            self.options.compression,
        )?;
        for (key, val) in entries {
            writer.append(key, val)?;
        }
        writer.finish()?;

        // 3. Register the new SSTable Reader (inserted at the beginning of L0).
        let reader = SstableReader::open(&sst_path, next_id, 0)?;
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.entry(0).or_default().insert(0, Mutex::new(reader));
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

        // 6. Automatically trigger leveled compaction check.
        self.compact_if_needed()?;

        Ok(())
    }
}
