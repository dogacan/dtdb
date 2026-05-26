use crate::manifest::Manifest;
use crate::memtable::MemTable;
use crate::sstable::{SstableReader, SstableWriter};
use crate::wal::{Wal, WalEntry};
use crate::{DbKey, DbValue, EngineOptions, Result, ScanIterator, ThreadSpawner};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageEngineStatistics {
    pub num_sstables: usize,
    pub total_sstable_size: u64,
    pub sstable_entries: u64,
    pub sstable_tombstones: u64,
    pub sstable_uncompressed_bytes: u64,
    pub memtable_entries: u64,
    pub memtable_tombstones: u64,
    pub memtable_uncompressed_bytes: u64,
}

pub struct StorageEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    dir_path: PathBuf,
    memtable: RwLock<Arc<MemTable>>,
    wal: Mutex<Wal>,
    sstables: RwLock<BTreeMap<usize, Vec<Arc<SstableReader>>>>,
    options: EngineOptions,
    write_mutex: Mutex<()>,
    compaction_mutex: Mutex<()>,
    compaction_signal: Mutex<CompactionSignal>,
    manifest_mutex: Mutex<()>,
    next_sst_id: AtomicU64,
    spawner: Arc<dyn ThreadSpawner>,
    block_cache: Option<Arc<crate::BlockCache>>,
}

#[derive(Default)]
struct CompactionSignal {
    pending: bool,
    running: bool,
}

impl StorageEngine {
    /// Opens a StorageEngine directory.
    pub fn open(dir_path: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
        Self::open_with_spawner(dir_path, options, Arc::new(crate::DefaultSpawner))
    }

    /// Opens a StorageEngine directory with a custom ThreadSpawner.
    pub fn open_with_spawner(
        dir_path: impl AsRef<Path>,
        options: EngineOptions,
        spawner: Arc<dyn ThreadSpawner>,
    ) -> Result<Self> {
        let inner = Arc::new(EngineInner::open(dir_path, options, spawner.clone())?);

        if let Some(ms) = inner.options.wal_sync_interval_ms
            && ms > 0
        {
            let inner_weak = Arc::downgrade(&inner);
            spawner.spawn(Box::new(move || {
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                    if let Some(engine) = inner_weak.upgrade() {
                        if let Err(e) = engine.sync_wal() {
                            eprintln!("Background WAL sync error: {:?}", e);
                        }
                    } else {
                        break;
                    }
                }
            }));
        }

        Ok(Self { inner })
    }

    pub fn put(&self, key: DbKey, value: DbValue) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: DbKey) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn write_batch(&self, entries: Vec<WalEntry>) -> Result<()> {
        self.inner.write_batch(entries)
    }

    pub fn get(&self, key: &DbKey) -> Result<Option<DbValue>> {
        self.inner.get(key)
    }

    pub fn multi_get(&self, keys: &[DbKey]) -> Result<Vec<Option<DbValue>>> {
        self.inner.multi_get(keys)
    }

    pub fn filtered_scan<F>(
        &self,
        start: &DbKey,
        end: &DbKey,
        filter: F,
    ) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        self.inner.filtered_scan(start, end, filter)
    }

    pub fn scan_iter(&self, start: &DbKey, end: &DbKey) -> Result<ScanIterator> {
        self.inner.scan_iter(start, end)
    }

    pub fn flush_memtable(&self) -> Result<()> {
        self.inner.flush_memtable()
    }

    pub fn compact(&self) -> Result<()> {
        self.inner.compact()
    }

    pub fn compact_if_needed(&self) -> Result<()> {
        self.inner.compact_if_needed()
    }

    pub fn get_statistics(&self) -> Result<StorageEngineStatistics> {
        self.inner.get_statistics()
    }
}

impl EngineInner {
    pub fn open(
        dir_path: impl AsRef<Path>,
        options: EngineOptions,
        spawner: Arc<dyn ThreadSpawner>,
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

        let block_cache = if active_options.block_cache_capacity > 0 {
            Some(Arc::new(Mutex::new(crate::LruCache::new(
                active_options.block_cache_capacity,
            ))))
        } else {
            None
        };

        // 1. Load or initialize/migrate the Manifest.
        let manifest_path = dir_path.join("manifest.bin");
        let manifest = if manifest_path.exists() {
            Manifest::load(&manifest_path)?
        } else {
            let mut active_sstables = HashSet::new();
            for entry in fs::read_dir(&dir_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "sst")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && stem.starts_with('L')
                {
                    let parts: Vec<&str> = stem[1..].split('_').collect();
                    if parts.len() == 2
                        && let (Ok(level), Ok(id)) =
                            (parts[0].parse::<usize>(), parts[1].parse::<u64>())
                    {
                        active_sstables.insert((level, id));
                    }
                }
            }
            let m = Manifest { active_sstables };
            m.save(&manifest_path)?;
            m
        };

        // 2. Discover active SSTables and clean up orphan/garbage files.
        let mut max_id = 0;
        let mut discovered_ssts = Vec::new();
        let mut files_to_delete = Vec::new();

        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                files_to_delete.push(path);
                continue;
            }

            if path.extension().is_some_and(|ext| ext == "sst")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && stem.starts_with('L')
            {
                let parts: Vec<&str> = stem[1..].split('_').collect();
                if parts.len() == 2
                    && let (Ok(level), Ok(id)) =
                        (parts[0].parse::<usize>(), parts[1].parse::<u64>())
                {
                    if manifest.active_sstables.contains(&(level, id)) {
                        max_id = max_id.max(id);
                        discovered_ssts.push((level, id, path));
                    } else {
                        files_to_delete.push(path);
                    }
                }
            }
        }

        // Delete orphan/garbage files
        for p in files_to_delete {
            let _ = fs::remove_file(p);
        }

        let mut sstables_map: BTreeMap<usize, Vec<Arc<SstableReader>>> = BTreeMap::new();
        for (level, id, path) in discovered_ssts {
            let reader = SstableReader::open(&path, id, level, block_cache.clone())?;
            sstables_map
                .entry(level)
                .or_default()
                .push(Arc::new(reader));
        }

        // Sort the files in each level:
        for (level, list) in sstables_map.iter_mut() {
            if *level == 0 {
                list.sort_by(|a, b| {
                    let id_a = a.id;
                    let id_b = b.id;
                    id_b.cmp(&id_a)
                });
            } else {
                list.sort_by(|a, b| {
                    let r_a = a;
                    let r_b = b;
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
            fn apply_entry(mem: &MemTable, ent: WalEntry) {
                match ent {
                    WalEntry::Put { key, value } => mem.put(key, value),
                    WalEntry::Delete { key } => mem.delete(key),
                    WalEntry::Batch(sub) => {
                        for e in sub {
                            apply_entry(mem, e);
                        }
                    }
                }
            }

            let entries = Wal::recover(&wal_path)?;
            for entry in entries {
                apply_entry(&memtable, entry);
            }

            if memtable.byte_size() > 0 {
                let next_id = max_id + 1;
                max_id = next_id;
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

                let reader = SstableReader::open(&sst_path, next_id, 0, block_cache.clone())?;
                sstables_map
                    .entry(0)
                    .or_default()
                    .insert(0, Arc::new(reader));
                memtable.clear();
            }

            fs::remove_file(&wal_path)?;
        }

        let wal = Wal::open(&wal_path, active_options.wal_sync_interval_ms)?;

        Ok(Self {
            dir_path,
            memtable: RwLock::new(memtable),
            wal: Mutex::new(wal),
            sstables: RwLock::new(sstables_map),
            options: active_options,
            write_mutex: Mutex::new(()),
            compaction_mutex: Mutex::new(()),
            compaction_signal: Mutex::new(CompactionSignal::default()),
            manifest_mutex: Mutex::new(()),
            next_sst_id: AtomicU64::new(max_id + 1),
            spawner,
            block_cache,
        })
    }

    pub fn put(self: &Arc<Self>, key: DbKey, value: DbValue) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_put(&key, &value)?;
        }

        let mem = self.memtable.read().unwrap();
        mem.put(key, value);

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

    pub fn delete(self: &Arc<Self>, key: DbKey) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_delete(&key)?;
        }

        let mem = self.memtable.read().unwrap();
        mem.delete(key);

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

    pub fn write_batch(self: &Arc<Self>, entries: Vec<WalEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let _write_lock = self.write_mutex.lock().unwrap();

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_batch(&entries)?;
        }

        {
            let mem = self.memtable.read().unwrap();
            for entry in entries {
                match entry {
                    WalEntry::Put { key, value } => mem.put(key, value),
                    WalEntry::Delete { key } => mem.delete(key),
                    WalEntry::Batch(_) => {}
                }
            }
        }

        let mem = self.memtable.read().unwrap();

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

    pub fn get(&self, key: &DbKey) -> Result<Option<DbValue>> {
        {
            let mem = self.memtable.read().unwrap();
            if let Some(res) = mem.get(key) {
                return Ok(res);
            }
        }

        let sstables_map = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                if let Some(res) = sstable.get(key)? {
                    return Ok(res);
                }
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            if ssts.is_empty() {
                continue;
            }

            let idx_res = ssts.binary_search_by(|sstable| {
                let f_key = sstable
                    .first_key()
                    .expect("Level 1+ SSTable must not be empty");
                let l_key = sstable.last_key();
                if key < f_key {
                    std::cmp::Ordering::Greater
                } else if key > l_key {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            });

            if let Ok(idx) = idx_res
                && let Some(res) = ssts[idx].get(key)?
            {
                return Ok(res);
            }
        }

        Ok(None)
    }

    pub fn multi_get(&self, keys: &[DbKey]) -> Result<Vec<Option<DbValue>>> {
        let mut results = vec![None; keys.len()];
        let mut remaining_indices: Vec<usize> = (0..keys.len()).collect();

        // 1. Check Memtable under a single read lock
        {
            let mem = self.memtable.read().unwrap();
            let mut i = 0;
            while i < remaining_indices.len() {
                let idx = remaining_indices[i];
                let key = &keys[idx];
                if let Some(res) = mem.get(key) {
                    results[idx] = res;
                    remaining_indices.swap_remove(i);
                } else {
                    i += 1;
                }
            }
        }

        if remaining_indices.is_empty() {
            return Ok(results);
        }

        // 2. Check SSTables under a single read lock
        let sstables_map = self.sstables.read().unwrap();

        // Check L0
        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let mut i = 0;
                while i < remaining_indices.len() {
                    let idx = remaining_indices[i];
                    let key = &keys[idx];
                    if let Some(res) = sstable.get(key)? {
                        results[idx] = res;
                        remaining_indices.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if remaining_indices.is_empty() {
                    return Ok(results);
                }
            }
        }

        // Check Leveled SSTables (Level 1+)
        for (level, ssts) in sstables_map.iter() {
            if *level == 0 || ssts.is_empty() {
                continue;
            }

            let mut i = 0;
            while i < remaining_indices.len() {
                let idx = remaining_indices[i];
                let key = &keys[idx];

                let idx_res = ssts.binary_search_by(|sstable| {
                    let f_key = sstable
                        .first_key()
                        .expect("Level 1+ SSTable must not be empty");
                    let l_key = sstable.last_key();
                    if key < f_key {
                        std::cmp::Ordering::Greater
                    } else if key > l_key {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });

                if let Ok(sst_idx) = idx_res
                    && let Some(res) = ssts[sst_idx].get(key)?
                {
                    results[idx] = res;
                    remaining_indices.swap_remove(i);
                } else {
                    i += 1;
                }
            }

            if remaining_indices.is_empty() {
                return Ok(results);
            }
        }

        Ok(results)
    }

    pub fn scan_iter(&self, start: &DbKey, end: &DbKey) -> Result<ScanIterator> {
        let mem = self.memtable.read().unwrap();
        let mem_entries = mem.scan_range_raw(start, end);

        let sstables_map = self.sstables.read().unwrap();
        let mut sst_iters = Vec::new();
        let mut next_priority = 0;

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                if let Some(fk) = sstable.first_key() {
                    let lk = sstable.last_key();
                    if fk <= end && lk >= start {
                        sst_iters.push(crate::merge_iter::SstableBlockIterator::new(
                            sstable.clone(),
                            next_priority,
                        )?);
                    }
                }
                next_priority += 1;
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            for sstable in ssts.iter() {
                if let Some(fk) = sstable.first_key() {
                    let lk = sstable.last_key();
                    if fk <= end && lk >= start {
                        sst_iters.push(crate::merge_iter::SstableBlockIterator::new(
                            sstable.clone(),
                            next_priority,
                        )?);
                        next_priority += 1;
                    }
                }
            }
        }

        // Decision (A): Owned iterator snapshots memtable range on construction.
        // No lifetime parameters are needed, avoiding complex lifetime annotations throughout the SQL engine.
        ScanIterator::new(mem_entries, sst_iters, end.clone())
    }

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

        {
            let mem = self.memtable.read().unwrap();
            for (k, v) in mem.scan_range_raw(start, end) {
                seen.insert(k.clone());
                if let Some(val) = v
                    && filter(&k, &val)
                {
                    results.insert(k, val);
                }
            }
        }

        let sstables_map = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let entries = sstable.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone())
                        && let Some(val) = v
                        && filter(&k, &val)
                    {
                        results.insert(k, val);
                    }
                }
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            for sstable in ssts.iter() {
                let f_key = sstable
                    .first_key()
                    .expect("Level 1+ SSTable must not be empty");
                let l_key = sstable.last_key();
                if f_key > end || l_key < start {
                    continue;
                }

                let entries = sstable.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone())
                        && let Some(val) = v
                        && filter(&k, &val)
                    {
                        results.insert(k, val);
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    pub fn flush_memtable(self: &Arc<Self>) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();
        let mem = self.memtable.read().unwrap();
        self.flush_memtable_internal(&mem)?;
        Ok(())
    }

    pub fn compact(self: &Arc<Self>) -> Result<()> {
        // 1. Flush active memtable to disk first so all data is in SSTables (holds write_mutex).
        {
            let _write_lock = self.write_mutex.lock().unwrap();
            let mem = self.memtable.read().unwrap();
            self.flush_memtable_internal_no_trigger(&mem)?;
        }

        // 2. Run compaction synchronously
        let _compaction_lock = self.compaction_mutex.lock().unwrap();

        let has_l0 = {
            let sstables = self.sstables.read().unwrap();
            sstables.get(&0).is_some_and(|list| !list.is_empty())
        };
        if has_l0 {
            self.compact_level(0)?;
        }

        self.compact_if_needed_locked()?;

        Ok(())
    }

    pub fn compact_if_needed(&self) -> Result<()> {
        let _lock = self.compaction_mutex.lock().unwrap();
        self.compact_if_needed_locked()
    }

    fn compact_if_needed_locked(&self) -> Result<()> {
        while let Some(level) = self.find_level_to_compact() {
            self.compact_level(level)?;
        }
        Ok(())
    }

    pub fn get_statistics(&self) -> Result<StorageEngineStatistics> {
        let sstables_guard = self.sstables.read().unwrap();
        let memtable_guard = self.memtable.read().unwrap();

        let mut num_sstables = 0;
        let mut total_sstable_size = 0;
        let mut sstable_entries = 0;
        let mut sstable_tombstones = 0;
        let mut sstable_uncompressed_bytes = 0;

        for ssts in sstables_guard.values() {
            for sst in ssts {
                let reader = sst;
                num_sstables += 1;
                total_sstable_size += reader.file_size();
                sstable_entries += reader.stats.num_entries;
                sstable_tombstones += reader.stats.num_tombstones;
                sstable_uncompressed_bytes += reader.stats.total_uncompressed_bytes;
            }
        }

        let (memtable_entries, memtable_tombstones) = memtable_guard.entry_counts();
        let memtable_uncompressed_bytes = memtable_guard.byte_size() as u64;

        Ok(StorageEngineStatistics {
            num_sstables,
            total_sstable_size,
            sstable_entries,
            sstable_tombstones,
            sstable_uncompressed_bytes,
            memtable_entries: memtable_entries as u64,
            memtable_tombstones: memtable_tombstones as u64,
            memtable_uncompressed_bytes,
        })
    }

    fn find_level_to_compact(&self) -> Option<usize> {
        let sstables = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables.get(&0)
            && l0_ssts.len() >= self.options.l0_compaction_threshold
        {
            return Some(0);
        }

        for level in 1..self.options.max_level {
            if let Some(ssts) = sstables.get(&level) {
                let total_size: u64 = ssts.iter().map(|s| s.file_size()).sum();
                let limit = self.level_size_limit(level);
                if total_size > limit {
                    return Some(level);
                }
            }
        }

        None
    }

    fn level_size_limit(&self, level: usize) -> u64 {
        if level == 0 {
            0
        } else {
            self.options.base_level_size_limit as u64
                * (self.options.level_size_multiplier.pow((level - 1) as u32) as u64)
        }
    }

    fn get_next_id(&self) -> u64 {
        self.next_sst_id.fetch_add(1, Ordering::SeqCst)
    }

    fn compact_level(&self, source_level: usize) -> Result<()> {
        let target_level = source_level + 1;
        if target_level > self.options.max_level {
            return Ok(());
        }

        let mut source_files = Vec::new();
        let mut overlapping_target_files = Vec::new();

        // 1. Select source files and compute overlapping target files under read lock
        {
            let sstables_guard = self.sstables.read().unwrap();

            if source_level == 0 {
                if let Some(list) = sstables_guard.get(&0) {
                    source_files = list.clone();
                }
            } else if let Some(list) = sstables_guard.get(&source_level)
                && !list.is_empty()
            {
                source_files.push(list[0].clone());
            }

            if source_files.is_empty() {
                return Ok(());
            }

            let mut min_key = None;
            let mut max_key = None;
            for sstable in &source_files {
                let reader = sstable;
                if let Some(fk) = reader.first_key()
                    && min_key.as_ref().is_none_or(|k| fk < k)
                {
                    min_key = Some(fk.clone());
                }
                let lk = reader.last_key();
                if max_key.as_ref().is_none_or(|k| lk > k) {
                    max_key = Some(lk.clone());
                }
            }

            if let (Some(min_k), Some(max_k)) = (min_key, max_key)
                && let Some(target_list) = sstables_guard.get(&target_level)
            {
                for sstable in target_list {
                    let overlaps = {
                        let reader = sstable;
                        let fk = reader
                            .first_key()
                            .expect("Target SSTable must not be empty");
                        let lk = reader.last_key();
                        fk <= &max_k && lk >= &min_k
                    };
                    if overlaps {
                        overlapping_target_files.push(sstable.clone());
                    }
                }
            }
        }

        // 2. Merge-sort all selected files (locks released) using streaming k-way merge iterator
        let mut l0_files_sorted = Vec::new();
        let mut other_files = Vec::new();
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f;
            if reader.level == 0 {
                l0_files_sorted.push(f.clone());
            } else {
                other_files.push(f.clone());
            }
        }
        l0_files_sorted.sort_by_key(|f| f.id);

        let mut sources = Vec::new();
        let l0_count = l0_files_sorted.len();
        for (i, f) in other_files.iter().enumerate() {
            sources.push(crate::merge_iter::SstableBlockIterator::new(
                f.clone(),
                l0_count + i,
            )?);
        }
        for (i, f) in l0_files_sorted.iter().enumerate().rev() {
            let priority = l0_count - 1 - i;
            sources.push(crate::merge_iter::SstableBlockIterator::new(
                f.clone(),
                priority,
            )?);
        }

        let mut merge_iter = crate::merge_iter::MergeIterator::new(sources)?;

        // 3. Write merged data to new SSTables
        let mut new_sstables = Vec::new();
        let mut current_writer: Option<SstableWriter> = None;
        let mut current_path = None;
        let mut current_id = 0;
        let mut current_writer_uncompressed_bytes = 0;

        while let Some((k, v)) = merge_iter.next()? {
            if v.is_none() {
                // Decision: To avoid expensive point lookups during compaction, we only
                // check if any SSTable in lower levels has a key range overlapping with the tombstone.
                // If there are no overlapping SSTables in lower levels (or if we are at max_level),
                // the tombstone is safe to drop. This has zero I/O cost as it only uses in-memory range metadata.
                if target_level < self.options.max_level {
                    let mut overlaps_below = false;
                    let sstables_guard = self.sstables.read().unwrap();
                    for (level, ssts) in sstables_guard.iter() {
                        if *level > target_level {
                            for sst in ssts {
                                if let Some(fk) = sst.first_key() {
                                    let lk = sst.last_key();
                                    if &k >= fk && &k <= lk {
                                        overlaps_below = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if overlaps_below {
                            break;
                        }
                    }
                    if !overlaps_below {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if current_writer.is_none() {
                current_id = self.get_next_id();
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

            let entry_sz = k.byte_size()
                + match &v {
                    Some(DbValue::Int(_)) => 8,
                    Some(DbValue::Float(_)) => 8,
                    Some(DbValue::String(s)) => s.len(),
                    Some(DbValue::Bytes(b)) => b.len(),
                    Some(DbValue::Bool(_)) => 1,
                    Some(DbValue::Null) => 1,
                    None => 1,
                };
            current_writer_uncompressed_bytes += entry_sz;

            let writer = current_writer.as_mut().unwrap();
            writer.append(k, v)?;

            if current_writer_uncompressed_bytes >= self.options.sstable_target_size {
                let writer = current_writer.take().unwrap();
                writer.finish()?;
                let reader = SstableReader::open(
                    current_path.take().unwrap(),
                    current_id,
                    target_level,
                    self.block_cache.clone(),
                )?;
                new_sstables.push(Arc::new(reader));
            }
        }

        if let Some(writer) = current_writer.take() {
            writer.finish()?;
            let reader = SstableReader::open(
                current_path.take().unwrap(),
                current_id,
                target_level,
                self.block_cache.clone(),
            )?;
            new_sstables.push(Arc::new(reader));
        }

        // Update manifest under manifest_mutex lock
        {
            let _manifest_lock = self.manifest_mutex.lock().unwrap();
            let manifest_path = self.dir_path.join("manifest.bin");
            let mut manifest = Manifest::load(&manifest_path)?;
            for f in source_files.iter().chain(overlapping_target_files.iter()) {
                let reader = f;
                manifest.active_sstables.remove(&(reader.level, reader.id));
            }
            for f in &new_sstables {
                let reader = f;
                manifest.active_sstables.insert((reader.level, reader.id));
            }
            manifest.save(&manifest_path)?;
        }

        // 4. Update the active sstables map
        {
            let mut sstables_guard = self.sstables.write().unwrap();

            let source_ids: HashSet<u64> = source_files.iter().map(|f| f.id).collect();
            if let Some(source_list) = sstables_guard.get_mut(&source_level) {
                source_list.retain(|f| !source_ids.contains(&f.id));
            }

            let target_ids: HashSet<u64> = overlapping_target_files.iter().map(|f| f.id).collect();
            let target_list = sstables_guard.entry(target_level).or_default();
            target_list.retain(|f| !target_ids.contains(&f.id));
            target_list.extend(new_sstables);
            target_list.sort_by(|a, b| {
                let r_a = a;
                let r_b = b;
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
            let reader = f;
            let _ = fs::remove_file(&reader.path);
        }

        Ok(())
    }

    fn flush_memtable_internal(self: &Arc<Self>, mem: &MemTable) -> Result<()> {
        self.flush_memtable_internal_no_trigger(mem)?;
        self.trigger_compaction();
        Ok(())
    }

    fn flush_memtable_internal_no_trigger(&self, mem: &MemTable) -> Result<()> {
        let entries = mem.entries();
        if entries.is_empty() {
            return Ok(());
        }

        let next_id = self.get_next_id();
        let sst_path = self.dir_path.join(format!("L0_{:05}.sst", next_id));

        let mut writer = SstableWriter::create(
            &sst_path,
            self.options.block_size_limit,
            self.options.compression,
        )?;
        for (key, val) in entries {
            writer.append(key, val)?;
        }
        writer.finish()?;

        // Update manifest under manifest_mutex lock
        {
            let _manifest_lock = self.manifest_mutex.lock().unwrap();
            let manifest_path = self.dir_path.join("manifest.bin");
            let mut manifest = Manifest::load(&manifest_path)?;
            manifest.active_sstables.insert((0, next_id));
            manifest.save(&manifest_path)?;
        }

        let reader = SstableReader::open(&sst_path, next_id, 0, self.block_cache.clone())?;
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.entry(0).or_default().insert(0, Arc::new(reader));
        }

        let temp_wal_path = self.dir_path.join("active.wal.tmp");
        let new_wal = Wal::open(&temp_wal_path, self.options.wal_sync_interval_ms)?;

        let wal_path = self.dir_path.join("active.wal");
        {
            let mut wal_guard = self.wal.lock().unwrap();
            *wal_guard = new_wal;
        }

        fs::rename(&temp_wal_path, &wal_path)?;
        mem.clear();

        Ok(())
    }

    fn trigger_compaction(self: &Arc<Self>) {
        let mut sig = self.compaction_signal.lock().unwrap();
        sig.pending = true;
        if !sig.running {
            sig.running = true;
            let inner = self.clone();
            self.spawner.spawn(Box::new(move || {
                inner.run_compaction_loop();
            }));
        }
    }

    fn run_compaction_loop(self: Arc<Self>) {
        let _compaction_lock = self.compaction_mutex.lock().unwrap();
        loop {
            if let Err(e) = self.compact_if_needed_locked() {
                eprintln!("Background compaction error: {:?}", e);
                break;
            }

            let mut sig = self.compaction_signal.lock().unwrap();
            if !sig.pending {
                sig.running = false;
                break;
            }
            sig.pending = false;
        }
    }

    pub fn sync_wal(&self) -> Result<()> {
        let wal = self.wal.lock().unwrap();
        wal.sync_all()?;
        Ok(())
    }
}
