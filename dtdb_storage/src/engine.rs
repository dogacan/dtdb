use crate::memtable::MemTable;
use crate::sstable::{SstableReader, SstableWriter};
use crate::wal::{Wal, WalEntry};
use crate::manifest::Manifest;
use crate::{DbKey, DbValue, EngineOptions, Result, ThreadSpawner};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct StorageEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    dir_path: PathBuf,
    memtable: RwLock<Arc<MemTable>>,
    wal: Mutex<Wal>,
    sstables: RwLock<BTreeMap<usize, Vec<Arc<Mutex<SstableReader>>>>>,
    options: EngineOptions,
    write_mutex: Mutex<()>,
    compaction_mutex: Mutex<()>,
    compaction_signal: Mutex<CompactionSignal>,
    manifest_mutex: Mutex<()>,
    next_sst_id: AtomicU64,
    spawner: Arc<dyn ThreadSpawner>,
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
        let inner = EngineInner::open(dir_path, options, spawner)?;
        Ok(Self { inner: Arc::new(inner) })
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

    pub fn filtered_scan<F>(&self, start: &DbKey, end: &DbKey, filter: F) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        self.inner.filtered_scan(start, end, filter)
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

        // 1. Load or initialize/migrate the Manifest.
        let manifest_path = dir_path.join("manifest.bin");
        let manifest = if manifest_path.exists() {
            Manifest::load(&manifest_path)?
        } else {
            let mut active_sstables = HashSet::new();
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
                                    active_sstables.insert((level, id));
                                }
                            }
                        }
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
            if path.extension().map_or(false, |ext| ext == "tmp") {
                files_to_delete.push(path);
                continue;
            }

            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if stem.starts_with('L') {
                        let parts: Vec<&str> = stem[1..].split('_').collect();
                        if parts.len() == 2 {
                            if let (Ok(level), Ok(id)) =
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
                }
            }
        }

        // Delete orphan/garbage files
        for p in files_to_delete {
            let _ = fs::remove_file(p);
        }

        let mut sstables_map: BTreeMap<usize, Vec<Arc<Mutex<SstableReader>>>> = BTreeMap::new();
        for (level, id, path) in discovered_ssts {
            let reader = SstableReader::open(&path, id, level)?;
            sstables_map
                .entry(level)
                .or_default()
                .push(Arc::new(Mutex::new(reader)));
        }

        // Sort the files in each level:
        for (level, list) in sstables_map.iter_mut() {
            if *level == 0 {
                list.sort_by(|a, b| {
                    let id_a = a.lock().unwrap().id;
                    let id_b = b.lock().unwrap().id;
                    id_b.cmp(&id_a)
                });
            } else {
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

                let reader = SstableReader::open(&sst_path, next_id, 0)?;
                sstables_map
                    .entry(0)
                    .or_default()
                    .insert(0, Arc::new(Mutex::new(reader)));
                memtable.clear();
            }

            fs::remove_file(&wal_path)?;
        }

        let wal = Wal::open(&wal_path)?;

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
            wal.append_batch(entries.clone())?;
        }

        {
            let mem = self.memtable.write().unwrap();
            for entry in &entries {
                match entry {
                    WalEntry::Put { key, value } => mem.put(key.clone(), value.clone()),
                    WalEntry::Delete { key } => mem.delete(key.clone()),
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
                let mut reader = sstable.lock().unwrap();
                if let Some(res) = reader.get(key)? {
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

    pub fn filtered_scan<F>(&self, start: &DbKey, end: &DbKey, filter: F) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let mut seen = HashSet::new();
        let mut results = BTreeMap::new();

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

        let sstables_map = self.sstables.read().unwrap();

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

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            for sstable in ssts.iter() {
                let mut reader = sstable.lock().unwrap();
                let f_key = reader.first_key().expect("Level 1+ SSTable must not be empty");
                let l_key = reader.last_key();
                if f_key > end || l_key < start {
                    continue;
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
            sstables.get(&0).map_or(false, |list| !list.is_empty())
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
        loop {
            if let Some(level) = self.find_level_to_compact() {
                self.compact_level(level)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn find_level_to_compact(&self) -> Option<usize> {
        let sstables = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables.get(&0) {
            if l0_ssts.len() >= self.options.l0_compaction_threshold {
                return Some(0);
            }
        }

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
            } else if let Some(list) = sstables_guard.get(&source_level) {
                if !list.is_empty() {
                    source_files.push(list[0].clone());
                }
            }

            if source_files.is_empty() {
                return Ok(());
            }

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
                if let Some(target_list) = sstables_guard.get(&target_level) {
                    for sstable in target_list {
                        let overlaps = {
                            let reader = sstable.lock().unwrap();
                            let fk = reader.first_key().expect("Target SSTable must not be empty");
                            let lk = reader.last_key();
                            fk <= &max_k && lk >= &min_k
                        };
                        if overlaps {
                            overlapping_target_files.push(sstable.clone());
                        }
                    }
                }
            }
        }

        // 2. Merge-sort all selected files (locks released)
        let mut merged_data = BTreeMap::new();
        let mut l0_files_sorted = Vec::new();
        let mut other_files = Vec::new();
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f.lock().unwrap();
            if reader.level == 0 {
                l0_files_sorted.push(f.clone());
            } else {
                other_files.push(f.clone());
            }
        }
        l0_files_sorted.sort_by_key(|f| f.lock().unwrap().id);

        for f in other_files {
            let mut reader = f.lock().unwrap();
            let entries = reader.read_all()?;
            for (k, v) in entries {
                merged_data.insert(k, v);
            }
        }
        for f in l0_files_sorted {
            let mut reader = f.lock().unwrap();
            let entries = reader.read_all()?;
            for (k, v) in entries {
                merged_data.insert(k, v);
            }
        }

        // 3. Write merged data to new SSTables
        let mut new_sstables = Vec::new();
        let mut current_writer: Option<SstableWriter> = None;
        let mut current_path = None;
        let mut current_id = 0;
        let mut current_writer_uncompressed_bytes = 0;

        for (k, v) in merged_data {
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
                    continue;
                }
            }

            if current_writer.is_none() {
                current_id = self.get_next_id();
                let path = self.dir_path.join(format!("L{}_{:05}.sst", target_level, current_id));
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
                Some(DbValue::Null) => 1,
                None => 1,
            };
            current_writer_uncompressed_bytes += entry_sz;

            if current_writer_uncompressed_bytes >= self.options.sstable_target_size {
                let writer = current_writer.take().unwrap();
                writer.finish()?;
                let reader = SstableReader::open(current_path.take().unwrap(), current_id, target_level)?;
                new_sstables.push(Arc::new(Mutex::new(reader)));
            }
        }

        if let Some(writer) = current_writer.take() {
            writer.finish()?;
            let reader = SstableReader::open(current_path.take().unwrap(), current_id, target_level)?;
            new_sstables.push(Arc::new(Mutex::new(reader)));
        }

        // Update manifest under manifest_mutex lock
        {
            let _manifest_lock = self.manifest_mutex.lock().unwrap();
            let manifest_path = self.dir_path.join("manifest.bin");
            let mut manifest = Manifest::load(&manifest_path)?;
            for f in source_files.iter().chain(overlapping_target_files.iter()) {
                let reader = f.lock().unwrap();
                manifest.active_sstables.remove(&(reader.level, reader.id));
            }
            for f in &new_sstables {
                let reader = f.lock().unwrap();
                manifest.active_sstables.insert((reader.level, reader.id));
            }
            manifest.save(&manifest_path)?;
        }

        // 4. Update the active sstables map
        {
            let mut sstables_guard = self.sstables.write().unwrap();

            let source_ids: HashSet<u64> = source_files.iter().map(|f| f.lock().unwrap().id).collect();
            if let Some(source_list) = sstables_guard.get_mut(&source_level) {
                source_list.retain(|f| !source_ids.contains(&f.lock().unwrap().id));
            }

            let target_ids: HashSet<u64> = overlapping_target_files.iter().map(|f| f.lock().unwrap().id).collect();
            let target_list = sstables_guard.entry(target_level).or_default();
            target_list.retain(|f| !target_ids.contains(&f.lock().unwrap().id));
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

        let reader = SstableReader::open(&sst_path, next_id, 0)?;
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.entry(0).or_default().insert(0, Arc::new(Mutex::new(reader)));
        }

        let temp_wal_path = self.dir_path.join("active.wal.tmp");
        let new_wal = Wal::open(&temp_wal_path)?;

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
}
