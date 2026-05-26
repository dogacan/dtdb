use crate::error::{RelationalError, Result};
use crate::row::Row;
use crate::schema::{IndexDefinition, Schema};
use crate::transaction::Transaction;
use dtdb_storage::{
    CompressionType, DbKey, DbValue, EngineOptions, StorageEngine, ThreadSpawner, WalEntry,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum TransactionRecord {
    Prepared {
        tx_id: u64,
        mutations: HashMap<String, Vec<WalEntry>>,
    },
    Committed {
        tx_id: u64,
    },
}

#[derive(Debug, Clone)]
pub struct CommitRecord {
    pub commit_version: u64,
    pub keys: HashMap<String, HashSet<dtdb_storage::DbKey>>,
}

/// Table represents a relational table mapping column definitions to an underlying LSM engine.
///
/// We implement `Clone` on `Table`. This is a clean Rust design pattern:
/// cloning a `Table` performs a cheap clone of the `name`, the `schema`, and the
/// reference-counted pointer to the storage engine (`Arc<StorageEngine>`).
/// This allows client transactions to retrieve a copy of the table handles
/// without holding a read lock on the database catalog for the entire duration
/// of the transaction, which avoids catalog lock starvation.
#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub schema: Schema,
    pub engines: HashMap<String, Arc<StorageEngine>>,
    pub index_engines: HashMap<String, Arc<StorageEngine>>,
}

impl Table {
    /// Helper to get the physical directory path for a locality group.
    /// If the group is empty, we use a subdirectory named "default".
    pub fn group_dir(table_path: &Path, group: &str) -> PathBuf {
        if group.is_empty() {
            table_path.join("default")
        } else {
            table_path.join(format!("lg_{}", group))
        }
    }

    /// Helper to get the physical directory path for a secondary index.
    pub fn index_dir(table_path: &Path, index_name: &str) -> PathBuf {
        table_path.join(format!("index_{}", index_name))
    }

    /// Performs a write batch of mutations to the table, splitting updates across locality groups.
    pub fn write_batch(&self, entries: Vec<WalEntry>) -> Result<()> {
        let mut group_batches: HashMap<String, Vec<WalEntry>> = HashMap::new();
        for group in self.engines.keys() {
            group_batches.insert(group.clone(), Vec::new());
        }

        let mut index_batches: HashMap<String, Vec<WalEntry>> = HashMap::new();
        for idx in &self.schema.indexes {
            index_batches.insert(idx.name.clone(), Vec::new());
        }

        for entry in entries {
            match entry {
                WalEntry::Put { key, value } => {
                    let bytes = match value {
                        DbValue::Bytes(b) => b,
                        _ => {
                            return Err(RelationalError::Storage(
                                dtdb_storage::StorageError::Corruption(
                                    "Expected bytes for row serialization".to_string(),
                                ),
                            ));
                        }
                    };
                    let full_row = Row::from_bytes(&bytes)?;

                    // If we have indexes, perform read-before-write maintenance
                    if !self.schema.indexes.is_empty() {
                        let old_row_opt = self.get(&key, None)?;
                        if let Some(old_row) = old_row_opt {
                            for idx in &self.schema.indexes {
                                let mut old_keys = Vec::new();
                                for col_name in &idx.columns {
                                    let col_idx = self.schema.column_index(col_name).unwrap();
                                    let col_val = &old_row.values[col_idx];
                                    if matches!(col_val, DbValue::Null) {
                                        continue;
                                    }
                                    let k = match col_val {
                                        DbValue::Int(v) => DbKey::Int(*v),
                                        DbValue::String(s) => DbKey::String(s.clone()),
                                        DbValue::Bool(b) => DbKey::Bool(*b),
                                        _ => continue,
                                    };
                                    old_keys.push(k);
                                }
                                if old_keys.len() == idx.columns.len() {
                                    old_keys.push(key.clone());
                                    index_batches.get_mut(&idx.name).unwrap().push(
                                        WalEntry::Delete {
                                            key: DbKey::Composite(old_keys),
                                        },
                                    );
                                }
                            }
                        }

                        // Add new index entry
                        for idx in &self.schema.indexes {
                            let mut new_keys = Vec::new();
                            for col_name in &idx.columns {
                                let col_idx = self.schema.column_index(col_name).unwrap();
                                let col_val = &full_row.values[col_idx];
                                if matches!(col_val, DbValue::Null) {
                                    continue;
                                }
                                let k = match col_val {
                                    DbValue::Int(v) => DbKey::Int(*v),
                                    DbValue::String(s) => DbKey::String(s.clone()),
                                    DbValue::Bool(b) => DbKey::Bool(*b),
                                    _ => continue,
                                };
                                new_keys.push(k);
                            }
                            if new_keys.len() == idx.columns.len() {
                                new_keys.push(key.clone());
                                index_batches
                                    .get_mut(&idx.name)
                                    .unwrap()
                                    .push(WalEntry::Put {
                                        key: DbKey::Composite(new_keys),
                                        value: DbValue::Null,
                                    });
                            }
                        }
                    }

                    for (group, batch) in &mut group_batches {
                        let sub_row = self.schema.split_row(&full_row, group);
                        let sub_bytes = sub_row.to_bytes()?;
                        batch.push(WalEntry::Put {
                            key: key.clone(),
                            value: DbValue::Bytes(sub_bytes),
                        });
                    }
                }
                WalEntry::Delete { key } => {
                    // If we have indexes, perform read-before-write maintenance
                    if !self.schema.indexes.is_empty() {
                        let old_row_opt = self.get(&key, None)?;
                        if let Some(old_row) = old_row_opt {
                            for idx in &self.schema.indexes {
                                let mut old_keys = Vec::new();
                                for col_name in &idx.columns {
                                    let col_idx = self.schema.column_index(col_name).unwrap();
                                    let col_val = &old_row.values[col_idx];
                                    if matches!(col_val, DbValue::Null) {
                                        continue;
                                    }
                                    let k = match col_val {
                                        DbValue::Int(v) => DbKey::Int(*v),
                                        DbValue::String(s) => DbKey::String(s.clone()),
                                        DbValue::Bool(b) => DbKey::Bool(*b),
                                        _ => continue,
                                    };
                                    old_keys.push(k);
                                }
                                if old_keys.len() == idx.columns.len() {
                                    old_keys.push(key.clone());
                                    index_batches.get_mut(&idx.name).unwrap().push(
                                        WalEntry::Delete {
                                            key: DbKey::Composite(old_keys),
                                        },
                                    );
                                }
                            }
                        }
                    }

                    for batch in group_batches.values_mut() {
                        batch.push(WalEntry::Delete { key: key.clone() });
                    }
                }
                WalEntry::Batch(_) => {}
            }
        }

        // Apply mutations to main tables
        for (group, batch) in group_batches {
            if let Some(engine) = self.engines.get(&group) {
                engine.write_batch(batch)?;
            }
        }

        // Apply mutations to indexes
        for (idx_name, batch) in index_batches {
            if !batch.is_empty()
                && let Some(engine) = self.index_engines.get(&idx_name)
            {
                engine.write_batch(batch)?;
            }
        }

        Ok(())
    }

    /// Flushes the memtables of all storage engines belonging to the table.
    pub fn flush_memtable(&self) -> Result<()> {
        for engine in self.engines.values() {
            engine.flush_memtable()?;
        }
        for engine in self.index_engines.values() {
            engine.flush_memtable()?;
        }
        Ok(())
    }

    /// Fetches a row by primary key, reading only the necessary locality group engines.
    pub fn get(&self, key: &DbKey, columns: Option<&[String]>) -> Result<Option<Row>> {
        let mut needed_groups = HashSet::new();
        if let Some(cols) = columns {
            for col_name in cols {
                if let Some(col) = self.schema.columns.iter().find(|c| {
                    &c.name == col_name
                        || col_name.ends_with(&format!(".{}", c.name))
                        || c.name.ends_with(&format!(".{}", col_name))
                }) {
                    needed_groups.insert(col.locality_group.as_deref().unwrap_or("").to_string());
                }
            }
            if needed_groups.is_empty() {
                needed_groups.insert("".to_string());
            }
        } else {
            for g in self.schema.locality_groups() {
                needed_groups.insert(g);
            }
        }

        let mut group_rows = HashMap::new();
        let mut found_any = false;
        for group in needed_groups {
            if let Some(engine) = self.engines.get(&group) {
                if let Some(DbValue::Bytes(bytes)) = engine.get(key)? {
                    let sub_row = Row::from_bytes(&bytes)?;
                    group_rows.insert(group, Some(sub_row));
                    found_any = true;
                } else {
                    group_rows.insert(group, None);
                }
            }
        }

        if found_any {
            Ok(Some(self.schema.merge_rows(&group_rows)))
        } else {
            Ok(None)
        }
    }

    /// Performs a range scan across the necessary locality group engines, merge-joining the sorted streams.
    pub fn filtered_scan(
        &self,
        start: &DbKey,
        end: &DbKey,
        columns: Option<&[String]>,
    ) -> Result<Vec<(DbKey, Row)>> {
        let mut needed_groups = HashSet::new();
        if let Some(cols) = columns {
            for col_name in cols {
                if let Some(col) = self.schema.columns.iter().find(|c| {
                    &c.name == col_name
                        || col_name.ends_with(&format!(".{}", c.name))
                        || c.name.ends_with(&format!(".{}", col_name))
                }) {
                    needed_groups.insert(col.locality_group.as_deref().unwrap_or("").to_string());
                }
            }
            if needed_groups.is_empty() {
                needed_groups.insert("".to_string());
            }
        } else {
            for g in self.schema.locality_groups() {
                needed_groups.insert(g);
            }
        }

        let needed_groups_vec: Vec<String> = needed_groups.into_iter().collect();
        let mut group_scans = HashMap::new();
        for group in &needed_groups_vec {
            let Some(engine) = self.engines.get(group) else {
                continue;
            };
            let entries = engine.filtered_scan(start, end, |_, _| true)?;
            let mut rows = Vec::new();
            for (k, v) in entries {
                if let DbValue::Bytes(bytes) = v {
                    let r = Row::from_bytes(&bytes)?;
                    rows.push((k, r));
                }
            }
            group_scans.insert(group.clone(), rows);
        }

        let mut result = Vec::new();
        let mut cursors: HashMap<String, usize> =
            needed_groups_vec.iter().map(|g| (g.clone(), 0)).collect();

        loop {
            let mut min_key: Option<DbKey> = None;
            for (group, &idx) in &cursors {
                if let Some(entries) = group_scans.get(group)
                    && idx < entries.len()
                {
                    let key = &entries[idx].0;
                    if min_key.is_none() || key < min_key.as_ref().unwrap() {
                        min_key = Some(key.clone());
                    }
                }
            }

            let Some(k) = min_key else {
                break;
            };

            let mut row_parts = HashMap::new();
            for group in &needed_groups_vec {
                let idx = cursors.get_mut(group).unwrap();
                if let Some(entries) = group_scans.get(group) {
                    if *idx < entries.len() && entries[*idx].0 == k {
                        row_parts.insert(group.clone(), Some(entries[*idx].1.clone()));
                        *idx += 1;
                    } else {
                        row_parts.insert(group.clone(), None);
                    }
                }
            }

            let merged_row = self.schema.merge_rows(&row_parts);
            result.push((k, merged_row));
        }

        Ok(result)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: u64,
    pub total_size_bytes: u64,
    pub locality_group_stats: HashMap<String, GroupStats>,
    pub index_stats: HashMap<String, IndexStats>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct GroupStats {
    pub num_sstables: usize,
    pub total_sstable_size: u64,
    pub entry_count: u64,
    pub tombstone_count: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct IndexStats {
    pub entry_count: u64,
    pub unique_values: u64,
    pub avg_rows_per_value: f64,
}

/// DatabaseOptions defines the configuration parameters for a Database.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DatabaseOptions {
    pub compression: CompressionType,
    pub memtable_size_limit: usize,
    pub block_size_limit: usize,
    pub wal_size_limit: usize,
    pub flush_interval_ms: Option<u64>,
    pub l0_compaction_threshold: Option<usize>,
    pub sstable_target_size: Option<usize>,
    pub base_level_size_limit: Option<usize>,
    pub level_size_multiplier: Option<usize>,
    pub max_level: Option<usize>,
    pub block_cache_capacity: Option<usize>,
    #[serde(default)]
    pub analyze_frequency_ms: Option<u64>,
}

/// Database represents a catalog of Tables stored in a base directory.
pub struct Database {
    dir_path: PathBuf,
    tables: RwLock<HashMap<String, Table>>,
    pub options: DatabaseOptions,
    transaction_log_path: PathBuf,
    active_transactions: Mutex<HashSet<u64>>,
    global_commit_version: std::sync::atomic::AtomicU64,
    occ_active_transactions: Mutex<HashMap<u64, u64>>,
    commit_history: Mutex<Vec<CommitRecord>>,
    spawner: Arc<dyn ThreadSpawner>,
    active_table_access: Mutex<HashMap<String, HashSet<u64>>>,
    pub auto_increment_sequences: Mutex<HashMap<String, i64>>,
    pub statistics: RwLock<HashMap<String, TableStatistics>>,
    is_background_analyze_started: std::sync::atomic::AtomicBool,
}

impl Database {
    /// Opens the database catalog directory and loads all tables.
    ///
    /// It scans the base directory for table subdirectories containing `schema.bin`.
    pub fn open(dir_path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_spawner(dir_path, Arc::new(dtdb_storage::DefaultSpawner))
    }

    pub fn open_with_spawner(
        dir_path: impl AsRef<Path>,
        spawner: Arc<dyn ThreadSpawner>,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        let db_options_path = dir_path.join("db_options.bin");
        let options = if db_options_path.exists() {
            let bytes = fs::read(&db_options_path)?;
            bincode::deserialize::<DatabaseOptions>(&bytes).map_err(|e| {
                RelationalError::Storage(dtdb_storage::StorageError::Serialization(e))
            })?
        } else {
            DatabaseOptions {
                compression: CompressionType::Lz4,
                memtable_size_limit: 1024 * 1024,
                block_size_limit: 4096,
                wal_size_limit: 32 * 1024 * 1024,
                flush_interval_ms: None,
                l0_compaction_threshold: None,
                sstable_target_size: None,
                base_level_size_limit: None,
                level_size_multiplier: None,
                max_level: None,
                block_cache_capacity: Some(1000),
                analyze_frequency_ms: None,
            }
        };
        Self::open_with_options_and_spawner(dir_path, options, spawner)
    }

    /// Opens the database catalog directory with specified options and loads all tables.
    pub fn open_with_options(dir_path: impl AsRef<Path>, options: DatabaseOptions) -> Result<Self> {
        Self::open_with_options_and_spawner(
            dir_path,
            options,
            Arc::new(dtdb_storage::DefaultSpawner),
        )
    }

    /// Opens the database catalog directory with specified options and a custom ThreadSpawner, and loads all tables.
    pub fn open_with_options_and_spawner(
        dir_path: impl AsRef<Path>,
        options: DatabaseOptions,
        spawner: Arc<dyn ThreadSpawner>,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        // Clean up stranded temporary directories from previous crashes
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir()
                && let Some(name) = path.file_name().and_then(|s| s.to_str())
                && name.starts_with(".tmp_drop_")
            {
                let _ = fs::remove_dir_all(&path);
            }
        }

        let db_options_path = dir_path.join("db_options.bin");
        let bytes = bincode::serialize(&options)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        fs::write(&db_options_path, bytes)?;

        let mut tables = HashMap::new();
        let mut statistics = HashMap::new();

        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let schema_path = path.join("schema.bin");
                if schema_path.exists() {
                    let name = path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .ok_or_else(|| {
                            RelationalError::Storage(dtdb_storage::StorageError::Corruption(
                                "Invalid table directory name".to_string(),
                            ))
                        })?
                        .to_string();

                    // Load the schema definition file.
                    let schema = Schema::load_from_file(&schema_path)?;

                    // Open the underlying LSM engine.
                    let engine_opts = EngineOptions {
                        compression: options.compression,
                        memtable_size_limit: options.memtable_size_limit,
                        block_size_limit: options.block_size_limit,
                        wal_size_limit: options.wal_size_limit,
                        l0_compaction_threshold: options.l0_compaction_threshold.unwrap_or(4),
                        sstable_target_size: options.sstable_target_size.unwrap_or(2 * 1024 * 1024),
                        base_level_size_limit: options
                            .base_level_size_limit
                            .unwrap_or(10 * 1024 * 1024),
                        level_size_multiplier: options.level_size_multiplier.unwrap_or(10),
                        max_level: options.max_level.unwrap_or(7),
                        block_cache_capacity: options.block_cache_capacity.unwrap_or(1000),
                    };

                    let mut engines = HashMap::new();
                    let groups = schema.locality_groups();

                    // Check for old table layout (backward compatibility)
                    if groups.len() <= 1
                        && groups.contains("")
                        && (path.join("manifest.bin").exists() || path.join("wal.log").exists())
                    {
                        let mut group_opts = engine_opts;
                        if let Some(opts) = schema.locality_group_options.get("") {
                            group_opts = opts.apply_to(group_opts);
                        }
                        let engine = Arc::new(StorageEngine::open_with_spawner(
                            &path,
                            group_opts,
                            spawner.clone(),
                        )?);
                        engines.insert("".to_string(), engine);
                    } else {
                        // Multi-engine / new layout
                        for group in groups {
                            let g_path = Table::group_dir(&path, &group);
                            let mut group_opts = engine_opts;
                            if let Some(opts) = schema.locality_group_options.get(&group) {
                                group_opts = opts.apply_to(group_opts);
                            }
                            let engine = Arc::new(StorageEngine::open_with_spawner(
                                &g_path,
                                group_opts,
                                spawner.clone(),
                            )?);
                            engines.insert(group, engine);
                        }
                    }

                    let mut index_engines = HashMap::new();
                    for idx_def in &schema.indexes {
                        let idx_path = Table::index_dir(&path, &idx_def.name);
                        let engine = Arc::new(StorageEngine::open_with_spawner(
                            &idx_path,
                            engine_opts,
                            spawner.clone(),
                        )?);
                        index_engines.insert(idx_def.name.clone(), engine);
                    }

                    let stats_path = path.join("statistics.bin");
                    if stats_path.exists()
                        && let Ok(bytes) = fs::read(&stats_path)
                        && let Ok(stats) = bincode::deserialize::<TableStatistics>(&bytes)
                    {
                        statistics.insert(name.clone(), stats);
                    }

                    tables.insert(
                        name.clone(),
                        Table {
                            name,
                            schema,
                            engines,
                            index_engines,
                        },
                    );
                }
            }
        }

        let transaction_log_path = dir_path.join("transactions.log");
        let active_transactions = Mutex::new(HashSet::new());
        let global_commit_version = std::sync::atomic::AtomicU64::new(0);
        let occ_active_transactions = Mutex::new(HashMap::new());
        let commit_history = Mutex::new(Vec::new());
        let active_table_access = Mutex::new(HashMap::new());
        let auto_increment_sequences = Mutex::new(HashMap::new());

        let db = Self {
            dir_path,
            tables: RwLock::new(tables),
            options,
            transaction_log_path,
            active_transactions,
            global_commit_version,
            occ_active_transactions,
            commit_history,
            spawner,
            active_table_access,
            auto_increment_sequences,
            statistics: RwLock::new(statistics),
            is_background_analyze_started: std::sync::atomic::AtomicBool::new(false),
        };

        // Initialize auto-increment sequences for loaded tables
        {
            let mut seqs = db.auto_increment_sequences.lock().unwrap();
            let tables_guard = db.tables.read().unwrap();
            for (name, table) in tables_guard.iter() {
                if let Some(col_idx) = table
                    .schema
                    .columns
                    .iter()
                    .position(|c| c.is_auto_increment)
                {
                    let mut max_val = 0i64;
                    if let Ok((start, end)) = table.schema.primary_key_bounds()
                        && let Ok(rows) = table.filtered_scan(&start, &end, None)
                    {
                        for (_, row) in rows {
                            if let Some(DbValue::Int(v)) = row.get_by_index(col_idx)
                                && *v > max_val
                            {
                                max_val = *v;
                            }
                        }
                    }
                    seqs.insert(name.clone(), max_val + 1);
                }
            }
        }

        db.recover_transactions()?;

        Ok(db)
    }

    /// Creates a new relational table.
    ///
    /// Creates the directory, writes the schema file, and initializes the storage engine.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();
        if tables_guard.contains_key(name) {
            return Err(RelationalError::TableAlreadyExists(name.to_string()));
        }

        let table_path = self.dir_path.join(name);
        fs::create_dir_all(&table_path)?;

        // Save schema configuration
        let schema_path = table_path.join("schema.bin");
        schema.save_to_file(&schema_path)?;

        // Pass EngineOptions based on self.options
        let engine_opts = EngineOptions {
            compression: self.options.compression,
            memtable_size_limit: self.options.memtable_size_limit,
            block_size_limit: self.options.block_size_limit,
            wal_size_limit: self.options.wal_size_limit,
            l0_compaction_threshold: self.options.l0_compaction_threshold.unwrap_or(4),
            sstable_target_size: self.options.sstable_target_size.unwrap_or(2 * 1024 * 1024),
            base_level_size_limit: self
                .options
                .base_level_size_limit
                .unwrap_or(10 * 1024 * 1024),
            level_size_multiplier: self.options.level_size_multiplier.unwrap_or(10),
            max_level: self.options.max_level.unwrap_or(7),
            block_cache_capacity: self.options.block_cache_capacity.unwrap_or(1000),
        };

        let mut engines = HashMap::new();
        let groups = schema.locality_groups();
        if groups.len() <= 1 && groups.contains("") {
            let mut group_opts = engine_opts;
            if let Some(opts) = schema.locality_group_options.get("") {
                group_opts = opts.apply_to(group_opts);
            }
            let engine = Arc::new(StorageEngine::open_with_spawner(
                &table_path,
                group_opts,
                self.spawner.clone(),
            )?);
            engines.insert("".to_string(), engine);
        } else {
            for group in groups {
                let g_path = Table::group_dir(&table_path, &group);
                let mut group_opts = engine_opts;
                if let Some(opts) = schema.locality_group_options.get(&group) {
                    group_opts = opts.apply_to(group_opts);
                }
                let engine = Arc::new(StorageEngine::open_with_spawner(
                    &g_path,
                    group_opts,
                    self.spawner.clone(),
                )?);
                engines.insert(group, engine);
            }
        }

        let has_auto_inc = schema.columns.iter().any(|c| c.is_auto_increment);

        let initial_stats = TableStatistics {
            table_name: name.to_string(),
            ..Default::default()
        };

        {
            let mut stats_guard = self.statistics.write().unwrap();
            stats_guard.insert(name.to_string(), initial_stats.clone());
        }

        let stats_path = table_path.join("statistics.bin");
        let bytes = bincode::serialize(&initial_stats)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        fs::write(stats_path, bytes)?;

        tables_guard.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                schema,
                engines,
                index_engines: HashMap::new(),
            },
        );

        if has_auto_inc {
            let mut seqs = self.auto_increment_sequences.lock().unwrap();
            seqs.insert(name.to_string(), 1);
        }

        Ok(())
    }

    /// Drops a relational table.
    ///
    /// Removes table metadata from the catalog, drops the storage engine reference,
    /// and deletes the table directory on disk.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();

        // Remove the table from catalog mapping.
        // This drops our `Table` instance, dropping the `Arc<StorageEngine>`.
        let table = tables_guard
            .remove(name)
            .ok_or_else(|| RelationalError::TableNotFound(name.to_string()))?;

        // Explicitly drop table handles to close open files.
        drop(table);

        {
            let mut stats_guard = self.statistics.write().unwrap();
            stats_guard.remove(name);
        }

        // Wait until all transactions currently accessing this table have finished.
        loop {
            let has_active_readers = {
                let access = self.active_table_access.lock().unwrap();
                access.get(name).is_some_and(|readers| !readers.is_empty())
            };
            if !has_active_readers {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let table_path = self.dir_path.join(name);
        if table_path.exists() {
            // Generate a unique temporary directory name.
            let rand_val = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let temp_name = format!(".tmp_drop_{}_{}", name, rand_val);
            let temp_table_path = self.dir_path.join(&temp_name);

            // Atomically rename the table directory to a unique temporary "tombstone" directory.
            fs::rename(&table_path, &temp_table_path)?;

            // Remove the temporary directory non-atomically (crash safe).
            fs::remove_dir_all(temp_table_path)?;

            // Clean up the tracking entry.
            let mut access = self.active_table_access.lock().unwrap();
            access.remove(name);
        }

        Ok(())
    }

    /// Generates and returns the next auto-increment sequence ID for a table.
    pub fn next_sequence_value(&self, table_name: &str) -> Result<i64> {
        let mut seqs = self.auto_increment_sequences.lock().unwrap();
        if let Some(val) = seqs.get_mut(table_name) {
            let next = *val;
            *val += 1;
            Ok(next)
        } else {
            let table = self.get_table(table_name)?;
            let mut max_val = 0i64;
            if let Some(col_idx) = table
                .schema
                .columns
                .iter()
                .position(|c| c.is_auto_increment)
            {
                let (start, end) = table.schema.primary_key_bounds()?;
                if let Ok(rows) = table.filtered_scan(&start, &end, None) {
                    for (_, row) in rows {
                        if let Some(DbValue::Int(v)) = row.get_by_index(col_idx)
                            && *v > max_val
                        {
                            max_val = *v;
                        }
                    }
                }
            }
            let next = max_val + 1;
            seqs.insert(table_name.to_string(), next + 1);
            Ok(next)
        }
    }

    /// Updates the auto-increment sequence to be at least `val + 1`.
    pub fn update_sequence_value(&self, table_name: &str, val: i64) -> Result<()> {
        let mut seqs = self.auto_increment_sequences.lock().unwrap();
        if let Some(current) = seqs.get_mut(table_name) {
            if val >= *current {
                *current = val + 1;
            }
        } else {
            seqs.insert(table_name.to_string(), val + 1);
        }
        Ok(())
    }

    /// Fetches a cloneable table handle from the database.
    pub fn get_table(&self, name: &str) -> Result<Table> {
        let tables_guard = self.tables.read().unwrap();
        tables_guard
            .get(name)
            .cloned()
            .ok_or_else(|| RelationalError::TableNotFound(name.to_string()))
    }

    /// List all loaded table names.
    pub fn list_tables(&self) -> Vec<String> {
        let tables_guard = self.tables.read().unwrap();
        tables_guard.keys().cloned().collect()
    }

    fn append_record(&self, record: &TransactionRecord) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.transaction_log_path)?;
        let bytes = bincode::serialize(record)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        let len = bytes.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn write_transaction_record(&self, record: &TransactionRecord) -> Result<()> {
        let mut active = self.active_transactions.lock().unwrap();
        if let TransactionRecord::Prepared { tx_id, .. } = record {
            active.insert(*tx_id);
        }
        self.append_record(record)?;
        Ok(())
    }

    pub fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let mut active = self.active_transactions.lock().unwrap();
        active.remove(&tx_id);

        if active.is_empty() {
            // Truncate the file to zero to keep it compact.
            let _ = File::create(&self.transaction_log_path)?;
        } else {
            let record = TransactionRecord::Committed { tx_id };
            self.append_record(&record)?;
        }
        Ok(())
    }

    fn recover_transactions(&self) -> Result<()> {
        if !self.transaction_log_path.exists() {
            return Ok(());
        }

        let file = File::open(&self.transaction_log_path)?;
        let mut reader = std::io::BufReader::new(file);
        let mut prepared = std::collections::BTreeMap::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match std::io::Read::read_exact(&mut reader, &mut len_bytes) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(RelationalError::Io(e)),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut bytes = vec![0u8; len];
            match std::io::Read::read_exact(&mut reader, &mut bytes) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // Ignore truncated
                Err(e) => return Err(RelationalError::Io(e)),
            }

            let record: TransactionRecord = bincode::deserialize(&bytes).map_err(|e| {
                RelationalError::Storage(dtdb_storage::StorageError::Serialization(e))
            })?;
            match record {
                TransactionRecord::Prepared { tx_id, mutations } => {
                    prepared.insert(tx_id, mutations);
                }
                TransactionRecord::Committed { tx_id } => {
                    prepared.remove(&tx_id);
                }
            }
        }

        if prepared.is_empty() {
            let _ = File::create(&self.transaction_log_path)?;
            return Ok(());
        }

        println!("Recovering {} pending transactions...", prepared.len());
        for (tx_id, mutations) in prepared {
            for (table_name, entries) in mutations {
                if let Ok(table) = self.get_table(&table_name) {
                    println!(
                        "Rolling forward transaction {} for table {}",
                        tx_id, table_name
                    );
                    table.write_batch(entries)?;
                }
            }
        }

        // Clean up the log since recovery has completed.
        let _ = File::create(&self.transaction_log_path)?;
        Ok(())
    }

    pub fn register_transaction(&self, tx_id: u64) -> u64 {
        let version = self
            .global_commit_version
            .load(std::sync::atomic::Ordering::SeqCst);
        let mut active = self.occ_active_transactions.lock().unwrap();
        active.insert(tx_id, version);
        version
    }

    pub fn register_table_access(&self, table_name: &str, tx_id: u64) {
        let mut access = self.active_table_access.lock().unwrap();
        access
            .entry(table_name.to_string())
            .or_default()
            .insert(tx_id);
    }

    pub fn unregister_transaction(&self, tx_id: u64) {
        let min_start_version = {
            let mut active = self.occ_active_transactions.lock().unwrap();
            active.remove(&tx_id);
            active.values().copied().min()
        };

        let min_version = min_start_version.unwrap_or_else(|| {
            self.global_commit_version
                .load(std::sync::atomic::Ordering::SeqCst)
        });

        {
            let mut history = self.commit_history.lock().unwrap();
            history.retain(|r| r.commit_version >= min_version);
        }

        let mut access = self.active_table_access.lock().unwrap();
        for readers in access.values_mut() {
            readers.remove(&tx_id);
        }
    }

    pub fn validate_and_commit(
        &self,
        tx_id: u64,
        start_version: u64,
        read_set: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
        scan_ranges: &HashMap<String, Vec<(dtdb_storage::DbKey, dtdb_storage::DbKey)>>,
        write_keys: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
    ) -> Result<u64> {
        let mut history = self.commit_history.lock().unwrap();

        // 1. Perform validation against history for commits > start_version
        for record in history.iter() {
            if record.commit_version > start_version {
                // Check read-write conflict: Did the committed record modify a key we read?
                for (table_name, read_keys) in read_set {
                    if let Some(committed_keys) = record.keys.get(table_name) {
                        for k in read_keys {
                            if committed_keys.contains(k) {
                                return Err(RelationalError::TransactionConflict(format!(
                                    "Conflict detected: Key {:?} in table {} was modified by a concurrent transaction (tx_id: {})",
                                    k, table_name, tx_id
                                )));
                            }
                        }
                    }
                }

                // Check phantom conflict: Did the committed record modify/insert a key in a range we scanned?
                for (table_name, ranges) in scan_ranges {
                    if let Some(committed_keys) = record.keys.get(table_name) {
                        for k in committed_keys {
                            for (start, end) in ranges {
                                if k >= start && k <= end {
                                    return Err(RelationalError::TransactionConflict(format!(
                                        "Phantom read conflict detected: Key {:?} in table {} fell within scanned range [{:?}, {:?}] (tx_id: {})",
                                        k, table_name, start, end, tx_id
                                    )));
                                }
                            }
                        }
                    }
                }

                // Check write-write conflict: Did the committed record modify a key we want to write?
                for (table_name, w_keys) in write_keys {
                    if let Some(committed_keys) = record.keys.get(table_name) {
                        for k in w_keys {
                            if committed_keys.contains(k) {
                                return Err(RelationalError::TransactionConflict(format!(
                                    "Conflict detected: Key {:?} in table {} was modified by a concurrent transaction (tx_id: {})",
                                    k, table_name, tx_id
                                )));
                            }
                        }
                    }
                }
            }
        }

        // 2. Increment global commit version to get a unique commit version
        let commit_version = self
            .global_commit_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        // 3. Append to history
        let new_record = CommitRecord {
            commit_version,
            keys: write_keys.clone(),
        };
        history.push(new_record);

        // 4. Prune older history
        let min_start_version = {
            let active = self.occ_active_transactions.lock().unwrap();
            active.values().copied().min().unwrap_or(commit_version)
        };
        history.retain(|r| r.commit_version >= min_start_version);

        Ok(commit_version)
    }

    pub fn validate_read_only(
        &self,
        tx_id: u64,
        start_version: u64,
        read_set: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
        scan_ranges: &HashMap<String, Vec<(dtdb_storage::DbKey, dtdb_storage::DbKey)>>,
    ) -> Result<()> {
        let history = self.commit_history.lock().unwrap();

        for record in history.iter() {
            if record.commit_version > start_version {
                // Check read-write conflict: Did the committed record modify a key we read?
                for (table_name, read_keys) in read_set {
                    if let Some(committed_keys) = record.keys.get(table_name) {
                        for k in read_keys {
                            if committed_keys.contains(k) {
                                return Err(RelationalError::TransactionConflict(format!(
                                    "Conflict detected: Key {:?} in table {} was modified by a concurrent transaction (tx_id: {})",
                                    k, table_name, tx_id
                                )));
                            }
                        }
                    }
                }

                // Check phantom conflict: Did the committed record modify/insert a key in a range we scanned?
                for (table_name, ranges) in scan_ranges {
                    if let Some(committed_keys) = record.keys.get(table_name) {
                        for k in committed_keys {
                            for (start, end) in ranges {
                                if k >= start && k <= end {
                                    return Err(RelationalError::TransactionConflict(format!(
                                        "Phantom read conflict detected: Key {:?} in table {} fell within scanned range [{:?}, {:?}] (tx_id: {})",
                                        k, table_name, start, end, tx_id
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Creates a secondary index for a table.
    pub fn create_index(
        &self,
        table_name: &str,
        index_name: &str,
        columns: Vec<String>,
    ) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();

        // 1. Check table existence
        let table = tables_guard
            .get_mut(table_name)
            .ok_or_else(|| RelationalError::TableNotFound(table_name.to_string()))?;

        // 2. Check for duplicate index name
        if table
            .schema
            .indexes
            .iter()
            .any(|idx| idx.name == index_name)
        {
            return Err(RelationalError::SchemaMismatch(format!(
                "Index '{}' already exists on table '{}'",
                index_name, table_name
            )));
        }

        // 3. Verify columns exist in schema
        for col_name in &columns {
            if table.schema.column_index(col_name).is_none() {
                return Err(RelationalError::SchemaMismatch(format!(
                    "Column '{}' does not exist in table '{}' schema",
                    col_name, table_name
                )));
            }
        }

        // 4. Save updated schema configuration
        let table_path = self.dir_path.join(table_name);
        let schema_path = table_path.join("schema.bin");
        table.schema.indexes.push(IndexDefinition {
            name: index_name.to_string(),
            columns: columns.clone(),
        });
        table.schema.save_to_file(&schema_path)?;

        // 5. Construct EngineOptions
        let engine_opts = EngineOptions {
            compression: self.options.compression,
            memtable_size_limit: self.options.memtable_size_limit,
            block_size_limit: self.options.block_size_limit,
            wal_size_limit: self.options.wal_size_limit,
            l0_compaction_threshold: self.options.l0_compaction_threshold.unwrap_or(4),
            sstable_target_size: self.options.sstable_target_size.unwrap_or(2 * 1024 * 1024),
            base_level_size_limit: self
                .options
                .base_level_size_limit
                .unwrap_or(10 * 1024 * 1024),
            level_size_multiplier: self.options.level_size_multiplier.unwrap_or(10),
            max_level: self.options.max_level.unwrap_or(7),
            block_cache_capacity: self.options.block_cache_capacity.unwrap_or(1000),
        };

        // 6. Create index directory
        let idx_path = Table::index_dir(&table_path, index_name);
        fs::create_dir_all(&idx_path)?;

        // 7. Open the storage engine
        let engine = Arc::new(StorageEngine::open_with_spawner(
            &idx_path,
            engine_opts,
            self.spawner.clone(),
        )?);

        // 8. Wait until all active transactions accessing this table have finished (matching DDL behavior)
        loop {
            let has_active_readers = {
                let access = self.active_table_access.lock().unwrap();
                access
                    .get(table_name)
                    .is_some_and(|readers| !readers.is_empty())
            };
            if !has_active_readers {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        // 9. Populate the index from existing table data
        // Determine scan bounds based on primary key type
        let (start, end) = table.schema.primary_key_bounds()?;

        // Scan the table
        let rows = table.filtered_scan(&start, &end, None)?;
        let mut index_entries = Vec::new();
        for (pk_key, row) in rows {
            // Support composite index keys or single key.
            // For columns, we construct a vector of DbKeys.
            let mut keys = Vec::new();
            for col_name in &columns {
                let col_idx = table.schema.column_index(col_name).unwrap();
                let col_val = &row.values[col_idx];
                if matches!(col_val, DbValue::Null) {
                    // Skip indexing rows with Null values for simplification
                    continue;
                }
                let k = match col_val {
                    DbValue::Int(v) => DbKey::Int(*v),
                    DbValue::String(s) => DbKey::String(s.clone()),
                    DbValue::Bool(b) => DbKey::Bool(*b),
                    other => {
                        return Err(RelationalError::SchemaMismatch(format!(
                            "Cannot index non-indexable value type {:?}",
                            other
                        )));
                    }
                };
                keys.push(k);
            }
            if keys.len() == columns.len() {
                keys.push(pk_key);
                index_entries.push(WalEntry::Put {
                    key: DbKey::Composite(keys),
                    value: DbValue::Null,
                });
            }
        }

        if !index_entries.is_empty() {
            engine.write_batch(index_entries)?;
        }

        // 10. Store the engine reference
        table.index_engines.insert(index_name.to_string(), engine);

        Ok(())
    }

    /// Drops a secondary index from a table.
    pub fn drop_index(&self, table_name: &str, index_name: &str) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();

        let table = tables_guard
            .get_mut(table_name)
            .ok_or_else(|| RelationalError::TableNotFound(table_name.to_string()))?;

        // 1. Verify index exists
        let idx_pos = table
            .schema
            .indexes
            .iter()
            .position(|idx| idx.name == index_name)
            .ok_or_else(|| {
                RelationalError::SchemaMismatch(format!(
                    "Index '{}' does not exist on table '{}'",
                    index_name, table_name
                ))
            })?;

        // 2. Remove the engine reference so it releases file handles
        let engine = table.index_engines.remove(index_name);
        drop(engine);

        // 3. Wait for active readers of the table
        loop {
            let has_active_readers = {
                let access = self.active_table_access.lock().unwrap();
                access
                    .get(table_name)
                    .is_some_and(|readers| !readers.is_empty())
            };
            if !has_active_readers {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        // 4. Save schema with index removed
        let table_path = self.dir_path.join(table_name);
        let schema_path = table_path.join("schema.bin");
        table.schema.indexes.remove(idx_pos);
        table.schema.save_to_file(&schema_path)?;

        // 5. Delete on-disk index directory
        let idx_path = Table::index_dir(&table_path, index_name);
        if idx_path.exists() {
            fs::remove_dir_all(idx_path)?;
        }

        Ok(())
    }

    pub fn commit_history_len(&self) -> usize {
        self.commit_history.lock().unwrap().len()
    }

    /// Triggers background stats collection if options specify it and it hasn't been started.
    pub fn start_background_analyze_if_needed(&self, db_arc: &Arc<Database>) {
        if let Some(ms) = self.options.analyze_frequency_ms
            && !self
                .is_background_analyze_started
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let weak_db = Arc::downgrade(db_arc);
            let spawner = self.spawner.clone();
            spawner.spawn(Box::new(move || {
                let mut i = 0;
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                    if let Some(db) = weak_db.upgrade() {
                        let tx_id = 1_000_000_000_000 + i;
                        i += 1;
                        let tx = Transaction::new_with_isolation(
                            tx_id,
                            db.clone(),
                            crate::transaction::IsolationLevel::ReadUncommitted,
                        );
                        if let Err(e) = db.analyze_all(&tx) {
                            eprintln!("Background analyze error: {:?}", e);
                        }
                    } else {
                        break;
                    }
                }
            }));
        }
    }

    /// Gathers database statistics for a single table.
    pub fn analyze_table(&self, table_name: &str, _tx: &Transaction) -> Result<()> {
        let table = self.get_table(table_name)?;

        // 1. Compute logical row count
        let (start, end) = table.schema.primary_key_bounds()?;
        let rows = table.filtered_scan(&start, &end, Some(&[]))?;
        let row_count = rows.len() as u64;

        // 2. Fetch storage engine statistics for locality groups
        let mut locality_group_stats = HashMap::new();
        let mut total_size_bytes = 0;
        for (group, engine) in &table.engines {
            let stats = engine.get_statistics()?;
            let group_stats = GroupStats {
                num_sstables: stats.num_sstables,
                total_sstable_size: stats.total_sstable_size,
                entry_count: stats.sstable_entries + stats.memtable_entries,
                tombstone_count: stats.sstable_tombstones + stats.memtable_tombstones,
            };
            total_size_bytes += stats.total_sstable_size;
            locality_group_stats.insert(group.clone(), group_stats);
        }

        // 3. Fetch index statistics
        let mut index_stats = HashMap::new();
        let (min_pk, max_pk) = table.schema.primary_key_bounds()?;
        for idx_def in &table.schema.indexes {
            if let Some(index_engine) = table.index_engines.get(&idx_def.name) {
                let mut min_idx_keys = Vec::new();
                let mut max_idx_keys = Vec::new();
                for col_name in &idx_def.columns {
                    let col_idx = table.schema.column_index(col_name).unwrap();
                    let col = &table.schema.columns[col_idx];
                    let (min_val, max_val) = match col.data_type {
                        crate::schema::DataType::Int => {
                            (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX))
                        }
                        crate::schema::DataType::Bool => (DbKey::Bool(false), DbKey::Bool(true)),
                        _ => (
                            DbKey::String("".to_string()),
                            DbKey::String("\u{10ffff}".to_string()),
                        ),
                    };
                    min_idx_keys.push(min_val);
                    max_idx_keys.push(max_val);
                }
                min_idx_keys.push(min_pk.clone());
                max_idx_keys.push(max_pk.clone());
                let start_bound = DbKey::Composite(min_idx_keys);
                let end_bound = DbKey::Composite(max_idx_keys);

                let index_entries =
                    index_engine.filtered_scan(&start_bound, &end_bound, |_, _| true)?;

                // Track stats for the index engine too
                let index_engine_stats = index_engine.get_statistics()?;
                total_size_bytes += index_engine_stats.total_sstable_size;

                let entry_count = index_entries.len() as u64;
                let mut unique_prefixes = HashSet::new();
                for (idx_key, _) in &index_entries {
                    if let DbKey::Composite(parts) = idx_key
                        && !parts.is_empty()
                    {
                        let prefix = parts[0..parts.len() - 1].to_vec();
                        unique_prefixes.insert(prefix);
                    }
                }
                let unique_values = unique_prefixes.len() as u64;
                let avg_rows_per_value = if unique_values > 0 {
                    entry_count as f64 / unique_values as f64
                } else {
                    0.0
                };

                index_stats.insert(
                    idx_def.name.clone(),
                    IndexStats {
                        entry_count,
                        unique_values,
                        avg_rows_per_value,
                    },
                );
            }
        }

        let stats = TableStatistics {
            table_name: table_name.to_string(),
            row_count,
            total_size_bytes,
            locality_group_stats,
            index_stats,
        };

        // 4. Update in-memory cache and persist to file
        {
            let mut stats_guard = self.statistics.write().unwrap();
            stats_guard.insert(table_name.to_string(), stats.clone());
        }

        let table_path = self.dir_path.join(table_name);
        let stats_path = table_path.join("statistics.bin");
        let bytes = bincode::serialize(&stats)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        fs::write(stats_path, bytes)?;

        Ok(())
    }

    /// Gathers statistics for all tables in the database.
    pub fn analyze_all(&self, tx: &Transaction) -> Result<()> {
        let tables = self.list_tables();
        for table_name in tables {
            if self.get_table(&table_name).is_ok() {
                self.analyze_table(&table_name, tx)?;
            }
        }
        Ok(())
    }

    /// Returns the cached statistics for a table if they exist.
    pub fn get_table_statistics(&self, table_name: &str) -> Option<TableStatistics> {
        let stats_guard = self.statistics.read().unwrap();
        stats_guard.get(table_name).cloned()
    }
}
