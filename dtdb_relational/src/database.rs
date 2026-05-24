use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Mutex};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use dtdb_storage::{StorageEngine, CompressionType, EngineOptions, WalEntry, ThreadSpawner};
use crate::error::{RelationalError, Result};
use crate::schema::Schema;

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
    pub engine: Arc<StorageEngine>,
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
            bincode::deserialize::<DatabaseOptions>(&bytes).map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?
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
            }
        };
        Self::open_with_options_and_spawner(dir_path, options, spawner)
    }

    /// Opens the database catalog directory with specified options and loads all tables.
    pub fn open_with_options(dir_path: impl AsRef<Path>, options: DatabaseOptions) -> Result<Self> {
        Self::open_with_options_and_spawner(dir_path, options, Arc::new(dtdb_storage::DefaultSpawner))
    }

    /// Opens the database catalog directory with specified options and a custom ThreadSpawner, and loads all tables.
    pub fn open_with_options_and_spawner(
        dir_path: impl AsRef<Path>,
        options: DatabaseOptions,
        spawner: Arc<dyn ThreadSpawner>,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        let db_options_path = dir_path.join("db_options.bin");
        let bytes = bincode::serialize(&options).map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        fs::write(&db_options_path, bytes)?;

        let mut tables = HashMap::new();

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
                        base_level_size_limit: options.base_level_size_limit.unwrap_or(10 * 1024 * 1024),
                        level_size_multiplier: options.level_size_multiplier.unwrap_or(10),
                        max_level: options.max_level.unwrap_or(7),
                    };
                    let engine = Arc::new(StorageEngine::open_with_spawner(&path, engine_opts, spawner.clone())?);

                    tables.insert(
                        name.clone(),
                        Table {
                            name,
                            schema,
                            engine,
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
        };

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
            base_level_size_limit: self.options.base_level_size_limit.unwrap_or(10 * 1024 * 1024),
            level_size_multiplier: self.options.level_size_multiplier.unwrap_or(10),
            max_level: self.options.max_level.unwrap_or(7),
        };

        // Open the new Layer 1 storage engine
        let engine = Arc::new(StorageEngine::open_with_spawner(&table_path, engine_opts, self.spawner.clone())?);

        tables_guard.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                schema,
                engine,
            },
        );

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

        let table_path = self.dir_path.join(name);
        if table_path.exists() {
            fs::remove_dir_all(table_path)?;
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

            let record: TransactionRecord = bincode::deserialize(&bytes)
                .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
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
                    println!("Rolling forward transaction {} for table {}", tx_id, table_name);
                    table.engine.write_batch(entries)?;
                }
            }
        }

        // Clean up the log since recovery has completed.
        let _ = File::create(&self.transaction_log_path)?;
        Ok(())
    }

    pub fn register_transaction(&self, tx_id: u64) -> u64 {
        let version = self.global_commit_version.load(std::sync::atomic::Ordering::SeqCst);
        let mut active = self.occ_active_transactions.lock().unwrap();
        active.insert(tx_id, version);
        version
    }

    pub fn unregister_transaction(&self, tx_id: u64) {
        let mut active = self.occ_active_transactions.lock().unwrap();
        active.remove(&tx_id);
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
        let commit_version = self.global_commit_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

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
}
