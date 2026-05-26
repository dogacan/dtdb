use crate::database::{Database, Table, TransactionRecord};
use crate::error::{RelationalError, Result};
use crate::row::Row;
use dtdb_storage::{DbKey, DbValue, WalEntry};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    SnapshotIsolation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TransactionOptions {
    pub isolation_level: IsolationLevel,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            isolation_level: IsolationLevel::SnapshotIsolation,
        }
    }
}

/// Transaction represents a client connection transaction session.
///
/// It provides ACID guarantees using a local memory write buffer:
/// - All writes (inserts, updates, deletes) are held in `write_buffer`.
/// - Reads check the `write_buffer` first (implementing Read-Your-Own-Writes)
///   before falling back to the underlying Layer 1 storage engine.
/// - `rollback()` clears the write buffer.
/// - `commit()` flushes all buffered mutations to the table storage engines.
#[allow(clippy::type_complexity)]
pub struct Transaction {
    pub tx_id: u64,
    database: Arc<Database>,
    // Table Name -> (PrimaryKey -> Option<Row>)
    // Some(Row) represents an insert or update.
    // None represents a deletion (tombstone).
    write_buffer: Mutex<HashMap<String, HashMap<DbKey, Option<Row>>>>,
    start_version: u64,
    read_set: Arc<Mutex<HashMap<String, HashSet<DbKey>>>>,
    scan_ranges: Arc<Mutex<HashMap<String, Vec<(DbKey, DbKey)>>>>,
    pub isolation_level: IsolationLevel,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.database.unregister_transaction(self.tx_id);
    }
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(tx_id: u64, database: Arc<Database>) -> Self {
        Self::new_with_isolation(tx_id, database, IsolationLevel::SnapshotIsolation)
    }

    /// Creates a new transaction with a specific isolation level.
    pub fn new_with_isolation(
        tx_id: u64,
        database: Arc<Database>,
        isolation_level: IsolationLevel,
    ) -> Self {
        database.start_background_analyze_if_needed(&database);
        let start_version = database.register_transaction(tx_id);
        Self {
            tx_id,
            database,
            write_buffer: Mutex::new(HashMap::new()),
            start_version,
            read_set: Arc::new(Mutex::new(HashMap::new())),
            scan_ranges: Arc::new(Mutex::new(HashMap::new())),
            isolation_level,
        }
    }

    fn get_table(&self, table_name: &str) -> Result<Table> {
        let table = self.database.get_table(table_name)?;
        self.database.register_table_access(table_name, self.tx_id);
        Ok(table)
    }

    /// Inserts or updates a Row inside the transaction buffer.
    ///
    /// Validates the row schema and primary key constraints.
    pub fn put(&self, table_name: &str, key: DbKey, row: Row) -> Result<()> {
        let table = self.get_table(table_name)?;

        // Validate Row and Primary Key constraints.
        table.schema.validate_row(&row)?;
        table.schema.validate_key(&key, &row)?;

        // Insert into transaction buffer.
        let mut buffer = self.write_buffer.lock().unwrap();
        buffer
            .entry(table_name.to_string())
            .or_default()
            .insert(key, Some(row));

        Ok(())
    }

    /// Deletes a row by primary key inside the transaction buffer.
    pub fn delete(&self, table_name: &str, key: DbKey) -> Result<()> {
        let table = self.get_table(table_name)?;

        // Validate key.
        table.schema.validate_key_only(&key)?;

        // Insert a deletion tombstone (None) into transaction buffer.
        let mut buffer = self.write_buffer.lock().unwrap();
        buffer
            .entry(table_name.to_string())
            .or_default()
            .insert(key, None);

        Ok(())
    }

    /// Fetches a row by primary key.
    ///
    /// Checks the transaction write buffer first (Read-Your-Own-Writes),
    /// falling back to the underlying StorageEngine.
    pub fn get(&self, table_name: &str, key: &DbKey) -> Result<Option<Row>> {
        self.get_projected(table_name, key, None)
    }

    /// Fetches a row by primary key with optional column projection.
    pub fn get_projected(
        &self,
        table_name: &str,
        key: &DbKey,
        columns: Option<&[String]>,
    ) -> Result<Option<Row>> {
        let table = self.get_table(table_name)?;

        // 1. Check transaction write buffer.
        {
            let buffer = self.write_buffer.lock().unwrap();
            if let Some(table_buffer) = buffer.get(table_name)
                && let Some(buffered_val) = table_buffer.get(key)
            {
                return Ok(buffered_val.clone());
            }
        }

        // 2. Fall back to underlying storage engine.
        // Track the key in our read set.
        if self.isolation_level == IsolationLevel::SnapshotIsolation
            || self.isolation_level == IsolationLevel::RepeatableRead
        {
            let mut read_set = self.read_set.lock().unwrap();
            read_set
                .entry(table_name.to_string())
                .or_default()
                .insert(key.clone());
        }

        table.get(key, columns)
    }

    /// Performs a range scan, merging storage engine data and transaction write-buffers.
    ///
    /// The returned rows are sorted by their primary key.
    pub fn filtered_scan<F>(
        &self,
        table_name: &str,
        start: &DbKey,
        end: &DbKey,
        filter: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&Row) -> bool,
    {
        self.filtered_scan_projected(table_name, start, end, None, filter)
    }

    /// Performs a range scan with column projection, merging storage engine data and transaction write-buffers.
    pub fn filtered_scan_projected<F>(
        &self,
        table_name: &str,
        start: &DbKey,
        end: &DbKey,
        columns: Option<&[String]>,
        filter: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&Row) -> bool,
    {
        let table = self.get_table(table_name)?;

        // Track the scan range.
        if self.isolation_level == IsolationLevel::SnapshotIsolation {
            let mut scan_ranges = self.scan_ranges.lock().unwrap();
            scan_ranges
                .entry(table_name.to_string())
                .or_default()
                .push((start.clone(), end.clone()));
        }

        // We use a BTreeMap sorted by primary key to merge values.
        let mut merged = BTreeMap::new();
        let mut seen = HashSet::new();

        // 1. Merge transaction write-buffer entries.
        {
            let buffer = self.write_buffer.lock().unwrap();
            if let Some(table_buffer) = buffer.get(table_name) {
                for (k, v) in table_buffer {
                    if k >= start && k <= end {
                        seen.insert(k.clone());
                        if let Some(row) = v {
                            merged.insert(k.clone(), row.clone());
                        }
                    }
                }
            }
        }

        // 2. Scan underlying storage engine.
        let engine_entries = table.filtered_scan(start, end, columns)?;
        for (k, row) in engine_entries {
            // Track the read key in our read set.
            if self.isolation_level == IsolationLevel::SnapshotIsolation
                || self.isolation_level == IsolationLevel::RepeatableRead
            {
                let mut read_set = self.read_set.lock().unwrap();
                read_set
                    .entry(table_name.to_string())
                    .or_default()
                    .insert(k.clone());
            }

            // Only add if not overridden by the active transaction write buffer.
            if !seen.contains(&k) {
                merged.insert(k, row);
            }
        }

        // 3. Filter the merged and sorted rows.
        let results: Vec<Row> = merged.into_values().filter(|row| filter(row)).collect();

        Ok(results)
    }

    pub fn scan_iter(
        &self,
        table_name: &str,
        start: &DbKey,
        end: &DbKey,
        columns: Option<&[String]>,
    ) -> Result<TransactionScanIterator> {
        let table = self.get_table(table_name)?;

        // Track the scan range.
        if self.isolation_level == IsolationLevel::SnapshotIsolation {
            let mut scan_ranges = self.scan_ranges.lock().unwrap();
            scan_ranges
                .entry(table_name.to_string())
                .or_default()
                .push((start.clone(), end.clone()));
        }

        // 1. Snapshot and sort write-buffer entries in [start, end] range
        let mut write_buffer_entries = Vec::new();
        {
            let buffer = self.write_buffer.lock().unwrap();
            if let Some(table_buffer) = buffer.get(table_name) {
                for (k, v) in table_buffer {
                    if k >= start && k <= end {
                        write_buffer_entries.push((k.clone(), v.clone()));
                    }
                }
            }
        }
        write_buffer_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // 2. Open table scan iterator
        let table_iter = table.scan_iter(start, end, columns)?;

        // Decision (A): Owned iterator snapshot of write buffer + Arc references for read_set.
        // No lifetime annotations are required, which dramatically simplifies the Volcano implementation.
        // Q2 Decision (A): per-key read-set tracking during next() iteration.
        Ok(TransactionScanIterator::new(
            table_iter,
            write_buffer_entries,
            self.read_set.clone(),
            table_name.to_string(),
            self.isolation_level,
        ))
    }

    /// Performs an index range scan, resolving primary keys and fetching corresponding rows,
    /// then applying the transaction's write buffer modifications.
    pub fn index_scan(
        &self,
        table_name: &str,
        index_name: &str,
        start_val: &DbKey,
        end_val: &DbKey,
        columns: Option<&[String]>,
    ) -> Result<Vec<Row>> {
        let table = self.get_table(table_name)?;
        let index_engine = table.index_engines.get(index_name).ok_or_else(|| {
            RelationalError::SchemaMismatch(format!("Index '{}' not found", index_name))
        })?;

        // 1. Resolve primary key bounds
        let (min_pk, max_pk) = table.schema.primary_key_bounds()?;

        // Construct composite key scan bounds for the index engine
        let start_bound = DbKey::Composite(vec![start_val.clone(), min_pk]);
        let end_bound = DbKey::Composite(vec![end_val.clone(), max_pk]);

        // Scan the index engine
        let index_entries = index_engine.filtered_scan(&start_bound, &end_bound, |_, _| true)?;

        let mut rows = Vec::new();
        let mut resolved_pks = HashSet::new();

        // 2. Fetch rows by primary key from transaction/main table
        for (idx_key, _) in index_entries {
            if let DbKey::Composite(mut parts) = idx_key
                && let Some(pk) = parts.pop()
            {
                resolved_pks.insert(pk.clone());
                if let Some(row) = self.get_projected(table_name, &pk, columns)? {
                    rows.push(row);
                }
            }
        }

        // 3. Scan the transaction `write_buffer` for any matching rows not already resolved by index scan
        let buffer = self.write_buffer.lock().unwrap();
        if let Some(table_buffer) = buffer.get(table_name) {
            for (pk, row_opt) in table_buffer {
                if resolved_pks.contains(pk) {
                    continue;
                }
                if let Some(row) = row_opt
                    && let Some(idx_def) = table
                        .schema
                        .indexes
                        .iter()
                        .find(|idx| idx.name == index_name)
                    && let Some(col_name) = idx_def.columns.first()
                    && let Some(col_idx) = table.schema.column_index(col_name)
                {
                    let val = &row.values[col_idx];
                    let k = match val {
                        DbValue::Int(v) => Some(DbKey::Int(*v)),
                        DbValue::String(s) => Some(DbKey::String(s.clone())),
                        DbValue::Bool(b) => Some(DbKey::Bool(*b)),
                        _ => None,
                    };
                    if let Some(k) = k
                        && &k >= start_val
                        && &k <= end_val
                    {
                        rows.push(row.clone());
                    }
                }
            }
        }

        Ok(rows)
    }

    /// Commits all buffered mutations in this transaction to the tables.
    pub fn commit(&self) -> Result<()> {
        let mut buffer = self.write_buffer.lock().unwrap();

        // 1. Group all buffered mutations by table as WalEntry batches.
        let mut table_batches = HashMap::new();
        for (table_name, table_buffer) in buffer.iter() {
            let mut entries = Vec::new();
            for (key, val) in table_buffer {
                match val {
                    Some(row) => {
                        let bytes = row.to_bytes()?;
                        entries.push(WalEntry::Put {
                            key: key.clone(),
                            value: DbValue::Bytes(bytes),
                        });
                    }
                    None => {
                        entries.push(WalEntry::Delete { key: key.clone() });
                    }
                }
            }
            if !entries.is_empty() {
                table_batches.insert(table_name.clone(), entries);
            }
        }

        // 1.5 Extract write keys for OCC validation
        let mut write_keys = HashMap::new();
        for (table_name, entries) in &table_batches {
            let mut keys = HashSet::new();
            for entry in entries {
                match entry {
                    WalEntry::Put { key, .. } => {
                        keys.insert(key.clone());
                    }
                    WalEntry::Delete { key } => {
                        keys.insert(key.clone());
                    }
                    WalEntry::Batch(_) => {}
                }
            }
            if !keys.is_empty() {
                write_keys.insert(table_name.clone(), keys);
            }
        }

        // Validate transaction using OCC
        {
            let read_set = self.read_set.lock().unwrap();
            let scan_ranges = self.scan_ranges.lock().unwrap();
            if !write_keys.is_empty() {
                self.database.validate_and_commit(
                    self.tx_id,
                    self.start_version,
                    &read_set,
                    &scan_ranges,
                    &write_keys,
                )?;
            } else {
                self.database.validate_read_only(
                    self.tx_id,
                    self.start_version,
                    &read_set,
                    &scan_ranges,
                )?;
            }
        }

        if table_batches.is_empty() {
            buffer.clear();
            return Ok(());
        }

        // 2. Write Prepared record to Global Log & fsync
        let record = TransactionRecord::Prepared {
            tx_id: self.tx_id,
            mutations: table_batches.clone(),
        };
        self.database.write_transaction_record(&record)?;

        // 3. Write batches to respective table storage engines
        for (table_name, entries) in table_batches {
            let table = self.get_table(&table_name)?;
            table.write_batch(entries)?;
        }

        // 4. Mark Committed & Truncate if clean
        self.database.commit_transaction(self.tx_id)?;

        buffer.clear();
        Ok(())
    }

    /// Discards all buffered mutations.
    pub fn rollback(&self) -> Result<()> {
        let mut buffer = self.write_buffer.lock().unwrap();
        buffer.clear();
        Ok(())
    }
}

pub struct TransactionScanIterator {
    table_iter: crate::database::TableScanIterator,
    write_buffer_entries: Vec<(DbKey, Option<Row>)>,
    write_buffer_idx: usize,
    seen: HashSet<DbKey>,
    read_set: Arc<Mutex<HashMap<String, HashSet<DbKey>>>>,
    table_name: String,
    isolation_level: IsolationLevel,
}

impl TransactionScanIterator {
    pub fn new(
        table_iter: crate::database::TableScanIterator,
        write_buffer_entries: Vec<(DbKey, Option<Row>)>,
        read_set: Arc<Mutex<HashMap<String, HashSet<DbKey>>>>,
        table_name: String,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            table_iter,
            write_buffer_entries,
            write_buffer_idx: 0,
            seen: HashSet::new(),
            read_set,
            table_name,
            isolation_level,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let has_write = self.write_buffer_idx < self.write_buffer_entries.len();
            let has_storage = self.table_iter.peek().is_some();

            if !has_write && !has_storage {
                return Ok(None);
            }

            let choose_write = if has_write && has_storage {
                let write_key = &self.write_buffer_entries[self.write_buffer_idx].0;
                let storage_key = &self.table_iter.peek().unwrap().0;
                write_key <= storage_key
            } else {
                has_write
            };

            if choose_write {
                let (k, v) = &self.write_buffer_entries[self.write_buffer_idx];
                self.write_buffer_idx += 1;

                if self.seen.insert(k.clone()) {
                    // Q2 Decision (A): Record read-set for isolation level repeatable read / snapshot isolation
                    if self.isolation_level == IsolationLevel::SnapshotIsolation
                        || self.isolation_level == IsolationLevel::RepeatableRead
                    {
                        let mut read_set = self.read_set.lock().unwrap();
                        read_set
                            .entry(self.table_name.clone())
                            .or_default()
                            .insert(k.clone());
                    }

                    if let Some(row) = v {
                        return Ok(Some(row.clone()));
                    }
                }
            } else {
                let (k, row) = self.table_iter.peek().unwrap().clone();
                self.table_iter.advance()?;

                if self.seen.insert(k.clone()) {
                    // Q2 Decision (A): Record read-set
                    if self.isolation_level == IsolationLevel::SnapshotIsolation
                        || self.isolation_level == IsolationLevel::RepeatableRead
                    {
                        let mut read_set = self.read_set.lock().unwrap();
                        read_set
                            .entry(self.table_name.clone())
                            .or_default()
                            .insert(k.clone());
                    }

                    return Ok(Some(row));
                }
            }
        }
    }
}
