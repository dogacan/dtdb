use crate::error::{RelationalError, Result};
use crate::row::Row;
use crate::schema::{IndexDefinition, IndexType, Schema};
use crate::transaction::Transaction;
use dtdb_storage::{
    CompressionType, DbKey, DbValue, EngineOptions, Executor, FramedLog, FsyncMethod, LogFormat,
    PeriodicHandle, Priority, StorageEngine, WalEntry,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

/// A single relational-layer mutation: an insert/update (`Put`) or a delete.
///
/// This is the public mutation primitive used by `TransactionRecord` and any
/// future relational APIs that need to describe pending writes. It is
/// intentionally narrower than `dtdb_storage::WalEntry` — no nested
/// `Batch` variant — because nesting is only meaningful inside the storage
/// engine's WAL, never at the relational/transaction layer.
///
/// The bincode tag layout matches `WalEntry`'s first two variants
/// (0=Put, 1=Delete) so existing on-disk transaction logs decode unchanged.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum RelationalMutation {
    Put { key: DbKey, value: DbValue },
    Delete { key: DbKey },
}

impl From<RelationalMutation> for WalEntry {
    fn from(m: RelationalMutation) -> WalEntry {
        match m {
            RelationalMutation::Put { key, value } => WalEntry::Put { key, value },
            RelationalMutation::Delete { key } => WalEntry::Delete { key },
        }
    }
}

/// On-disk format tag for the transaction log. The magic distinguishes it from
/// other `FramedLog` files (e.g. a storage WAL) so opening the wrong kind fails
/// loudly on the header check. Exposed so white-box tests and offline tooling
/// can read/write the log in its real framed format.
pub const TXN_LOG_FORMAT: LogFormat = LogFormat {
    magic: *b"DTXN",
    version: 1,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum TransactionRecord {
    Prepared {
        tx_id: u64,
        mutations: HashMap<String, Vec<RelationalMutation>>,
        #[serde(default)]
        old_rows: Option<HashMap<String, HashMap<DbKey, Row>>>,
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

#[derive(Debug, Clone)]
pub struct TableWriteEntry {
    pub entry: WalEntry,
    pub row: Option<Row>,
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

    /// Helper to generate the index keys for a row.
    pub fn get_index_keys(
        &self,
        index_name: &str,
        row: &Row,
        pk_key: &DbKey,
    ) -> Result<Vec<DbKey>> {
        let idx = self
            .schema
            .indexes
            .iter()
            .find(|i| i.name == index_name)
            .ok_or_else(|| {
                RelationalError::SchemaMismatch(format!("Index '{}' not found", index_name))
            })?;
        let mut keys = Vec::new();
        if idx.index_type == IndexType::FullText {
            let col_name = &idx.columns[0];
            let col_idx = self.schema.column_index(col_name).unwrap();
            let col_val = &row.values[col_idx];
            if let DbValue::String(text) = col_val {
                let tokenizer_name = idx.tokenizer.as_deref().unwrap_or("simple");
                if let Some(tokenizer) = crate::tokenizer::get_tokenizer(tokenizer_name) {
                    let mut tokens = tokenizer.tokenize(text);
                    tokens.sort();
                    tokens.dedup();
                    for token in tokens {
                        let idx_key = DbKey::Composite(vec![DbKey::String(token), pk_key.clone()]);
                        keys.push(idx_key);
                    }
                }
            }
        } else {
            let mut col_keys = Vec::new();
            for col_name in &idx.columns {
                let col_idx = self.schema.column_index(col_name).unwrap();
                let col_val = &row.values[col_idx];
                if matches!(col_val, DbValue::Null) {
                    continue;
                }
                let k = match col_val {
                    DbValue::Int(v) => DbKey::Int(*v),
                    DbValue::String(s) => DbKey::String(s.clone()),
                    DbValue::Bool(b) => DbKey::Bool(*b),
                    _ => continue,
                };
                col_keys.push(k);
            }
            if col_keys.len() == idx.columns.len() {
                col_keys.push(pk_key.clone());
                keys.push(DbKey::Composite(col_keys));
            }
        }
        Ok(keys)
    }

    /// Performs a write batch of mutations to the table, splitting updates across locality groups.
    pub fn write_batch(
        &self,
        entries: Vec<TableWriteEntry>,
        preset_old_rows: Option<HashMap<DbKey, Row>>,
    ) -> Result<()> {
        let mut group_batches: HashMap<String, Vec<WalEntry>> = HashMap::new();
        for group in self.engines.keys() {
            group_batches.insert(group.clone(), Vec::new());
        }

        let mut index_batches: HashMap<String, Vec<WalEntry>> = HashMap::new();
        for idx in &self.schema.indexes {
            index_batches.insert(idx.name.clone(), Vec::new());
        }

        // Fetch old rows for index maintenance up-front to avoid N+1 point lookups
        let old_rows = if let Some(preset) = preset_old_rows {
            preset
        } else {
            let mut fetched = HashMap::new();
            if !self.schema.indexes.is_empty() {
                let mut keys_to_fetch = Vec::new();
                for wrapper in &entries {
                    match &wrapper.entry {
                        WalEntry::Put { key, .. } => keys_to_fetch.push(key.clone()),
                        WalEntry::Delete { key } => keys_to_fetch.push(key.clone()),
                        WalEntry::Batch(_) => {}
                    }
                }
                if !keys_to_fetch.is_empty() {
                    let rows = self.multi_get(&keys_to_fetch, None)?;
                    for (k, r) in keys_to_fetch.into_iter().zip(rows) {
                        if let Some(row) = r {
                            fetched.insert(k, row);
                        }
                    }
                }
            }
            fetched
        };

        for wrapper in entries {
            match wrapper.entry {
                WalEntry::Put { key, value } => {
                    let mut full_row_opt = wrapper.row;
                    let need_full_row = self.engines.len() > 1 || !self.schema.indexes.is_empty();
                    if need_full_row && full_row_opt.is_none() {
                        let bytes = match &value {
                            DbValue::Bytes(b) => b,
                            _ => {
                                return Err(RelationalError::Storage(
                                    dtdb_storage::StorageError::Corruption(
                                        "Expected bytes for row serialization".to_string(),
                                    ),
                                ));
                            }
                        };
                        full_row_opt = Some(Row::from_bytes(bytes)?);
                    }

                    // If we have indexes, perform read-before-write maintenance
                    if !self.schema.indexes.is_empty() {
                        let full_row = full_row_opt.as_ref().unwrap();
                        let old_row_opt = old_rows.get(&key);
                        if let Some(old_row) = old_row_opt {
                            for idx in &self.schema.indexes {
                                if idx.index_type == IndexType::FullText {
                                    let col_name = &idx.columns[0];
                                    let col_idx = self.schema.column_index(col_name).unwrap();
                                    let col_val = &old_row.values[col_idx];
                                    if let DbValue::String(old_text) = col_val {
                                        let tokenizer_name =
                                            idx.tokenizer.as_deref().unwrap_or("simple");
                                        if let Some(tokenizer) =
                                            crate::tokenizer::get_tokenizer(tokenizer_name)
                                        {
                                            let mut tokens = tokenizer.tokenize(old_text);
                                            tokens.sort();
                                            tokens.dedup();
                                            for token in tokens {
                                                let idx_key = DbKey::Composite(vec![
                                                    DbKey::String(token),
                                                    key.clone(),
                                                ]);
                                                index_batches
                                                    .get_mut(&idx.name)
                                                    .unwrap()
                                                    .push(WalEntry::Delete { key: idx_key });
                                            }
                                        }
                                    }
                                } else {
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

                        // Add new index entry
                        for idx in &self.schema.indexes {
                            if idx.index_type == IndexType::FullText {
                                let col_name = &idx.columns[0];
                                let col_idx = self.schema.column_index(col_name).unwrap();
                                let col_val = &full_row.values[col_idx];
                                if let DbValue::String(new_text) = col_val {
                                    let tokenizer_name =
                                        idx.tokenizer.as_deref().unwrap_or("simple");
                                    if let Some(tokenizer) =
                                        crate::tokenizer::get_tokenizer(tokenizer_name)
                                    {
                                        let tokens = tokenizer.tokenize(new_text);
                                        let mut token_positions: std::collections::HashMap<
                                            String,
                                            Vec<u32>,
                                        > = std::collections::HashMap::new();
                                        for (pos, token) in tokens.into_iter().enumerate() {
                                            token_positions
                                                .entry(token)
                                                .or_default()
                                                .push(pos as u32);
                                        }
                                        for (token, positions) in token_positions {
                                            let idx_key = DbKey::Composite(vec![
                                                DbKey::String(token),
                                                key.clone(),
                                            ]);
                                            let value_bytes =
                                                bincode::serialize(&positions).unwrap();
                                            index_batches.get_mut(&idx.name).unwrap().push(
                                                WalEntry::Put {
                                                    key: idx_key,
                                                    value: DbValue::Bytes(value_bytes),
                                                },
                                            );
                                        }
                                    }
                                }
                            } else {
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
                    }

                    if self.engines.len() == 1 {
                        let (_, batch) = group_batches.iter_mut().next().unwrap();
                        let sub_bytes = match &value {
                            DbValue::Bytes(b) => b.clone(),
                            _ => full_row_opt.as_ref().unwrap().to_bytes()?,
                        };
                        batch.push(WalEntry::Put {
                            key: key.clone(),
                            value: DbValue::Bytes(sub_bytes),
                        });
                    } else {
                        let full_row = full_row_opt.as_ref().unwrap();
                        for (group, batch) in &mut group_batches {
                            let sub_row = self.schema.split_row(full_row, group);
                            let sub_bytes = sub_row.to_bytes()?;
                            batch.push(WalEntry::Put {
                                key: key.clone(),
                                value: DbValue::Bytes(sub_bytes),
                            });
                        }
                    }
                }
                WalEntry::Delete { key } => {
                    // If we have indexes, perform read-before-write maintenance
                    if !self.schema.indexes.is_empty() {
                        let old_row_opt = old_rows.get(&key);
                        if let Some(old_row) = old_row_opt {
                            for idx in &self.schema.indexes {
                                if idx.index_type == IndexType::FullText {
                                    let col_name = &idx.columns[0];
                                    let col_idx = self.schema.column_index(col_name).unwrap();
                                    let col_val = &old_row.values[col_idx];
                                    if let DbValue::String(old_text) = col_val {
                                        let tokenizer_name =
                                            idx.tokenizer.as_deref().unwrap_or("simple");
                                        if let Some(tokenizer) =
                                            crate::tokenizer::get_tokenizer(tokenizer_name)
                                        {
                                            let mut tokens = tokenizer.tokenize(old_text);
                                            tokens.sort();
                                            tokens.dedup();
                                            for token in tokens {
                                                let idx_key = DbKey::Composite(vec![
                                                    DbKey::String(token),
                                                    key.clone(),
                                                ]);
                                                index_batches
                                                    .get_mut(&idx.name)
                                                    .unwrap()
                                                    .push(WalEntry::Delete { key: idx_key });
                                            }
                                        }
                                    }
                                } else {
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

    /// Fsyncs the WAL of every storage engine backing this table (locality
    /// group engines and index engines), making all applied mutations durable.
    /// Used by the database checkpoint to flush committed data before the
    /// transaction log that redo-protects it is truncated.
    pub fn sync_all_engines(&self) -> Result<()> {
        for engine in self.engines.values() {
            engine.sync_wal()?;
        }
        for engine in self.index_engines.values() {
            engine.sync_wal()?;
        }
        Ok(())
    }

    /// Fetches a row by primary key, reading only the necessary locality group engines.
    pub fn get(&self, key: &DbKey, columns: Option<&[String]>) -> Result<Option<Row>> {
        let mut needed_groups = HashSet::new();
        if let Some(cols) = columns {
            for col_name in cols {
                if let Some(col) = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.matches_name(col_name))
                {
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

    pub fn multi_get(
        &self,
        keys: &[DbKey],
        columns: Option<&[String]>,
    ) -> Result<Vec<Option<Row>>> {
        let mut needed_groups = HashSet::new();
        if let Some(cols) = columns {
            for col_name in cols {
                if let Some(col) = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.matches_name(col_name))
                {
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

        let mut group_results = HashMap::new();
        for group in &needed_groups {
            if let Some(engine) = self.engines.get(group) {
                let db_values = engine.multi_get(keys)?;
                let mut sub_rows = Vec::with_capacity(keys.len());
                for val in db_values {
                    if let Some(DbValue::Bytes(bytes)) = val {
                        let sub_row = Row::from_bytes(&bytes)?;
                        sub_rows.push(Some(sub_row));
                    } else {
                        sub_rows.push(None);
                    }
                }
                group_results.insert(group.clone(), sub_rows);
            }
        }

        let mut final_rows = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let mut group_rows = HashMap::new();
            let mut found_any = false;
            for group in &needed_groups {
                if let Some(sub_rows) = group_results.get(group) {
                    if let Some(sub_row) = &sub_rows[i] {
                        group_rows.insert(group.clone(), Some(sub_row.clone()));
                        found_any = true;
                    } else {
                        group_rows.insert(group.clone(), None);
                    }
                }
            }
            if found_any {
                final_rows.push(Some(self.schema.merge_rows(&group_rows)));
            } else {
                final_rows.push(None);
            }
        }

        Ok(final_rows)
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
                if let Some(col) = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.matches_name(col_name))
                {
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

    pub fn scan_iter(
        &self,
        start: &DbKey,
        end: &DbKey,
        columns: Option<&[String]>,
    ) -> Result<TableScanIterator> {
        let mut needed_groups = HashSet::new();
        if let Some(cols) = columns {
            for col_name in cols {
                if let Some(col) = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.matches_name(col_name))
                {
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
        // One storage iterator per group *slot* (parallel to `needed_groups_vec`),
        // `None` if the group has no engine yet (nothing written). Slot indexing
        // replaces the old group-name-keyed HashMaps so the per-row merge is flat
        // Vec access with no string hashing.
        let mut group_iters: Vec<Option<dtdb_storage::ScanIterator>> =
            Vec::with_capacity(needed_groups_vec.len());
        for group in &needed_groups_vec {
            match self.engines.get(group) {
                Some(engine) => group_iters.push(Some(engine.scan_iter(start, end)?)),
                None => group_iters.push(None),
            }
        }

        TableScanIterator::new(self.schema.clone(), &needed_groups_vec, group_iters)
    }
}

pub struct TableScanIterator {
    schema: Schema,
    /// Per-group storage iterators, indexed by group *slot* (0..N). `None` when
    /// the group had no engine. Parallel to `group_peeks` and `row_parts`.
    group_iters: Vec<Option<dtdb_storage::ScanIterator>>,
    /// Current peeked `(key, value)` for each group slot. Parallel to
    /// `group_iters`.
    group_peeks: Vec<Option<(DbKey, DbValue)>>,
    /// Per-column merge plan: for each schema column, the `(group_slot,
    /// relative_index)` to copy its value from, or `None` slot if the column's
    /// group is not part of this scan (the column is NULL-filled). Precomputed
    /// once at construction so the per-row merge is pure index arithmetic --
    /// no group-name hashing or lookups.
    column_mapping: Vec<(Option<usize>, usize)>,
    /// Reusable per-row scratch holding each slot's deserialized sub-row, so the
    /// merge does not allocate a fresh map/Vec every row.
    row_parts: Vec<Option<Row>>,
    peeked: Option<(DbKey, Row)>,
    /// True when the whole table is a single locality group. In that case the
    /// sole group's stored sub-row already *is* the full row in schema order, so
    /// `advance` returns it directly and skips the merge (and its second Vec +
    /// per-column value clones) entirely.
    single_group: bool,
}

impl TableScanIterator {
    pub fn new(
        schema: Schema,
        needed_groups: &[String],
        mut group_iters: Vec<Option<dtdb_storage::ScanIterator>>,
    ) -> Result<Self> {
        // Prime one peek per slot (parallel to `group_iters`).
        let mut group_peeks = Vec::with_capacity(group_iters.len());
        for iter in group_iters.iter_mut() {
            match iter {
                Some(it) => group_peeks.push(it.next()?),
                None => group_peeks.push(None),
            }
        }

        // Build the per-column merge plan: map each column's (group_name,
        // relative_index) -- from the schema -- to the *slot* of that group
        // within this scan. A column whose group isn't scanned gets `None`
        // (NULL-filled), which is exactly the subset-scan behaviour.
        let column_mapping: Vec<(Option<usize>, usize)> = schema
            .get_relative_indices()
            .iter()
            .map(|(group_name, rel_idx)| {
                let slot = needed_groups.iter().position(|g| g == group_name);
                (slot, *rel_idx)
            })
            .collect();

        let row_parts = vec![None; group_iters.len()];

        // Fast-path eligibility: the table itself is a single locality group.
        // We gate on the *schema* (not just `needed_groups.len() == 1`): a
        // multi-group table queried for one group still needs the merge to
        // place that group's columns at their absolute positions and null-fill
        // the rest, so passthrough would be wrong there. The `len() == 1` guard
        // also makes the slot-0 indexing in the fast path provably in-bounds.
        let single_group = schema.locality_groups().len() == 1 && group_peeks.len() == 1;

        let mut it = Self {
            schema,
            group_iters,
            group_peeks,
            column_mapping,
            row_parts,
            peeked: None,
            single_group,
        };
        it.advance()?;
        Ok(it)
    }

    pub fn peek(&self) -> Option<&(DbKey, Row)> {
        self.peeked.as_ref()
    }

    /// Consumes the current row and advances to the next one, returning the
    /// row by value. Equivalent to `peek().cloned()` followed by `advance()`,
    /// but without cloning the row -- the caller takes ownership of the row the
    /// iterator already materialized. Used by hot scan loops that consume every
    /// row anyway.
    pub fn take_next(&mut self) -> Result<Option<(DbKey, Row)>> {
        match self.peeked.take() {
            Some(pair) => {
                self.advance()?;
                Ok(Some(pair))
            }
            None => Ok(None),
        }
    }

    pub fn advance(&mut self) -> Result<()> {
        // Single-locality-group fast path: the sole group's stored sub-row is
        // already the full row in schema order, so there is nothing to merge.
        // This skips the per-row min-key scan, the `row_parts` HashMap and its
        // group-name String clones, and `merge_rows`' per-column String-keyed
        // lookups + second Vec allocation -- the bulk of the per-row churn.
        if self.single_group {
            // Sole group lives in slot 0 (guaranteed by the `single_group` gate).
            let Some((k, value)) = self.group_peeks[0].take() else {
                self.peeked = None;
                return Ok(());
            };
            // Refill the peek from the underlying storage iterator.
            if let Some(iter) = &mut self.group_iters[0] {
                self.group_peeks[0] = iter.next()?;
            }
            let row = match value {
                // Reconcile to the current schema width: a row stored before a
                // later `ALTER TABLE ADD COLUMN` decodes short, and `reconcile_row`
                // pads the missing trailing columns with their defaults so no
                // short row escapes the scan (see ADR 0003).
                DbValue::Bytes(bytes) => self.schema.reconcile_row(Row::from_bytes(&bytes)?),
                // Mirror the general path: a non-Bytes value yields a row of
                // all-NULLs (a missing/None group contributes only NULLs).
                _ => Row::new(vec![DbValue::Null; self.schema.columns.len()]),
            };
            self.peeked = Some((k, row));
            return Ok(());
        }

        // Multi-group merge path. Find the smallest key across all slots' peeks.
        let mut min_key: Option<DbKey> = None;
        for peek in &self.group_peeks {
            if let Some((k, _)) = peek
                && min_key.as_ref().is_none_or(|m| k < m)
            {
                min_key = Some(k.clone());
            }
        }

        let Some(k) = min_key else {
            self.peeked = None;
            return Ok(());
        };

        // Gather each slot's sub-row for this key into the reusable scratch,
        // advancing the slots that matched. A slot not at `k` contributes NULLs.
        for slot in 0..self.group_peeks.len() {
            let matches = matches!(&self.group_peeks[slot], Some((peek_k, _)) if *peek_k == k);
            if matches {
                let (_peek_k, peek_v) = self.group_peeks[slot].take().unwrap();
                self.row_parts[slot] = match peek_v {
                    DbValue::Bytes(bytes) => Some(Row::from_bytes(&bytes)?),
                    _ => None,
                };
                if let Some(iter) = &mut self.group_iters[slot] {
                    self.group_peeks[slot] = iter.next()?;
                }
            } else {
                self.row_parts[slot] = None;
            }
        }

        // Index-keyed merge: copy each column from its slot's sub-row at the
        // precomputed relative index. No group-name hashing, no per-row map.
        // Seed with per-column defaults (not bare NULL) so a column added by a
        // later `ALTER TABLE` that an old sub-row predates -- its relative index
        // is past that sub-row's end -- reconciles to its default (see ADR 0003).
        let mut full_values = self.schema.default_row_values();
        for (col_idx, (slot, rel_idx)) in self.column_mapping.iter().enumerate() {
            if let Some(slot) = slot
                && let Some(sub_row) = &self.row_parts[*slot]
                && let Some(val) = sub_row.values.get(*rel_idx)
            {
                full_values[col_idx] = val.clone();
            }
        }

        self.peeked = Some((k, Row::new(full_values)));
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: u64,
    pub total_size_bytes: u64,
    pub locality_group_stats: HashMap<String, GroupStats>,
    pub index_stats: HashMap<String, IndexStats>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq)]
pub struct GroupStats {
    pub num_sstables: usize,
    pub total_sstable_size: u64,
    pub entry_count: u64,
    pub tombstone_count: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq)]
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
    #[serde(default)]
    pub wal_sync_interval_ms: Option<u64>,
    #[serde(default, alias = "sort_memory_budget")]
    pub memory_budget: Option<usize>,
    #[serde(default)]
    pub fsync_method: FsyncMethod,
}

/// Default background WAL-sync interval (milliseconds) for the internal
/// table/index storage engines.
///
/// The relational transaction log is the authoritative durability barrier: a
/// commit fsyncs exactly one `Prepared` record that captures every mutation
/// across every engine. The per-engine storage WALs therefore never need to
/// fsync per write; they sync in the background and are force-flushed in bulk
/// by [`Database::checkpoint`] before the transaction log is truncated. This
/// is what reduces a commit's cost to a single fsync.
const ENGINE_WAL_BACKGROUND_SYNC_MS: u64 = 1000;

/// Once the transaction log grows past this many bytes and no transaction is
/// in flight, a commit triggers a [`Database::checkpoint`]. Checkpointing
/// fsyncs every engine's WAL and truncates the log, so this threshold bounds
/// both the wasted work on the next recovery and the per-engine fsync rate
/// (the fsyncs are amortized across every commit since the last checkpoint).
const CHECKPOINT_LOG_THRESHOLD_BYTES: u64 = 4 * 1024 * 1024;

/// Resolves the WAL-sync interval used when opening an internal storage engine.
///
/// Because the transaction log provides commit durability, engine WALs are
/// always opened in background-sync mode. A caller-supplied positive interval
/// is honored as a tuning knob; `None`/`Some(0)` (which would otherwise mean
/// "fsync every write") is mapped to the background default.
fn engine_wal_sync_interval(db_option: Option<u64>) -> Option<u64> {
    match db_option {
        Some(ms) if ms > 0 => Some(ms),
        _ => Some(ENGINE_WAL_BACKGROUND_SYNC_MS),
    }
}

/// Database represents a catalog of Tables stored in a base directory.
pub struct Database {
    dir_path: PathBuf,
    // Tables are stored behind `Arc` so `get_table` is a cheap refcount bump
    // instead of a deep `Schema` clone on every call (it is called multiple
    // times per query). DDL that mutates a table (create/drop index) uses
    // `Arc::make_mut` for copy-on-write, so in-flight holders keep their
    // snapshot -- matching the previous clone-on-read semantics.
    tables: RwLock<HashMap<String, Arc<Table>>>,
    pub options: DatabaseOptions,
    pub schema_version: std::sync::atomic::AtomicU64,
    transaction_log_path: PathBuf,
    transaction_log: Mutex<Option<FramedLog<TransactionRecord>>>,
    active_transactions: Mutex<HashSet<u64>>,
    global_commit_version: std::sync::atomic::AtomicU64,
    occ_active_transactions: Mutex<HashMap<u64, u64>>,
    commit_history: RwLock<Vec<CommitRecord>>,
    executor: Arc<dyn Executor>,
    active_table_access: Mutex<HashMap<String, HashSet<u64>>>,
    pub auto_increment_sequences: Mutex<HashMap<String, i64>>,
    pub statistics: RwLock<HashMap<String, TableStatistics>>,
    /// Per-table statistics epoch, bumped by `analyze_table` only when the
    /// recomputed statistics actually differ from the previous ones. Unlike
    /// [`schema_version`](Self::schema_version) (which tracks schema *shape* and
    /// gates correctness), this tracks the cost inputs the optimizer reads, so a
    /// plan cache can drop a cost-stale plan when, and only when, the stats it
    /// was compiled against have moved. Process-local and starts empty on open,
    /// matching the in-memory plan cache it serves. Entries are removed on
    /// `drop_table`.
    stats_versions: RwLock<HashMap<String, u64>>,
    /// Keeps the background ANALYZE and periodic-flush schedules alive; dropping
    /// the database cancels them.
    background_analyze_handle: Mutex<Option<PeriodicHandle>>,
    background_flush_handle: Mutex<Option<PeriodicHandle>>,
}

impl Database {
    /// Opens the database catalog directory and loads all tables.
    ///
    /// It scans the base directory for table subdirectories containing `schema.bin`.
    pub fn open(dir_path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_executor(dir_path, dtdb_storage::default_executor())
    }

    pub fn dir_path(&self) -> &Path {
        &self.dir_path
    }

    /// Exposes the current schema version of the database catalog.
    pub fn schema_version(&self) -> u64 {
        self.schema_version
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Exposes the current statistics epoch for `table_name`.
    ///
    /// Returns 0 when the table has never been analyzed (or its stats have not
    /// changed since open). `analyze_table` advances this only when a table's
    /// recomputed statistics actually differ, so a cached plan compiled against
    /// version `v` is cost-current as long as this still returns `v`.
    pub fn stats_version(&self, table_name: &str) -> u64 {
        self.stats_versions
            .read()
            .unwrap()
            .get(table_name)
            .copied()
            .unwrap_or(0)
    }

    pub fn register_tokenizer(&self, name: &str, tokenizer: Arc<dyn crate::tokenizer::Tokenizer>) {
        crate::tokenizer::register_global_tokenizer(name, tokenizer);
    }

    pub fn open_with_executor(
        dir_path: impl AsRef<Path>,
        executor: Arc<dyn Executor>,
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
                wal_sync_interval_ms: None,
                memory_budget: None,
                fsync_method: FsyncMethod::default(),
            }
        };
        Self::open_with_options_and_executor(dir_path, options, executor)
    }

    /// Opens the database catalog directory with specified options and loads all tables.
    pub fn open_with_options(dir_path: impl AsRef<Path>, options: DatabaseOptions) -> Result<Self> {
        Self::open_with_options_and_executor(dir_path, options, dtdb_storage::default_executor())
    }

    /// Opens the database catalog directory with specified options, backed by the
    /// given [`Executor`], and loads all tables.
    pub fn open_with_options_and_executor(
        dir_path: impl AsRef<Path>,
        options: DatabaseOptions,
        executor: Arc<dyn Executor>,
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

        // Clean up stale temp sort spill directory from previous crashes
        let tmp_dir = dir_path.join("_tmp");
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }

        let db_options_path = dir_path.join("db_options.bin");
        let bytes = bincode::serialize(&options)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;
        dtdb_storage::atomic_write(&db_options_path, &bytes, options.fsync_method)
            .map_err(RelationalError::Storage)?;

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
                        wal_sync_interval_ms: engine_wal_sync_interval(
                            options.wal_sync_interval_ms,
                        ),
                        fsync_method: options.fsync_method,
                    };

                    let mut engines = HashMap::new();
                    let groups = schema.locality_groups();

                    // Check for old table layout (backward compatibility): a
                    // table dir that is itself a single storage engine, marked
                    // by the engine's own files (its manifest dir or options).
                    if groups.len() <= 1
                        && groups.contains("")
                        && (path.join("manifest").is_dir() || path.join("options.bin").exists())
                    {
                        let mut group_opts = engine_opts;
                        if let Some(opts) = schema.locality_group_options.get("") {
                            group_opts = opts.apply_to(group_opts);
                        }
                        let engine = Arc::new(StorageEngine::open_with_executor(
                            &path,
                            group_opts,
                            executor.clone(),
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
                            let engine = Arc::new(StorageEngine::open_with_executor(
                                &g_path,
                                group_opts,
                                executor.clone(),
                            )?);
                            engines.insert(group, engine);
                        }
                    }

                    let mut index_engines = HashMap::new();
                    for idx_def in &schema.indexes {
                        let idx_path = Table::index_dir(&path, &idx_def.name);
                        let engine = Arc::new(StorageEngine::open_with_executor(
                            &idx_path,
                            engine_opts,
                            executor.clone(),
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
                        Arc::new(Table {
                            name,
                            schema,
                            engines,
                            index_engines,
                        }),
                    );
                }
            }
        }

        let transaction_log_path = dir_path.join("transactions.log");
        let transaction_log = Mutex::new(None);
        let active_transactions = Mutex::new(HashSet::new());
        let global_commit_version = std::sync::atomic::AtomicU64::new(0);
        let occ_active_transactions = Mutex::new(HashMap::new());
        let commit_history = RwLock::new(Vec::new());
        let active_table_access = Mutex::new(HashMap::new());
        let auto_increment_sequences = Mutex::new(HashMap::new());

        let db = Self {
            dir_path,
            tables: RwLock::new(tables),
            options,
            schema_version: std::sync::atomic::AtomicU64::new(0),
            transaction_log_path,
            transaction_log,
            active_transactions,
            global_commit_version,
            occ_active_transactions,
            commit_history,
            executor,
            active_table_access,
            auto_increment_sequences,
            statistics: RwLock::new(statistics),
            stats_versions: RwLock::new(HashMap::new()),
            background_analyze_handle: Mutex::new(None),
            background_flush_handle: Mutex::new(None),
        };

        db.recover_transactions()?;

        // Initialize auto-increment sequences for loaded tables.
        // This must run AFTER recover_transactions: recovery may roll forward a
        // prepared transaction whose rows contain an auto-increment value larger
        // than anything previously visible on disk. Seeding the sequence before
        // recovery would leave it pointing below the recovered max, causing the
        // very next allocation to collide with an existing row.
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

        // Open the persistent transaction log. A `None` sync interval makes
        // every append fsync, which is required: a transaction's `Prepared`
        // record must be durable before its mutations are applied.
        let log = FramedLog::open(
            &db.transaction_log_path,
            TXN_LOG_FORMAT,
            None,
            db.options.fsync_method,
        )
        .map_err(RelationalError::Storage)?;
        *db.transaction_log.lock().unwrap() = Some(log);

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
        schema.save_to_file(&schema_path, self.options.fsync_method)?;

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
            wal_sync_interval_ms: engine_wal_sync_interval(self.options.wal_sync_interval_ms),
            fsync_method: self.options.fsync_method,
        };

        let mut engines = HashMap::new();
        let groups = schema.locality_groups();
        if groups.len() <= 1 && groups.contains("") {
            let mut group_opts = engine_opts;
            if let Some(opts) = schema.locality_group_options.get("") {
                group_opts = opts.apply_to(group_opts);
            }
            let engine = Arc::new(StorageEngine::open_with_executor(
                &table_path,
                group_opts,
                self.executor.clone(),
            )?);
            engines.insert("".to_string(), engine);
        } else {
            for group in groups {
                let g_path = Table::group_dir(&table_path, &group);
                let mut group_opts = engine_opts;
                if let Some(opts) = schema.locality_group_options.get(&group) {
                    group_opts = opts.apply_to(group_opts);
                }
                let engine = Arc::new(StorageEngine::open_with_executor(
                    &g_path,
                    group_opts,
                    self.executor.clone(),
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
        dtdb_storage::atomic_write(&stats_path, &bytes, self.options.fsync_method)
            .map_err(RelationalError::Storage)?;

        let mut index_engines = HashMap::new();
        for idx_def in &schema.indexes {
            let idx_path = Table::index_dir(&table_path, &idx_def.name);
            let engine = Arc::new(StorageEngine::open_with_executor(
                &idx_path,
                engine_opts,
                self.executor.clone(),
            )?);
            index_engines.insert(idx_def.name.clone(), engine);
        }

        tables_guard.insert(
            name.to_string(),
            Arc::new(Table {
                name: name.to_string(),
                schema,
                engines,
                index_engines,
            }),
        );

        if has_auto_inc {
            let mut seqs = self.auto_increment_sequences.lock().unwrap();
            seqs.insert(name.to_string(), 1);
        }

        self.schema_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
        {
            // Forget the stats epoch so a future table reusing this name starts
            // fresh; any plan referencing the old table is already invalidated
            // by the schema_version bump below.
            let mut versions = self.stats_versions.write().unwrap();
            versions.remove(name);
        }

        // Wait until all transactions currently accessing this table have finished.
        // TODO: bound this spin-wait with a timeout that returns an error (and
        // names the offending table) instead of spinning forever. A stuck or
        // leaked reader currently hangs the DDL indefinitely with no diagnostic.
        // Same pattern in `create_index` and `drop_index`.
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
            // Persist the directory entry change so a crash mid-drop doesn't
            // resurrect the table name on the next mount.
            dtdb_storage::fsync_parent_dir(&temp_table_path)?;

            // Remove the temporary directory non-atomically (crash safe).
            fs::remove_dir_all(temp_table_path)?;

            // Clean up the tracking entry.
            let mut access = self.active_table_access.lock().unwrap();
            access.remove(name);
        }

        self.schema_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
    pub fn get_table(&self, name: &str) -> Result<Arc<Table>> {
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
        let mut log_guard = self.transaction_log.lock().unwrap();
        if let Some(ref mut log) = *log_guard {
            // The log is opened with a `None` sync interval, so this append
            // fsyncs before returning.
            log.append(record).map_err(RelationalError::Storage)?;
        }
        Ok(())
    }

    pub fn write_transaction_record(&self, record: &TransactionRecord) -> Result<()> {
        let mut active = self.active_transactions.lock().unwrap();
        if let TransactionRecord::Prepared { tx_id, .. } = record {
            active.insert(*tx_id);
        }
        drop(active);
        self.append_record(record)?;
        Ok(())
    }

    pub fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let no_active = {
            let mut active = self.active_transactions.lock().unwrap();
            active.remove(&tx_id);
            active.is_empty()
        };

        // The transaction is already durable: its `Prepared` record (which
        // carries every mutation) was fsynced before the engines were updated.
        // We no longer truncate eagerly here, nor fsync per engine — that is
        // deferred to a checkpoint so that the steady-state cost of a commit is
        // a single fsync. Once the log has grown past the threshold and no
        // transaction is in flight (so every logged record is fully applied to
        // the engines), fold the redo records into durable engine state.
        if no_active && self.transaction_log_len()? >= CHECKPOINT_LOG_THRESHOLD_BYTES {
            // Safe to checkpoint without re-acquiring the commit lock: this runs
            // inside the commit closure, which already holds it, and the empty
            // in-flight set means every logged record is fully applied.
            self.checkpoint_locked()?;
        }
        Ok(())
    }

    /// Returns the current size of the transaction log in bytes.
    fn transaction_log_len(&self) -> Result<u64> {
        let log_guard = self.transaction_log.lock().unwrap();
        match *log_guard {
            Some(ref log) => Ok(log.size()),
            None => Ok(0),
        }
    }

    /// Fsyncs every table/index storage engine's WAL, making all applied
    /// mutations durable in the engines themselves.
    fn sync_all_table_engines(&self) -> Result<()> {
        let tables = self.tables.read().unwrap();
        for table in tables.values() {
            table.sync_all_engines()?;
        }
        Ok(())
    }

    /// Checkpoints the database: durably flushes all storage-engine WALs and
    /// then truncates the transaction log.
    ///
    /// After this returns, every transaction previously recorded in the log is
    /// durably persisted in the storage engines, so the log's redo records are
    /// no longer needed for crash recovery and the log can start fresh. This is
    /// what amortizes the per-engine fsyncs across many commits.
    ///
    /// Takes the commit lock for its duration so it cannot interleave with a
    /// commit that has fsynced its `Prepared` record but not yet applied it to
    /// the engines — which would otherwise let the truncation drop a durable
    /// transaction. Use this for an explicit flush (e.g. clean shutdown).
    pub fn checkpoint(&self) -> Result<()> {
        let _commit_guard = self.commit_history.write().unwrap();
        self.checkpoint_locked()
    }

    /// Checkpoint body. The caller must hold the commit lock (`commit_history`
    /// write lock) so that no transaction is mid-apply; the commit path
    /// satisfies this directly, and [`Database::checkpoint`] acquires it.
    fn checkpoint_locked(&self) -> Result<()> {
        // 1. Make all applied mutations durable in the engines.
        self.sync_all_table_engines()?;

        // 2. The log's redo records are now redundant — discard them.
        let mut log_guard = self.transaction_log.lock().unwrap();
        if let Some(ref mut log) = *log_guard {
            log.reset().map_err(RelationalError::Storage)?;
        }
        Ok(())
    }

    fn recover_transactions(&self) -> Result<()> {
        // Read every record in *log order*, which is commit order (records are
        // appended sequentially under the commit lock). Replay must preserve
        // this order so later writes to a key win. Framing, checksums, and
        // truncation handling all live in `FramedLog::recover`; a missing file
        // recovers as empty.
        let records =
            FramedLog::<TransactionRecord>::recover(&self.transaction_log_path, TXN_LOG_FORMAT)
                .map_err(RelationalError::Storage)?;

        // Keep only the `Prepared` records. A `Prepared` record is the durable
        // commit point, so every one found here is a committed transaction
        // whose mutations must be present in the engines. The `Committed`
        // marker is informational and ignored: re-applying a transaction is
        // idempotent (LSM is last-writer-wins and each record carries its own
        // pre-image `old_rows` for deterministic index maintenance), so
        // replaying records that a background WAL sync had already persisted
        // before the crash is harmless.
        #[allow(clippy::type_complexity)]
        let prepared: Vec<(
            u64,
            HashMap<String, Vec<RelationalMutation>>,
            Option<HashMap<String, HashMap<DbKey, Row>>>,
        )> = records
            .into_iter()
            .filter_map(|record| match record {
                TransactionRecord::Prepared {
                    tx_id,
                    mutations,
                    old_rows,
                } => Some((tx_id, mutations, old_rows)),
                TransactionRecord::Committed { .. } => None,
            })
            .collect();

        if prepared.is_empty() {
            // Nothing to roll forward; drop any stale log so the persistent
            // handle (opened next) starts a clean epoch with a fresh header.
            let _ = fs::remove_file(&self.transaction_log_path);
            return Ok(());
        }

        tracing::info!(count = prepared.len(), "replaying committed transactions");
        for (tx_id, mutations, old_rows_opt) in prepared {
            for (table_name, entries) in mutations {
                if let Ok(table) = self.get_table(&table_name) {
                    tracing::info!(
                        tx_id,
                        table = %table_name,
                        "replaying transaction"
                    );
                    let write_entries = entries
                        .into_iter()
                        .map(|m| TableWriteEntry {
                            entry: WalEntry::from(m),
                            row: None,
                        })
                        .collect();
                    let table_old_rows = old_rows_opt
                        .as_ref()
                        .and_then(|m| m.get(&table_name))
                        .cloned();
                    table.write_batch(write_entries, table_old_rows)?;
                }
            }
        }

        // The replayed mutations now live in the engine memtables/WALs. Make
        // them durable, then discard the log so the next epoch starts clean.
        // (The persistent log handle is not open yet during recovery, so we
        // remove the file; the handle opened next recreates it with a fresh
        // header.)
        self.sync_all_table_engines()?;
        let _ = fs::remove_file(&self.transaction_log_path);
        Ok(())
    }

    pub fn has_active_transactions(&self) -> bool {
        !self.occ_active_transactions.lock().unwrap().is_empty()
    }

    pub fn global_commit_version(&self) -> u64 {
        self.global_commit_version
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn register_transaction(&self, tx_id: u64) -> u64 {
        let _history_lock = self.commit_history.read().unwrap();
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
            let mut history = self.commit_history.write().unwrap();
            history.retain(|r| r.commit_version >= min_version);
        }

        let mut access = self.active_table_access.lock().unwrap();
        for readers in access.values_mut() {
            readers.remove(&tx_id);
        }
    }

    pub fn validate_and_commit<F>(
        &self,
        tx_id: u64,
        start_version: u64,
        read_set: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
        scan_ranges: &HashMap<String, Vec<(dtdb_storage::DbKey, dtdb_storage::DbKey)>>,
        write_keys: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
        commit_fn: F,
    ) -> Result<u64>
    where
        F: FnOnce() -> Result<()>,
    {
        let snapshot_max_version = {
            let history = self.commit_history.read().unwrap();

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

            // Find current maximum version in history, or start_version if empty
            history
                .iter()
                .map(|r| r.commit_version)
                .max()
                .unwrap_or(start_version)
        };

        // Get pruning limit version before write-locking commit_history to prevent deadlock
        let min_start_version = {
            let active = self.occ_active_transactions.lock().unwrap();
            active.values().copied().min()
        };

        // Phase 2: Acquire write lock, perform delta validation, increment, append and prune
        let mut history = self.commit_history.write().unwrap();

        // Perform delta validation against history for commits > snapshot_max_version
        for record in history.iter() {
            if record.commit_version > snapshot_max_version {
                // Check read-write conflict
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

                // Check phantom conflict
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

                // Check write-write conflict
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

        // Execute the commit closure under the write lock
        commit_fn()?;

        // Increment global commit version to get a unique commit version
        let commit_version = self
            .global_commit_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        // Append to history
        let new_record = CommitRecord {
            commit_version,
            keys: write_keys.clone(),
        };
        history.push(new_record);

        // Prune older history
        let prune_limit = min_start_version.unwrap_or(commit_version);
        history.retain(|r| r.commit_version >= prune_limit);

        Ok(commit_version)
    }

    pub fn validate_read_only(
        &self,
        tx_id: u64,
        start_version: u64,
        read_set: &HashMap<String, HashSet<dtdb_storage::DbKey>>,
        scan_ranges: &HashMap<String, Vec<(dtdb_storage::DbKey, dtdb_storage::DbKey)>>,
    ) -> Result<()> {
        let history = self.commit_history.read().unwrap();

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
        index_type: IndexType,
        tokenizer: Option<String>,
    ) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();

        // 1. Check table existence
        let table_arc = tables_guard
            .get_mut(table_name)
            .ok_or_else(|| RelationalError::TableNotFound(table_name.to_string()))?;
        // Copy-on-write: clone the Table only if another holder (e.g. an
        // in-flight transaction's cache) still references this Arc.
        let table = Arc::make_mut(table_arc);

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

        // If it's FULLTEXT index, perform type and column count checks
        if index_type == IndexType::FullText {
            if columns.len() != 1 {
                return Err(RelationalError::SchemaMismatch(
                    "FULLTEXT index must be on exactly one column".to_string(),
                ));
            }
            let col_idx = table.schema.column_index(&columns[0]).unwrap();
            let col = &table.schema.columns[col_idx];
            if col.data_type != crate::schema::DataType::String {
                return Err(RelationalError::SchemaMismatch(
                    "FULLTEXT index can only be created on a STRING column".to_string(),
                ));
            }
            let tokenizer_name = tokenizer.as_deref().unwrap_or("simple");
            if crate::tokenizer::get_tokenizer(tokenizer_name).is_none() {
                return Err(RelationalError::SchemaMismatch(format!(
                    "Tokenizer '{}' not found",
                    tokenizer_name
                )));
            }
        }

        // 4. Stage the new index definition. We do NOT publish it to the
        //    on-disk schema yet: if we crash before the index has been fully
        //    populated below, the recovered schema would declare an index whose
        //    backing engine is missing or partial, silently corrupting any
        //    queries that consult it. Persist the schema only after the index
        //    data is durable.
        let table_path = self.dir_path.join(table_name);
        let schema_path = table_path.join("schema.bin");
        let new_index_def = IndexDefinition {
            name: index_name.to_string(),
            columns: columns.clone(),
            index_type,
            tokenizer: tokenizer.clone(),
        };

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
            wal_sync_interval_ms: engine_wal_sync_interval(self.options.wal_sync_interval_ms),
            fsync_method: self.options.fsync_method,
        };

        // 6. Create index directory
        let idx_path = Table::index_dir(&table_path, index_name);
        fs::create_dir_all(&idx_path)?;

        // 7. Open the storage engine
        let engine = Arc::new(StorageEngine::open_with_executor(
            &idx_path,
            engine_opts,
            self.executor.clone(),
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
            if index_type == IndexType::FullText {
                let col_name = &columns[0];
                let col_idx = table.schema.column_index(col_name).unwrap();
                let col_val = &row.values[col_idx];
                if let DbValue::String(text) = col_val {
                    let tokenizer_name = tokenizer.as_deref().unwrap_or("simple");
                    let tok = crate::tokenizer::get_tokenizer(tokenizer_name).unwrap();
                    let tokens = tok.tokenize(text);
                    let mut token_positions: std::collections::HashMap<String, Vec<u32>> =
                        std::collections::HashMap::new();
                    for (pos, token) in tokens.into_iter().enumerate() {
                        token_positions.entry(token).or_default().push(pos as u32);
                    }
                    for (token, positions) in token_positions {
                        let idx_key = DbKey::Composite(vec![DbKey::String(token), pk_key.clone()]);
                        let value_bytes = bincode::serialize(&positions).unwrap();
                        index_entries.push(WalEntry::Put {
                            key: idx_key,
                            value: DbValue::Bytes(value_bytes),
                        });
                    }
                }
            } else {
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
        }

        if !index_entries.is_empty() {
            engine.write_batch(index_entries)?;
        }
        // Force the index data through any pending memtable so a crash right
        // after the schema swap below cannot leave the on-disk index empty.
        engine.flush_memtable()?;

        // 10. Persist the schema with the new index AFTER the index data is
        //     durable. Use atomic temp+rename so a crash mid-write cannot
        //     truncate the schema file.
        let mut new_schema = table.schema.clone();
        new_schema.indexes.push(new_index_def.clone());
        new_schema.save_to_file(&schema_path, self.options.fsync_method)?;

        // 11. Publish the new index in-memory.
        table.schema.indexes.push(new_index_def);
        table.index_engines.insert(index_name.to_string(), engine);

        self.schema_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    /// Drops a secondary index from a table.
    pub fn drop_index(&self, table_name: &str, index_name: &str) -> Result<()> {
        let mut tables_guard = self.tables.write().unwrap();

        let table_arc = tables_guard
            .get_mut(table_name)
            .ok_or_else(|| RelationalError::TableNotFound(table_name.to_string()))?;
        // Copy-on-write (see create_index).
        let table = Arc::make_mut(table_arc);

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
        table
            .schema
            .save_to_file(&schema_path, self.options.fsync_method)?;

        // 5. Delete on-disk index directory
        let idx_path = Table::index_dir(&table_path, index_name);
        if idx_path.exists() {
            fs::remove_dir_all(idx_path)?;
        }

        self.schema_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    pub fn commit_history_len(&self) -> usize {
        self.commit_history.read().unwrap().len()
    }

    /// Triggers background stats collection if options specify it and it hasn't been started.
    pub fn start_background_analyze_if_needed(&self, db_arc: &Arc<Database>) {
        let Some(ms) = self.options.analyze_frequency_ms else {
            return;
        };
        let mut guard = self.background_analyze_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }
        let weak_db = Arc::downgrade(db_arc);
        // Per-run synthetic transaction id, advanced on each tick.
        let tx_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handle = self.executor.submit_periodic(
            std::time::Duration::from_millis(ms),
            Priority::Low,
            Box::new(move || {
                let Some(db) = weak_db.upgrade() else {
                    return;
                };
                let i = tx_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let tx_id = 1_000_000_000_000 + i;
                let tx = Transaction::new_with_isolation(
                    tx_id,
                    db.clone(),
                    crate::transaction::IsolationLevel::ReadUncommitted,
                );
                if let Err(e) = db.analyze_all(&tx) {
                    tracing::error!(error = ?e, "background analyze failed");
                }
            }),
        );
        *guard = Some(handle);
    }

    /// Triggers periodic memtable flushing if `flush_interval_ms` is configured
    /// and it hasn't been started.
    pub fn start_background_flush_if_needed(&self, db_arc: &Arc<Database>) {
        let Some(ms) = self.options.flush_interval_ms else {
            return;
        };
        let mut guard = self.background_flush_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }
        let weak_db = Arc::downgrade(db_arc);
        let handle = self.executor.submit_periodic(
            std::time::Duration::from_millis(ms),
            Priority::High,
            Box::new(move || {
                let Some(db) = weak_db.upgrade() else {
                    return;
                };
                for table_name in db.list_tables() {
                    if let Ok(table) = db.get_table(&table_name)
                        && let Err(e) = table.flush_memtable()
                    {
                        tracing::error!(table = %table_name, error = %e, "periodic flush failed");
                    }
                }
            }),
        );
        *guard = Some(handle);
    }

    /// Gathers database statistics for a single table.
    pub fn analyze_table(&self, table_name: &str, _tx: &Transaction) -> Result<()> {
        let table = self.get_table(table_name)?;

        // 1. Fetch storage engine statistics for locality groups and estimate row count
        let mut locality_group_stats = HashMap::new();
        let mut total_size_bytes = 0;
        let mut estimated_row_count = 0;
        let mut got_row_count = false;

        for (group, engine) in &table.engines {
            let stats = engine.get_statistics()?;
            let engine_rows = (stats.sstable_entries + stats.memtable_entries)
                .saturating_sub(stats.sstable_tombstones + stats.memtable_tombstones);
            if !got_row_count || group.is_empty() {
                estimated_row_count = engine_rows;
                got_row_count = true;
            }

            let group_stats = GroupStats {
                num_sstables: stats.num_sstables,
                total_sstable_size: stats.total_sstable_size,
                entry_count: stats.sstable_entries + stats.memtable_entries,
                tombstone_count: stats.sstable_tombstones + stats.memtable_tombstones,
            };
            total_size_bytes += stats.total_sstable_size;
            locality_group_stats.insert(group.clone(), group_stats);
        }

        let row_count = estimated_row_count;

        // 2. Fetch index statistics using metadata and scan index keys for uniqueness
        let mut index_stats = HashMap::new();
        let (min_pk, max_pk) = table.schema.primary_key_bounds()?;
        for idx_def in &table.schema.indexes {
            if let Some(index_engine) = table.index_engines.get(&idx_def.name) {
                let index_engine_stats = index_engine.get_statistics()?;
                total_size_bytes += index_engine_stats.total_sstable_size;

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

                let mut scan_iter = index_engine.scan_iter(&start_bound, &end_bound)?;
                let mut entry_count = 0;
                let mut unique_prefixes = HashSet::new();
                while let Some((idx_key, _)) = scan_iter.next()? {
                    entry_count += 1;
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

        let bytes = bincode::serialize(&stats)
            .map_err(|e| RelationalError::Storage(dtdb_storage::StorageError::Serialization(e)))?;

        // 4. Update in-memory cache and persist to file under catalog read lock
        // to prevent race with drop_table rename.
        let tables_guard = self.tables.read().unwrap();
        if tables_guard.contains_key(table_name) {
            let changed = {
                let mut stats_guard = self.statistics.write().unwrap();
                // Only the cost inputs matter for plan validity, so compare
                // against the previous stats and treat a first-ever analyze
                // (no prior entry) as a change too.
                let changed = stats_guard.get(table_name) != Some(&stats);
                stats_guard.insert(table_name.to_string(), stats);
                changed
            };

            // Advance the statistics epoch only when the numbers actually moved,
            // so a periodic background ANALYZE that recomputes identical stats
            // does not needlessly invalidate cached plans.
            if changed {
                let mut versions = self.stats_versions.write().unwrap();
                *versions.entry(table_name.to_string()).or_insert(0) += 1;
            }

            let table_path = self.dir_path.join(table_name);
            let stats_path = table_path.join("statistics.bin");
            dtdb_storage::atomic_write(&stats_path, &bytes, self.options.fsync_method)
                .map_err(RelationalError::Storage)?;
        }

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

// Background ANALYZE and periodic-flush schedules are cancelled automatically
// when the `PeriodicHandle`s stored on `Database` are dropped, so no explicit
// `Drop` impl is required.
