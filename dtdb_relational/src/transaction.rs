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
    accessed_tables: Mutex<HashSet<String>>,
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
            accessed_tables: Mutex::new(HashSet::new()),
        }
    }

    fn get_table(&self, table_name: &str) -> Result<Table> {
        let table = self.database.get_table(table_name)?;
        let mut accessed = self.accessed_tables.lock().unwrap();
        if accessed.insert(table_name.to_string()) {
            self.database.register_table_access(table_name, self.tx_id);
        }
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

    pub fn multi_get_projected(
        &self,
        table_name: &str,
        keys: &[DbKey],
        columns: Option<&[String]>,
    ) -> Result<Vec<Option<Row>>> {
        let table = self.get_table(table_name)?;
        let mut results = vec![None; keys.len()];
        let mut storage_keys = Vec::new();
        let mut storage_indices = Vec::new();

        // 1. Check transaction write buffer.
        {
            let buffer = self.write_buffer.lock().unwrap();
            if let Some(table_buffer) = buffer.get(table_name) {
                for (i, key) in keys.iter().enumerate() {
                    if let Some(buffered_val) = table_buffer.get(key) {
                        results[i] = buffered_val.clone();
                    } else {
                        storage_keys.push(key.clone());
                        storage_indices.push(i);
                    }
                }
            } else {
                for (i, key) in keys.iter().enumerate() {
                    storage_keys.push(key.clone());
                    storage_indices.push(i);
                }
            }
        }

        if storage_keys.is_empty() {
            return Ok(results);
        }

        // 2. Track the keys in our read set.
        if self.isolation_level == IsolationLevel::SnapshotIsolation
            || self.isolation_level == IsolationLevel::RepeatableRead
        {
            let mut read_set = self.read_set.lock().unwrap();
            let entry = read_set.entry(table_name.to_string()).or_default();
            for key in &storage_keys {
                entry.insert(key.clone());
            }
        }

        // 3. Fall back to underlying storage engine.
        let storage_results = table.multi_get(&storage_keys, columns)?;
        for (idx, row) in storage_indices.into_iter().zip(storage_results) {
            results[idx] = row;
        }

        Ok(results)
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

        // 2. Fetch rows by primary key from transaction/main table in batch
        let mut pks = Vec::new();
        for (idx_key, _) in index_entries {
            if let DbKey::Composite(mut parts) = idx_key
                && let Some(pk) = parts.pop()
            {
                pks.push(pk);
            }
        }

        if !pks.is_empty() {
            let batch_rows = self.multi_get_projected(table_name, &pks, columns)?;
            for (pk, row_opt) in pks.into_iter().zip(batch_rows) {
                resolved_pks.insert(pk.clone());
                if let Some(row) = row_opt {
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
                    && let Some(val) = row.get_by_index(col_idx)
                {
                    if idx_def.index_type == crate::schema::IndexType::FullText {
                        if let DbValue::String(s) = val {
                            let tokenizer_name = idx_def.tokenizer.as_deref().unwrap_or("simple");
                            if let Some(tokenizer) = crate::tokenizer::get_tokenizer(tokenizer_name)
                            {
                                let tokens = tokenizer.tokenize(s.as_str());
                                let mut match_found = false;
                                if let DbKey::String(query_token) = start_val
                                    && tokens.contains(query_token)
                                {
                                    match_found = true;
                                }
                                if match_found {
                                    rows.push(row.clone());
                                }
                            }
                        }
                    } else {
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
        }

        Ok(rows)
    }

    /// Performs a full-text boolean query scan, resolving matching primary keys using the inverted index,
    /// and applying the transaction's write buffer modifications.
    pub fn fulltext_scan(
        &self,
        table_name: &str,
        index_name: &str,
        query_str: &str,
        columns: Option<&[String]>,
    ) -> Result<Vec<Row>> {
        let table = self.get_table(table_name)?;
        let index_engine = table.index_engines.get(index_name).ok_or_else(|| {
            RelationalError::SchemaMismatch(format!("Index '{}' not found", index_name))
        })?;

        let index_def = table
            .schema
            .indexes
            .iter()
            .find(|idx| idx.name == index_name)
            .ok_or_else(|| {
                RelationalError::SchemaMismatch(format!(
                    "Index '{}' definition not found",
                    index_name
                ))
            })?;
        let tokenizer_name = index_def.tokenizer.as_deref().unwrap_or("simple");
        let tokenizer = crate::tokenizer::get_tokenizer(tokenizer_name).ok_or_else(|| {
            RelationalError::SchemaMismatch(format!("Tokenizer '{}' not found", tokenizer_name))
        })?;

        let query = crate::fts_parser::FullTextQuery::parse(query_str, tokenizer.as_ref())
            .map_err(RelationalError::SchemaMismatch)?;

        // 1. Recursively resolve primary key set matching the query from the index engine
        let (min_pk, max_pk) = table.schema.primary_key_bounds()?;
        let matched_pks = self.eval_fulltext_query(index_engine, &query, &min_pk, &max_pk)?;

        // 2. Fetch rows by primary key from storage
        let mut pks: Vec<DbKey> = matched_pks.into_iter().collect();
        pks.sort();

        let mut rows = Vec::new();
        let mut resolved_pks = HashSet::new();

        if !pks.is_empty() {
            let batch_rows = self.multi_get_projected(table_name, &pks, columns)?;
            for (pk, row_opt) in pks.into_iter().zip(batch_rows) {
                resolved_pks.insert(pk.clone());
                if let Some(row) = row_opt {
                    rows.push(row);
                }
            }
        }

        // 3. Scan the transaction `write_buffer` for any matching rows not already resolved by index scan,
        // and remove any rows that have been deleted in this transaction.
        let buffer = self.write_buffer.lock().unwrap();
        if let Some(table_buffer) = buffer.get(table_name) {
            rows.retain(|row| {
                if let Ok(pk) = table.schema.extract_primary_key(row)
                    && let Some(row_opt) = table_buffer.get(&pk)
                {
                    return row_opt.is_some();
                }
                true
            });

            if let Some(idx_def) = table
                .schema
                .indexes
                .iter()
                .find(|idx| idx.name == index_name)
                && let Some(col_name) = idx_def.columns.first()
                && let Some(col_idx) = table.schema.column_index(col_name)
            {
                let tokenizer_name = idx_def.tokenizer.as_deref().unwrap_or("simple");
                if let Some(tokenizer) = crate::tokenizer::get_tokenizer(tokenizer_name) {
                    for (pk, row_opt) in table_buffer {
                        if resolved_pks.contains(pk) {
                            continue;
                        }
                        if let Some(row) = row_opt
                            && self.eval_row_fts_query(&query, row, col_idx, tokenizer.as_ref())
                        {
                            rows.push(row.clone());
                        }
                    }
                }
            }
        }

        Ok(rows)
    }

    fn eval_fulltext_query(
        &self,
        index_engine: &Arc<dtdb_storage::StorageEngine>,
        query: &crate::fts_parser::FullTextQuery,
        min_pk: &DbKey,
        max_pk: &DbKey,
    ) -> Result<HashSet<DbKey>> {
        match query {
            crate::fts_parser::FullTextQuery::Token(tok) => {
                let start_bound =
                    DbKey::Composite(vec![DbKey::String(tok.clone()), min_pk.clone()]);
                let end_bound = DbKey::Composite(vec![DbKey::String(tok.clone()), max_pk.clone()]);
                let index_entries =
                    index_engine.filtered_scan(&start_bound, &end_bound, |_, _| true)?;
                let mut pks = HashSet::new();
                for (idx_key, _) in index_entries {
                    if let DbKey::Composite(mut parts) = idx_key
                        && let Some(pk) = parts.pop()
                    {
                        pks.insert(pk);
                    }
                }
                Ok(pks)
            }
            crate::fts_parser::FullTextQuery::And(left, right) => {
                let left_pks = self.eval_fulltext_query(index_engine, left, min_pk, max_pk)?;
                let right_pks = self.eval_fulltext_query(index_engine, right, min_pk, max_pk)?;
                Ok(left_pks.intersection(&right_pks).cloned().collect())
            }
            crate::fts_parser::FullTextQuery::Or(left, right) => {
                let left_pks = self.eval_fulltext_query(index_engine, left, min_pk, max_pk)?;
                let right_pks = self.eval_fulltext_query(index_engine, right, min_pk, max_pk)?;
                Ok(left_pks.union(&right_pks).cloned().collect())
            }
            crate::fts_parser::FullTextQuery::Phrase(phrase_tokens) => {
                if phrase_tokens.is_empty() {
                    return Ok(HashSet::new());
                }
                if phrase_tokens.len() == 1 {
                    let start_bound = DbKey::Composite(vec![
                        DbKey::String(phrase_tokens[0].clone()),
                        min_pk.clone(),
                    ]);
                    let end_bound = DbKey::Composite(vec![
                        DbKey::String(phrase_tokens[0].clone()),
                        max_pk.clone(),
                    ]);
                    let index_entries =
                        index_engine.filtered_scan(&start_bound, &end_bound, |_, _| true)?;
                    let mut pks = HashSet::new();
                    for (idx_key, _) in index_entries {
                        if let DbKey::Composite(mut parts) = idx_key
                            && let Some(pk) = parts.pop()
                        {
                            pks.insert(pk);
                        }
                    }
                    return Ok(pks);
                }

                let mut token_pos_maps: Vec<HashMap<DbKey, Vec<u32>>> = Vec::new();
                for tok in phrase_tokens {
                    let start_bound =
                        DbKey::Composite(vec![DbKey::String(tok.clone()), min_pk.clone()]);
                    let end_bound =
                        DbKey::Composite(vec![DbKey::String(tok.clone()), max_pk.clone()]);
                    let index_entries =
                        index_engine.filtered_scan(&start_bound, &end_bound, |_, _| true)?;
                    let mut pos_map = HashMap::new();
                    for (idx_key, val) in index_entries {
                        if let DbKey::Composite(mut parts) = idx_key
                            && let Some(pk) = parts.pop()
                            && let DbValue::Bytes(bytes) = val
                            && let Ok(positions) = bincode::deserialize::<Vec<u32>>(&bytes)
                        {
                            pos_map.insert(pk, positions);
                        }
                    }
                    token_pos_maps.push(pos_map);
                }

                let mut matched_pks = HashSet::new();
                if let Some(first_map) = token_pos_maps.first() {
                    let mut candidate_pks: HashSet<DbKey> = first_map.keys().cloned().collect();
                    for map in token_pos_maps.iter().skip(1) {
                        candidate_pks.retain(|pk| map.contains_key(pk));
                    }

                    for pk in candidate_pks {
                        let first_positions = &token_pos_maps[0][&pk];
                        let mut phrase_matched = false;
                        for &start_pos in first_positions {
                            let mut contiguous = true;
                            for (i, map) in token_pos_maps.iter().enumerate().skip(1) {
                                let expected_pos = start_pos + i as u32;
                                if let Some(positions) = map.get(&pk) {
                                    if !positions.contains(&expected_pos) {
                                        contiguous = false;
                                        break;
                                    }
                                } else {
                                    contiguous = false;
                                    break;
                                }
                            }
                            if contiguous {
                                phrase_matched = true;
                                break;
                            }
                        }
                        if phrase_matched {
                            matched_pks.insert(pk);
                        }
                    }
                }
                Ok(matched_pks)
            }
        }
    }

    fn eval_row_fts_query(
        &self,
        query: &crate::fts_parser::FullTextQuery,
        row: &Row,
        col_idx: usize,
        tokenizer: &dyn crate::tokenizer::Tokenizer,
    ) -> bool {
        match query {
            crate::fts_parser::FullTextQuery::Token(tok) => {
                if let Some(DbValue::String(s)) = row.get_by_index(col_idx) {
                    let tokens = tokenizer.tokenize(s.as_str());
                    tokens.contains(tok)
                } else {
                    false
                }
            }
            crate::fts_parser::FullTextQuery::And(left, right) => {
                self.eval_row_fts_query(left, row, col_idx, tokenizer)
                    && self.eval_row_fts_query(right, row, col_idx, tokenizer)
            }
            crate::fts_parser::FullTextQuery::Or(left, right) => {
                self.eval_row_fts_query(left, row, col_idx, tokenizer)
                    || self.eval_row_fts_query(right, row, col_idx, tokenizer)
            }
            crate::fts_parser::FullTextQuery::Phrase(phrase_tokens) => {
                if let Some(DbValue::String(s)) = row.get_by_index(col_idx) {
                    let tokens = tokenizer.tokenize(s.as_str());
                    if phrase_tokens.is_empty() {
                        return true;
                    }
                    if tokens.len() < phrase_tokens.len() {
                        return false;
                    }
                    for i in 0..=(tokens.len() - phrase_tokens.len()) {
                        let sub_slice = &tokens[i..(i + phrase_tokens.len())];
                        if sub_slice == phrase_tokens {
                            return true;
                        }
                    }
                    false
                } else {
                    false
                }
            }
        }
    }

    /// Commits all buffered mutations in this transaction to the tables.
    pub fn commit(&self) -> Result<()> {
        let mut buffer = self.write_buffer.lock().unwrap();

        // 1. Group all buffered mutations by table as WalEntry batches and TableWriteEntry batches.
        let mut table_batches = HashMap::new();
        let mut table_write_entries = HashMap::new();
        for (table_name, table_buffer) in buffer.iter() {
            let mut entries = Vec::new();
            let mut write_entries = Vec::new();
            for (key, val) in table_buffer {
                match val {
                    Some(row) => {
                        let bytes = row.to_bytes()?;
                        let entry = WalEntry::Put {
                            key: key.clone(),
                            value: DbValue::Bytes(bytes),
                        };
                        entries.push(entry.clone());
                        write_entries.push(crate::database::TableWriteEntry {
                            entry,
                            row: Some(row.clone()),
                        });
                    }
                    None => {
                        let entry = WalEntry::Delete { key: key.clone() };
                        entries.push(entry.clone());
                        write_entries.push(crate::database::TableWriteEntry { entry, row: None });
                    }
                }
            }
            if !entries.is_empty() {
                table_batches.insert(table_name.clone(), entries);
                table_write_entries.insert(table_name.clone(), write_entries);
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
        for (table_name, entries) in table_write_entries {
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
    local_read_keys: HashSet<DbKey>,
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
            local_read_keys: HashSet::new(),
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
                        self.local_read_keys.insert(k.clone());
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
                        self.local_read_keys.insert(k.clone());
                    }

                    return Ok(Some(row));
                }
            }
        }
    }
}

impl Drop for TransactionScanIterator {
    fn drop(&mut self) {
        if !self.local_read_keys.is_empty() {
            let mut read_set = self.read_set.lock().unwrap();
            let entry = read_set.entry(self.table_name.clone()).or_default();
            for key in self.local_read_keys.drain() {
                entry.insert(key);
            }
        }
    }
}
