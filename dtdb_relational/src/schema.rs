use crate::error::{RelationalError, Result};
use crate::row::Row;
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// DataType represents the supported SQL data types in DuctTapeDB.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    String,
    Bytes,
    Null,
    Bool,
}

fn default_nullable() -> bool {
    true
}

/// Column represents a schema definition for a single table column.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    #[serde(default = "default_nullable")]
    pub is_nullable: bool,
    #[serde(default)]
    pub locality_group: Option<String>,
    #[serde(default)]
    pub default_value: Option<DbValue>,
    #[serde(default)]
    pub is_auto_increment: bool,
}

/// LocalityGroupOptions represents overridden storage configurations for a locality group.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct LocalityGroupOptions {
    pub compression: Option<CompressionType>,
    pub memtable_size_limit: Option<usize>,
    pub block_size_limit: Option<usize>,
    pub wal_size_limit: Option<usize>,
    pub l0_compaction_threshold: Option<usize>,
    pub sstable_target_size: Option<usize>,
    pub base_level_size_limit: Option<usize>,
    pub level_size_multiplier: Option<usize>,
    pub max_level: Option<usize>,
    pub block_cache_capacity: Option<usize>,
    #[serde(default)]
    pub wal_sync_interval_ms: Option<Option<u64>>,
}

impl LocalityGroupOptions {
    /// Applies these options on top of default EngineOptions.
    pub fn apply_to(&self, defaults: EngineOptions) -> EngineOptions {
        EngineOptions {
            compression: self.compression.unwrap_or(defaults.compression),
            memtable_size_limit: self
                .memtable_size_limit
                .unwrap_or(defaults.memtable_size_limit),
            block_size_limit: self.block_size_limit.unwrap_or(defaults.block_size_limit),
            wal_size_limit: self.wal_size_limit.unwrap_or(defaults.wal_size_limit),
            l0_compaction_threshold: self
                .l0_compaction_threshold
                .unwrap_or(defaults.l0_compaction_threshold),
            sstable_target_size: self
                .sstable_target_size
                .unwrap_or(defaults.sstable_target_size),
            base_level_size_limit: self
                .base_level_size_limit
                .unwrap_or(defaults.base_level_size_limit),
            level_size_multiplier: self
                .level_size_multiplier
                .unwrap_or(defaults.level_size_multiplier),
            max_level: self.max_level.unwrap_or(defaults.max_level),
            block_cache_capacity: self
                .block_cache_capacity
                .unwrap_or(defaults.block_cache_capacity),
            wal_sync_interval_ms: self
                .wal_sync_interval_ms
                .unwrap_or(defaults.wal_sync_interval_ms),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    FullText,
}

fn default_index_type() -> IndexType {
    IndexType::BTree
}

/// IndexDefinition represents a secondary index configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default = "default_index_type")]
    pub index_type: IndexType,
    #[serde(default)]
    pub tokenizer: Option<String>,
}

/// Schema defines the set of columns and types of a relational table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
    #[serde(default)]
    pub locality_group_options: HashMap<String, LocalityGroupOptions>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    #[serde(skip, default)]
    pub relative_indices: std::sync::OnceLock<Vec<(String, usize)>>,
}

impl Column {
    /// Returns true if this column matches `query_name` under SQL-style
    /// table-qualified lookup rules:
    ///   - exact match on `col.name`
    ///   - if `query_name` is qualified (e.g. `users.id`), match its trailing
    ///     portion against an unqualified `col.name`
    ///   - if `col.name` is qualified, match its trailing portion against an
    ///     unqualified `query_name`
    ///
    /// This is the canonical comparison the SQL layer uses for binding
    /// column references; centralizing it here keeps every call site
    /// consistent and makes it the single place to extend later (e.g. for
    /// schema-qualified `schema.table.col` names).
    pub fn matches_name(&self, query_name: &str) -> bool {
        column_names_match(&self.name, query_name)
    }
}

/// Returns true if two column names refer to the same column under SQL-style
/// table-qualified matching. The relation is symmetric: either name may be
/// qualified (e.g. `users.id`) and matches the other's trailing portion.
///
/// This is the canonical name comparison shared by the relational layer and
/// the SQL layer above it, so qualified-lookup rules live in exactly one place.
pub fn column_names_match(a: &str, b: &str) -> bool {
    a == b || ends_with_dot_suffix(a, b) || ends_with_dot_suffix(b, a)
}

impl Schema {
    /// Returns true if any column in this schema matches `query_name` under
    /// the same qualified-lookup rules as `Column::matches_name`. Useful as
    /// a presence check; for actual binding the caller should iterate
    /// `columns` and collect indices so it can detect ambiguity.
    pub fn matches_column(&self, query_name: &str) -> bool {
        self.columns.iter().any(|c| c.matches_name(query_name))
    }

    /// Creates a new Schema.
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            locality_group_options: HashMap::new(),
            indexes: Vec::new(),
            relative_indices: std::sync::OnceLock::new(),
        }
    }

    /// Creates a new Schema with options.
    pub fn new_with_options(
        columns: Vec<Column>,
        locality_group_options: HashMap<String, LocalityGroupOptions>,
    ) -> Self {
        Self {
            columns,
            locality_group_options,
            indexes: Vec::new(),
            relative_indices: std::sync::OnceLock::new(),
        }
    }

    /// Returns the set of all unique locality group names in the table schema.
    /// Columns without an explicit locality group are mapped to the default group `""`.
    pub fn locality_groups(&self) -> HashSet<String> {
        let mut groups = HashSet::new();
        for col in &self.columns {
            let group = col.locality_group.as_deref().unwrap_or("");
            groups.insert(group.to_string());
        }
        if groups.is_empty() {
            groups.insert("".to_string());
        }
        groups
    }

    /// Splits a full row into a sub-row containing only the columns belonging to the specified group.
    pub fn split_row(&self, row: &Row, group: &str) -> Row {
        let mut sub_values = Vec::new();
        for col in &self.columns {
            let col_group = col.locality_group.as_deref().unwrap_or("");
            if col_group == group
                && let Some(idx) = self.column_index(&col.name)
            {
                sub_values.push(row.values[idx].clone());
            }
        }
        Row::new(sub_values)
    }

    /// Merges sub-rows from various locality groups back into a single full row.
    /// Columns in groups that are missing/None in the input map will be populated with DbValue::Null.
    /// Returns the cached relative indices mapping.
    pub fn get_relative_indices(&self) -> &[(String, usize)] {
        self.relative_indices.get_or_init(|| {
            let mut mapping = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let group = col.locality_group.as_deref().unwrap_or("").to_string();
                let relative_idx = self
                    .columns
                    .iter()
                    .filter(|c| c.locality_group.as_deref().unwrap_or("") == group)
                    .position(|c| c.name == col.name)
                    .unwrap_or(0);
                mapping.push((group, relative_idx));
            }
            mapping
        })
    }

    /// Merges sub-rows from various locality groups back into a single full row.
    /// Columns in groups that are missing/None in the input map will be populated with DbValue::Null.
    pub fn merge_rows(&self, group_rows: &HashMap<String, Option<Row>>) -> Row {
        let mut full_values = vec![DbValue::Null; self.columns.len()];
        let mapping = self.get_relative_indices();
        for (col_idx, (group, r_idx)) in mapping.iter().enumerate() {
            if let Some(Some(sub_row)) = group_rows.get(group)
                && let Some(val) = sub_row.get_by_index(*r_idx)
            {
                full_values[col_idx] = val.clone();
            }
        }
        Row::new(full_values)
    }

    /// Validates a Row against this Schema.
    ///
    /// Verifies that:
    /// 1. The number of values in the row matches the number of columns.
    /// 2. The type of each value matches the column type definition.
    pub fn validate_row(&self, row: &Row) -> Result<()> {
        if row.values.len() != self.columns.len() {
            return Err(RelationalError::SchemaMismatch(format!(
                "Row has {} columns, but schema expects {}",
                row.values.len(),
                self.columns.len()
            )));
        }

        for (idx, (col, val)) in self.columns.iter().zip(row.values.iter()).enumerate() {
            match (col.data_type, val) {
                (_, DbValue::Null) => {
                    if !col.is_nullable {
                        return Err(RelationalError::SchemaMismatch(format!(
                            "Column '{}' (index {}) is not nullable, but got NULL",
                            col.name, idx
                        )));
                    }
                }
                (DataType::Int, DbValue::Int(_)) => {}
                (DataType::Float, DbValue::Float(_)) => {}
                (DataType::String, DbValue::String(_)) => {}
                (DataType::Bytes, DbValue::Bytes(_)) => {}
                (DataType::Bool, DbValue::Bool(_)) => {}
                (expected, actual) => {
                    return Err(RelationalError::SchemaMismatch(format!(
                        "Column '{}' (index {}) expects type {:?}, but got value {:?}",
                        col.name, idx, expected, actual
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validates that a DbKey matches the primary key column(s) defined in the Schema.
    /// Also checks that the key matches the primary key column value(s) in the row.
    pub fn validate_key(&self, key: &DbKey, row: &Row) -> Result<()> {
        let indices = self.primary_key_indices();
        if indices.is_empty() {
            return Err(RelationalError::SchemaMismatch(
                "Schema does not define a primary key".to_string(),
            ));
        }

        let validate_single = |col_idx: usize, k: &DbKey, val: &DbValue| -> Result<()> {
            let col = &self.columns[col_idx];
            // 1. Verify key type matches column type
            match (col.data_type, k) {
                (DataType::Int, DbKey::Int(_)) => {}
                (DataType::String, DbKey::String(_)) => {}
                (DataType::Bool, DbKey::Bool(_)) => {}
                (expected, actual_key) => {
                    return Err(RelationalError::SchemaMismatch(format!(
                        "Primary key column '{}' expects {:?}, but key is {:?}",
                        col.name, expected, actual_key
                    )));
                }
            }
            // 2. Verify key matches value in row
            match (k, val) {
                (DbKey::Int(kv), DbValue::Int(vv)) if kv == vv => {}
                (DbKey::String(kv), DbValue::String(vv)) if kv == vv => {}
                (DbKey::Bool(kv), DbValue::Bool(vv)) if kv == vv => {}
                (kv, vv) => {
                    return Err(RelationalError::SchemaMismatch(format!(
                        "Key mismatch: primary key value in Row is {:?}, but passed key is {:?}",
                        vv, kv
                    )));
                }
            }
            Ok(())
        };

        if indices.len() == 1 {
            let pk_idx = indices[0];
            let pk_value = &row.values[pk_idx];
            validate_single(pk_idx, key, pk_value)?;
        } else {
            match key {
                DbKey::Composite(parts) => {
                    if parts.len() != indices.len() {
                        return Err(RelationalError::SchemaMismatch(format!(
                            "Composite key has {} parts, but schema expects {}",
                            parts.len(),
                            indices.len()
                        )));
                    }
                    for (i, part) in parts.iter().enumerate() {
                        let pk_idx = indices[i];
                        let pk_value = &row.values[pk_idx];
                        validate_single(pk_idx, part, pk_value)?;
                    }
                }
                _ => {
                    return Err(RelationalError::SchemaMismatch(format!(
                        "Expected Composite key for multi-column primary key, got {:?}",
                        key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validates a DbKey structure and type against the primary key columns definition only.
    pub fn validate_key_only(&self, key: &DbKey) -> Result<()> {
        let indices = self.primary_key_indices();
        if indices.is_empty() {
            return Err(RelationalError::SchemaMismatch(
                "Schema does not define a primary key".to_string(),
            ));
        }

        let validate_single = |col_idx: usize, k: &DbKey| -> Result<()> {
            let col = &self.columns[col_idx];
            match (col.data_type, k) {
                (DataType::Int, DbKey::Int(_)) => Ok(()),
                (DataType::String, DbKey::String(_)) => Ok(()),
                (DataType::Bool, DbKey::Bool(_)) => Ok(()),
                (expected, actual_key) => Err(RelationalError::SchemaMismatch(format!(
                    "Primary key column '{}' expects {:?}, but key is {:?}",
                    col.name, expected, actual_key
                ))),
            }
        };

        if indices.len() == 1 {
            validate_single(indices[0], key)?;
        } else {
            match key {
                DbKey::Composite(parts) => {
                    if parts.len() != indices.len() {
                        return Err(RelationalError::SchemaMismatch(format!(
                            "Composite key has {} parts, but schema expects {}",
                            parts.len(),
                            indices.len()
                        )));
                    }
                    for (i, part) in parts.iter().enumerate() {
                        validate_single(indices[i], part)?;
                    }
                }
                _ => {
                    return Err(RelationalError::SchemaMismatch(format!(
                        "Expected Composite key for multi-column primary key, got {:?}",
                        key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Returns the indices of the primary key columns.
    pub fn primary_key_indices(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_primary_key)
            .map(|(idx, _)| idx)
            .collect()
    }

    /// Extract the primary key (single or composite) from a Row.
    pub fn extract_primary_key(&self, row: &Row) -> Result<DbKey> {
        let indices = self.primary_key_indices();
        if indices.is_empty() {
            return Err(RelationalError::SchemaMismatch(
                "Schema does not define a primary key".to_string(),
            ));
        }
        if indices.len() == 1 {
            let val = &row.values[indices[0]];
            match val {
                DbValue::Int(v) => Ok(DbKey::Int(*v)),
                DbValue::String(s) => Ok(DbKey::String(s.clone())),
                DbValue::Bool(b) => Ok(DbKey::Bool(*b)),
                DbValue::Null => Err(RelationalError::SchemaMismatch(
                    "Primary key cannot be NULL".to_string(),
                )),
                other => Err(RelationalError::SchemaMismatch(format!(
                    "Unsupported primary key type: {:?}",
                    other
                ))),
            }
        } else {
            let mut keys = Vec::new();
            for &idx in &indices {
                let val = &row.values[idx];
                let k = match val {
                    DbValue::Int(v) => DbKey::Int(*v),
                    DbValue::String(s) => DbKey::String(s.clone()),
                    DbValue::Bool(b) => DbKey::Bool(*b),
                    DbValue::Null => {
                        return Err(RelationalError::SchemaMismatch(
                            "Primary key cannot be NULL".to_string(),
                        ));
                    }
                    other => {
                        return Err(RelationalError::SchemaMismatch(format!(
                            "Unsupported primary key type in composite: {:?}",
                            other
                        )));
                    }
                };
                keys.push(k);
            }
            Ok(DbKey::Composite(keys))
        }
    }

    /// Get default ranges/bounds for primary key scan.
    pub fn primary_key_bounds(&self) -> Result<(DbKey, DbKey)> {
        let indices = self.primary_key_indices();
        if indices.is_empty() {
            return Err(RelationalError::SchemaMismatch(
                "Schema does not define a primary key".to_string(),
            ));
        }

        let get_bounds = |dt: DataType| match dt {
            DataType::Int => (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX)),
            DataType::Bool => (DbKey::Bool(false), DbKey::Bool(true)),
            _ => (
                DbKey::String("".to_string()),
                DbKey::String("\u{10ffff}".to_string()),
            ),
        };

        if indices.len() == 1 {
            let col = &self.columns[indices[0]];
            Ok(get_bounds(col.data_type))
        } else {
            let mut mins = Vec::new();
            let mut maxs = Vec::new();
            for &idx in &indices {
                let col = &self.columns[idx];
                let (min_val, max_val) = get_bounds(col.data_type);
                mins.push(min_val);
                maxs.push(max_val);
            }
            Ok((DbKey::Composite(mins), DbKey::Composite(maxs)))
        }
    }

    /// Returns the index of the primary key column if there is exactly one.
    pub fn primary_key_index(&self) -> Option<usize> {
        let pks = self.primary_key_indices();
        if pks.len() == 1 { Some(pks[0]) } else { None }
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    /// Saves the schema to a file at the given path.
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let bytes = bincode::serialize(self)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }

    /// Atomically replaces the on-disk schema file via temp-write + rename.
    ///
    /// Use this rather than `save_to_file` for in-place schema updates
    /// (e.g. adding an index) where a partial write or a crash mid-update
    /// must not leave the schema file truncated or pointing at an index
    /// whose data hasn't been written yet.
    pub fn save_to_file_atomic(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let bytes = bincode::serialize(self)?;
        let tmp_path = match path.file_name() {
            Some(name) => {
                let mut tmp_name = std::ffi::OsString::from(".");
                tmp_name.push(name);
                tmp_name.push(".tmp");
                path.with_file_name(tmp_name)
            }
            None => path.with_extension("tmp"),
        };
        std::fs::write(&tmp_path, bytes)?;
        std::fs::rename(&tmp_path, path)?;
        if let Err(e) = dtdb_storage::fsync_parent_dir(path) {
            // Mirror the storage layer's tolerance for filesystems that don't
            // support directory fsync (the rename itself is durable on POSIX).
            return Err(RelationalError::Storage(e));
        }
        Ok(())
    }

    /// Loads a schema from a file at the given path.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        let schema: Schema = bincode::deserialize(&bytes)?;
        Ok(schema)
    }
}

/// Helper to check if a string ends with a suffix preceded by a dot `.`,
/// avoiding heap allocation from `format!(".{}", suffix)`.
///
/// Crate-internal: callers outside `dtdb_relational` should use the symmetric
/// [`column_names_match`] instead of reaching for this low-level primitive.
pub(crate) fn ends_with_dot_suffix(full: &str, suffix: &str) -> bool {
    full.len() > suffix.len()
        && full.ends_with(suffix)
        && full.as_bytes()[full.len() - suffix.len() - 1] == b'.'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, dt: DataType, pk: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type: dt,
            is_primary_key: pk,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }
    }

    fn col_group(name: &str, dt: DataType, pk: bool, group: &str) -> Column {
        let mut c = col(name, dt, pk);
        c.locality_group = Some(group.to_string());
        c
    }

    // ----- name matching -----

    #[test]
    fn ends_with_dot_suffix_rules() {
        assert!(ends_with_dot_suffix("users.id", "id"));
        // The match must be preceded by a literal dot.
        assert!(!ends_with_dot_suffix("usersid", "id"));
        // Equal-length strings are not a dot-suffix match (need a preceding dot).
        assert!(!ends_with_dot_suffix("id", "id"));
        // Suffix not at the end.
        assert!(!ends_with_dot_suffix("id.users", "id"));
        // Char before suffix is not a dot.
        assert!(!ends_with_dot_suffix("user_id", "id"));
    }

    #[test]
    fn column_names_match_is_symmetric_and_qualified() {
        assert!(column_names_match("id", "id"));
        assert!(column_names_match("users.id", "id"));
        assert!(column_names_match("id", "users.id"));
        assert!(column_names_match("users.id", "users.id"));
        assert!(!column_names_match("users.id", "orders.id"));
        assert!(!column_names_match("id", "name"));
    }

    #[test]
    fn column_matches_name_and_schema_lookup() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col("name", DataType::String, false),
        ]);
        assert!(schema.columns[0].matches_name("users.id"));
        assert!(schema.matches_column("name"));
        assert!(schema.matches_column("users.name"));
        assert!(!schema.matches_column("missing"));
    }

    #[test]
    fn column_index_lookup() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col("name", DataType::String, false),
        ]);
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("nope"), None);
    }

    // ----- locality groups & row splitting/merging -----

    #[test]
    fn locality_groups_default_and_explicit() {
        // No explicit groups -> single default "" group.
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        let groups = schema.locality_groups();
        assert_eq!(groups.len(), 1);
        assert!(groups.contains(""));

        // Mixed explicit + implicit groups.
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col_group("bio", DataType::String, false, "detail"),
        ]);
        let groups = schema.locality_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(""));
        assert!(groups.contains("detail"));
    }

    #[test]
    fn split_and_merge_rows_roundtrip() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col_group("bio", DataType::String, false, "detail"),
            col("name", DataType::String, false),
        ]);
        let row = Row::new(vec![
            DbValue::Int(1),
            DbValue::String("hello".to_string()),
            DbValue::String("alice".to_string()),
        ]);

        // Default group holds id + name; "detail" group holds bio.
        let default_sub = schema.split_row(&row, "");
        assert_eq!(
            default_sub.values,
            vec![DbValue::Int(1), DbValue::String("alice".to_string())]
        );
        let detail_sub = schema.split_row(&row, "detail");
        assert_eq!(
            detail_sub.values,
            vec![DbValue::String("hello".to_string())]
        );

        // Merging the sub-rows back yields the original full row.
        let mut group_rows = HashMap::new();
        group_rows.insert("".to_string(), Some(default_sub));
        group_rows.insert("detail".to_string(), Some(detail_sub));
        let merged = schema.merge_rows(&group_rows);
        assert_eq!(merged, row);
    }

    #[test]
    fn merge_rows_missing_group_fills_nulls() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col_group("bio", DataType::String, false, "detail"),
        ]);
        let mut group_rows = HashMap::new();
        group_rows.insert("".to_string(), Some(Row::new(vec![DbValue::Int(7)])));
        // "detail" group intentionally absent.
        let merged = schema.merge_rows(&group_rows);
        assert_eq!(merged.values, vec![DbValue::Int(7), DbValue::Null]);
    }

    #[test]
    fn get_relative_indices_maps_columns_within_group() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col_group("bio", DataType::String, false, "detail"),
            col("name", DataType::String, false),
        ]);
        let mapping = schema.get_relative_indices();
        // id is index 0 within default group; name is index 1 within default group;
        // bio is index 0 within its own "detail" group.
        assert_eq!(mapping[0], ("".to_string(), 0));
        assert_eq!(mapping[1], ("detail".to_string(), 0));
        assert_eq!(mapping[2], ("".to_string(), 1));
        // Repeated calls return the cached slice.
        let mapping2 = schema.get_relative_indices();
        assert_eq!(mapping, mapping2);
    }

    // ----- row validation -----

    #[test]
    fn validate_row_accepts_matching_types() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col("f", DataType::Float, false),
            col("s", DataType::String, false),
            col("b", DataType::Bytes, false),
            col("flag", DataType::Bool, false),
        ]);
        let row = Row::new(vec![
            DbValue::Int(1),
            DbValue::Float(1.5),
            DbValue::String("x".to_string()),
            DbValue::Bytes(vec![1, 2]),
            DbValue::Bool(true),
        ]);
        assert!(schema.validate_row(&row).is_ok());
    }

    #[test]
    fn validate_row_rejects_wrong_arity() {
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        let row = Row::new(vec![DbValue::Int(1), DbValue::Int(2)]);
        let err = schema.validate_row(&row).unwrap_err();
        assert!(matches!(err, RelationalError::SchemaMismatch(_)));
    }

    #[test]
    fn validate_row_rejects_type_mismatch() {
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        let row = Row::new(vec![DbValue::String("nope".to_string())]);
        assert!(schema.validate_row(&row).is_err());
    }

    #[test]
    fn validate_row_null_respects_nullability() {
        let mut c = col("id", DataType::Int, true);
        c.is_nullable = false;
        let schema = Schema::new(vec![c]);
        // NULL into a non-nullable column is rejected.
        assert!(schema.validate_row(&Row::new(vec![DbValue::Null])).is_err());

        // Nullable column accepts NULL.
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        assert!(schema.validate_row(&Row::new(vec![DbValue::Null])).is_ok());
    }

    // ----- primary key helpers -----

    #[test]
    fn primary_key_indices_and_single_index() {
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col("name", DataType::String, false),
        ]);
        assert_eq!(schema.primary_key_indices(), vec![0]);
        assert_eq!(schema.primary_key_index(), Some(0));

        let composite = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        assert_eq!(composite.primary_key_indices(), vec![0, 1]);
        // More than one PK column -> no single index.
        assert_eq!(composite.primary_key_index(), None);
    }

    #[test]
    fn extract_primary_key_single() {
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        let row = Row::new(vec![DbValue::Int(42)]);
        assert_eq!(schema.extract_primary_key(&row).unwrap(), DbKey::Int(42));

        let schema = Schema::new(vec![col("id", DataType::String, true)]);
        let row = Row::new(vec![DbValue::String("k".to_string())]);
        assert_eq!(
            schema.extract_primary_key(&row).unwrap(),
            DbKey::String("k".to_string())
        );

        let schema = Schema::new(vec![col("id", DataType::Bool, true)]);
        let row = Row::new(vec![DbValue::Bool(true)]);
        assert_eq!(schema.extract_primary_key(&row).unwrap(), DbKey::Bool(true));
    }

    #[test]
    fn extract_primary_key_composite() {
        let schema = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        let row = Row::new(vec![DbValue::Int(1), DbValue::String("x".to_string())]);
        assert_eq!(
            schema.extract_primary_key(&row).unwrap(),
            DbKey::Composite(vec![DbKey::Int(1), DbKey::String("x".to_string())])
        );
    }

    #[test]
    fn extract_primary_key_errors() {
        // No primary key defined.
        let schema = Schema::new(vec![col("name", DataType::String, false)]);
        assert!(
            schema
                .extract_primary_key(&Row::new(vec![DbValue::String("a".to_string())]))
                .is_err()
        );

        // NULL primary key (single).
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        assert!(
            schema
                .extract_primary_key(&Row::new(vec![DbValue::Null]))
                .is_err()
        );

        // Unsupported single primary key type (Float).
        let schema = Schema::new(vec![col("id", DataType::Float, true)]);
        assert!(
            schema
                .extract_primary_key(&Row::new(vec![DbValue::Float(1.0)]))
                .is_err()
        );

        // NULL within a composite primary key.
        let schema = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        assert!(
            schema
                .extract_primary_key(&Row::new(vec![DbValue::Int(1), DbValue::Null]))
                .is_err()
        );

        // Unsupported type within a composite primary key.
        let schema = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::Float, true),
        ]);
        assert!(
            schema
                .extract_primary_key(&Row::new(vec![DbValue::Int(1), DbValue::Float(2.0)]))
                .is_err()
        );
    }

    #[test]
    fn primary_key_bounds_single_and_composite() {
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        assert_eq!(
            schema.primary_key_bounds().unwrap(),
            (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX))
        );

        let schema = Schema::new(vec![col("id", DataType::Bool, true)]);
        assert_eq!(
            schema.primary_key_bounds().unwrap(),
            (DbKey::Bool(false), DbKey::Bool(true))
        );

        let schema = Schema::new(vec![col("id", DataType::String, true)]);
        let (lo, hi) = schema.primary_key_bounds().unwrap();
        assert_eq!(lo, DbKey::String("".to_string()));
        assert_eq!(hi, DbKey::String("\u{10ffff}".to_string()));

        let composite = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        let (lo, hi) = composite.primary_key_bounds().unwrap();
        assert_eq!(
            lo,
            DbKey::Composite(vec![DbKey::Int(i64::MIN), DbKey::String("".to_string())])
        );
        assert_eq!(
            hi,
            DbKey::Composite(vec![
                DbKey::Int(i64::MAX),
                DbKey::String("\u{10ffff}".to_string())
            ])
        );

        // No primary key -> error.
        let schema = Schema::new(vec![col("name", DataType::String, false)]);
        assert!(schema.primary_key_bounds().is_err());
    }

    // ----- key validation -----

    #[test]
    fn validate_key_single_ok_and_errors() {
        let schema = Schema::new(vec![col("id", DataType::Int, true)]);
        let row = Row::new(vec![DbValue::Int(5)]);
        assert!(schema.validate_key(&DbKey::Int(5), &row).is_ok());

        // Key type does not match column type.
        assert!(
            schema
                .validate_key(&DbKey::String("5".to_string()), &row)
                .is_err()
        );
        // Key value does not match the row's primary key value.
        assert!(schema.validate_key(&DbKey::Int(6), &row).is_err());

        // No primary key.
        let no_pk = Schema::new(vec![col("name", DataType::String, false)]);
        assert!(
            no_pk
                .validate_key(
                    &DbKey::Int(1),
                    &Row::new(vec![DbValue::String("x".to_string())])
                )
                .is_err()
        );
    }

    #[test]
    fn validate_key_composite_ok_and_errors() {
        let schema = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        let row = Row::new(vec![DbValue::Int(1), DbValue::String("x".to_string())]);
        let good = DbKey::Composite(vec![DbKey::Int(1), DbKey::String("x".to_string())]);
        assert!(schema.validate_key(&good, &row).is_ok());

        // Wrong number of parts.
        let short = DbKey::Composite(vec![DbKey::Int(1)]);
        assert!(schema.validate_key(&short, &row).is_err());

        // Non-composite key for a composite primary key.
        assert!(schema.validate_key(&DbKey::Int(1), &row).is_err());

        // Mismatched part value.
        let bad = DbKey::Composite(vec![DbKey::Int(1), DbKey::String("y".to_string())]);
        assert!(schema.validate_key(&bad, &row).is_err());
    }

    #[test]
    fn validate_key_only_variants() {
        let schema = Schema::new(vec![col("id", DataType::Bool, true)]);
        assert!(schema.validate_key_only(&DbKey::Bool(true)).is_ok());
        assert!(schema.validate_key_only(&DbKey::Int(1)).is_err());

        // No primary key.
        let no_pk = Schema::new(vec![col("name", DataType::String, false)]);
        assert!(no_pk.validate_key_only(&DbKey::Int(1)).is_err());

        // Composite.
        let composite = Schema::new(vec![
            col("a", DataType::Int, true),
            col("b", DataType::String, true),
        ]);
        let good = DbKey::Composite(vec![DbKey::Int(1), DbKey::String("x".to_string())]);
        assert!(composite.validate_key_only(&good).is_ok());
        assert!(
            composite
                .validate_key_only(&DbKey::Composite(vec![DbKey::Int(1)]))
                .is_err()
        );
        assert!(composite.validate_key_only(&DbKey::Int(1)).is_err());
        // Wrong type within composite.
        let bad = DbKey::Composite(vec![DbKey::Int(1), DbKey::Int(2)]);
        assert!(composite.validate_key_only(&bad).is_err());
    }

    // ----- locality group options -----

    fn base_engine_options() -> EngineOptions {
        EngineOptions {
            compression: CompressionType::Uncompressed,
            memtable_size_limit: 100,
            block_size_limit: 200,
            wal_size_limit: 300,
            l0_compaction_threshold: 4,
            sstable_target_size: 2048,
            base_level_size_limit: 4096,
            level_size_multiplier: 10,
            max_level: 7,
            block_cache_capacity: 1000,
            wal_sync_interval_ms: Some(50),
        }
    }

    #[test]
    fn locality_group_options_apply_overrides_and_defaults() {
        let defaults = base_engine_options();

        // Empty options leave defaults untouched.
        let empty = LocalityGroupOptions::default();
        assert_eq!(empty.apply_to(defaults), defaults);

        // Overriding a subset replaces only those fields.
        let opts = LocalityGroupOptions {
            compression: Some(CompressionType::Lz4),
            memtable_size_limit: Some(999),
            wal_sync_interval_ms: Some(None),
            ..Default::default()
        };
        let applied = opts.apply_to(defaults);
        assert_eq!(applied.compression, CompressionType::Lz4);
        assert_eq!(applied.memtable_size_limit, 999);
        assert_eq!(applied.wal_sync_interval_ms, None);
        // Untouched fields retain defaults.
        assert_eq!(applied.block_size_limit, defaults.block_size_limit);
        assert_eq!(applied.max_level, defaults.max_level);
    }

    // ----- serialization to/from disk -----

    #[test]
    fn save_and_load_schema_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.bin");
        let schema = Schema::new(vec![
            col("id", DataType::Int, true),
            col("name", DataType::String, false),
        ]);
        schema.save_to_file(&path).unwrap();
        let loaded = Schema::load_from_file(&path).unwrap();
        assert_eq!(schema, loaded);
    }

    #[test]
    fn save_to_file_atomic_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.bin");
        let schema = Schema::new_with_options(vec![col("id", DataType::Int, true)], HashMap::new());
        schema.save_to_file_atomic(&path).unwrap();
        assert!(path.exists());
        let loaded = Schema::load_from_file(&path).unwrap();
        assert_eq!(schema, loaded);

        // Atomic replace over an existing file leaves no stray temp file.
        schema.save_to_file_atomic(&path).unwrap();
        let tmp = dir.path().join(".schema.bin.tmp");
        assert!(!tmp.exists());
    }
}
