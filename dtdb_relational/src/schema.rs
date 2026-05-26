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

/// IndexDefinition represents a secondary index configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
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

impl Schema {
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

    /// Loads a schema from a file at the given path.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        let schema: Schema = bincode::deserialize(&bytes)?;
        Ok(schema)
    }
}
