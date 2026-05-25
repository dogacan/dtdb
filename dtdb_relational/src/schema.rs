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
        }
    }
}

/// Schema defines the set of columns and types of a relational table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
    #[serde(default)]
    pub locality_group_options: HashMap<String, LocalityGroupOptions>,
}

impl Schema {
    /// Creates a new Schema.
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            locality_group_options: HashMap::new(),
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
    pub fn merge_rows(&self, group_rows: &HashMap<String, Option<Row>>) -> Row {
        let mut full_values = vec![DbValue::Null; self.columns.len()];
        for (col_idx, col) in self.columns.iter().enumerate() {
            let group = col.locality_group.as_deref().unwrap_or("");
            if let Some(Some(sub_row)) = group_rows.get(group) {
                // Find the relative index of this column within the sub-row of the group.
                let relative_idx = self
                    .columns
                    .iter()
                    .filter(|c| c.locality_group.as_deref().unwrap_or("") == group)
                    .position(|c| c.name == col.name);
                if let Some(r_idx) = relative_idx
                    && let Some(val) = sub_row.get_by_index(r_idx)
                {
                    full_values[col_idx] = val.clone();
                }
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

    /// Validates that a DbKey matches the primary key column defined in the Schema.
    /// Also checks that the key matches the primary key column value in the row.
    pub fn validate_key(&self, key: &DbKey, row: &Row) -> Result<()> {
        let pk_index = self.primary_key_index().ok_or_else(|| {
            RelationalError::SchemaMismatch("Schema does not define a primary key".to_string())
        })?;
        let pk_column = &self.columns[pk_index];
        let pk_value = &row.values[pk_index];

        // 1. Verify that key matches column's DataType.
        match (pk_column.data_type, key) {
            (DataType::Int, DbKey::Int(_)) => {}
            (DataType::String, DbKey::String(_)) => {}
            (expected, actual_key) => {
                return Err(RelationalError::SchemaMismatch(format!(
                    "Primary key column '{}' expects {:?}, but key is {:?}",
                    pk_column.name, expected, actual_key
                )));
            }
        }

        // 2. Verify key matches the value inside the row's primary key column.
        match (key, pk_value) {
            (DbKey::Int(k), DbValue::Int(v)) if k == v => {}
            (DbKey::String(k), DbValue::String(v)) if k == v => {}
            (k, v) => {
                return Err(RelationalError::SchemaMismatch(format!(
                    "Key mismatch: primary key value in Row is {:?}, but passed key is {:?}",
                    v, k
                )));
            }
        }

        Ok(())
    }

    /// Returns the index of the primary key column.
    pub fn primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|col| col.is_primary_key)
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
