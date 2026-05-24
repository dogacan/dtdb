use crate::error::{RelationalError, Result};
use crate::row::Row;
use dtdb_storage::{DbKey, DbValue};
use serde::{Deserialize, Serialize};
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
}

/// Schema defines the set of columns and types of a relational table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    /// Creates a new Schema.
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
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
