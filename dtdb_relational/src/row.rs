use crate::error::Result;
use crate::schema::Schema;
use dtdb_storage::DbValue;
use serde::{Deserialize, Serialize};

/// Row represents a tuple/record in a relational table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Row {
    pub values: Vec<DbValue>,
}

impl Row {
    /// Creates a new Row.
    pub fn new(values: Vec<DbValue>) -> Self {
        Self { values }
    }

    /// Serializes the row into a compact binary format.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }

    /// Deserializes a row from its binary representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let row: Row = bincode::deserialize(bytes)?;
        Ok(row)
    }

    /// Gets a value from the row by index.
    pub fn get_by_index(&self, index: usize) -> Option<&DbValue> {
        self.values.get(index)
    }

    /// Gets a value from the row by column name, using the table schema.
    pub fn get_by_name(&self, schema: &Schema, name: &str) -> Option<&DbValue> {
        let idx = schema.column_index(name)?;
        self.get_by_index(idx)
    }
}
