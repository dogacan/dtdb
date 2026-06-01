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

    /// Deserializes a row, parsing only the columns in `projection` and skipping the others.
    /// Non-projected columns are populated with `DbValue::Null`.
    pub fn from_bytes_projected(bytes: &[u8], projection: Option<&[usize]>) -> Result<Self> {
        if projection.is_none() {
            return Self::from_bytes(bytes);
        }
        let projected = projection.unwrap();

        let mut cursor = bytes;
        if cursor.len() < 8 {
            return Err(crate::error::RelationalError::Storage(
                dtdb_storage::StorageError::Corruption("Row bytes too short".to_string()),
            ));
        }
        let len = u64::from_le_bytes(cursor[0..8].try_into().unwrap()) as usize;
        cursor = &cursor[8..];

        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            if cursor.len() < 4 {
                return Err(crate::error::RelationalError::Storage(
                    dtdb_storage::StorageError::Corruption("Truncated variant tag".to_string()),
                ));
            }
            let tag = u32::from_le_bytes(cursor[0..4].try_into().unwrap());
            cursor = &cursor[4..];

            let keep = projected.contains(&i);

            match tag {
                0 => {
                    // Int
                    if cursor.len() < 8 {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated Int payload".to_string(),
                            ),
                        ));
                    }
                    if keep {
                        let val = i64::from_le_bytes(cursor[0..8].try_into().unwrap());
                        values.push(DbValue::Int(val));
                    } else {
                        values.push(DbValue::Null);
                    }
                    cursor = &cursor[8..];
                }
                1 => {
                    // Float
                    if cursor.len() < 8 {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated Float payload".to_string(),
                            ),
                        ));
                    }
                    if keep {
                        let val = f64::from_le_bytes(cursor[0..8].try_into().unwrap());
                        values.push(DbValue::Float(val));
                    } else {
                        values.push(DbValue::Null);
                    }
                    cursor = &cursor[8..];
                }
                2 => {
                    // String
                    if cursor.len() < 8 {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated String length".to_string(),
                            ),
                        ));
                    }
                    let str_len = u64::from_le_bytes(cursor[0..8].try_into().unwrap()) as usize;
                    cursor = &cursor[8..];
                    if cursor.len() < str_len {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated String payload".to_string(),
                            ),
                        ));
                    }
                    if keep {
                        let s = String::from_utf8(cursor[0..str_len].to_vec()).map_err(|e| {
                            dtdb_storage::StorageError::Corruption(format!("Invalid UTF-8: {}", e))
                        })?;
                        values.push(DbValue::string(s));
                    } else {
                        values.push(DbValue::Null);
                    }
                    cursor = &cursor[str_len..];
                }
                3 => {
                    // Bytes
                    if cursor.len() < 8 {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated Bytes length".to_string(),
                            ),
                        ));
                    }
                    let bytes_len = u64::from_le_bytes(cursor[0..8].try_into().unwrap()) as usize;
                    cursor = &cursor[8..];
                    if cursor.len() < bytes_len {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated Bytes payload".to_string(),
                            ),
                        ));
                    }
                    if keep {
                        values.push(DbValue::bytes(cursor[0..bytes_len].to_vec()));
                    } else {
                        values.push(DbValue::Null);
                    }
                    cursor = &cursor[bytes_len..];
                }
                4 => {
                    // Bool
                    if cursor.is_empty() {
                        return Err(crate::error::RelationalError::Storage(
                            dtdb_storage::StorageError::Corruption(
                                "Truncated Bool payload".to_string(),
                            ),
                        ));
                    }
                    if keep {
                        let val = cursor[0] != 0;
                        values.push(DbValue::Bool(val));
                    } else {
                        values.push(DbValue::Null);
                    }
                    cursor = &cursor[1..];
                }
                5 => {
                    // Null
                    values.push(DbValue::Null);
                }
                other => {
                    return Err(crate::error::RelationalError::Storage(
                        dtdb_storage::StorageError::Corruption(format!(
                            "Unknown DbValue variant tag: {}",
                            other
                        )),
                    ));
                }
            }
        }

        Ok(Row { values })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes_projected() {
        let row = Row::new(vec![
            DbValue::Int(42),
            DbValue::string("hello"),
            DbValue::Null,
            DbValue::Bool(true),
            DbValue::bytes(vec![1, 2, 3]),
            DbValue::Float(1.5),
        ]);

        let bytes = row.to_bytes().unwrap();

        // 1. Projection = None (should match eager deserialize)
        let row_all = Row::from_bytes_projected(&bytes, None).unwrap();
        assert_eq!(row_all, row);

        // 2. Projection with all indices (should match eager deserialize)
        let row_all_proj = Row::from_bytes_projected(&bytes, Some(&[0, 1, 2, 3, 4, 5])).unwrap();
        assert_eq!(row_all_proj, row);

        // 3. Projection with subset (only keep index 0, 1, 4)
        let row_subset = Row::from_bytes_projected(&bytes, Some(&[0, 1, 4])).unwrap();
        assert_eq!(row_subset.values[0], DbValue::Int(42));
        assert_eq!(row_subset.values[1], DbValue::string("hello"));
        assert_eq!(row_subset.values[2], DbValue::Null); // Not kept
        assert_eq!(row_subset.values[3], DbValue::Null); // Not kept
        assert_eq!(row_subset.values[4], DbValue::bytes(vec![1, 2, 3]));
        assert_eq!(row_subset.values[5], DbValue::Null); // Not kept
    }
}
