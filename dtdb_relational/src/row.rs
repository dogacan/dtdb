use crate::error::{RelationalError, Result};
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
        let bytes = postcard::to_allocvec(self)?;
        Ok(bytes)
    }

    /// Deserializes a row from its binary representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let row: Row = postcard::from_bytes(bytes)?;
        Ok(row)
    }

    /// Deserializes a row, parsing only the columns in `projection` and skipping the others.
    /// Non-projected columns are populated with `DbValue::Null`.
    ///
    /// This walks postcard's wire format directly so skipped columns are never
    /// allocated — their bytes are consumed but no `String`/`Bytes` value is
    /// built. It must stay in sync with how `DbValue` serializes under postcard:
    /// an enum is a varint discriminant followed by the variant payload, and a
    /// `Row`'s single `Vec` field is a varint length followed by the elements.
    pub fn from_bytes_projected(bytes: &[u8], projection: Option<&[usize]>) -> Result<Self> {
        let Some(projected) = projection else {
            return Self::from_bytes(bytes);
        };

        let mut cursor = bytes;
        let len = read_varint_u64(&mut cursor)? as usize;

        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            let pre_tag_cursor = cursor;
            let tag = read_varint_u64(&mut cursor)?;
            let keep = projected.contains(&i);

            match tag {
                // Int: zigzag-varint payload.
                0 => {
                    let val = read_varint_i64(&mut cursor)?;
                    values.push(if keep {
                        DbValue::Int(val)
                    } else {
                        DbValue::Null
                    });
                }
                // Float: 8 little-endian bytes.
                1 => {
                    let raw = take_bytes(&mut cursor, 8)?;
                    if keep {
                        let val = f64::from_le_bytes(raw.try_into().unwrap());
                        values.push(DbValue::Float(val));
                    } else {
                        values.push(DbValue::Null);
                    }
                }
                // String: varint length, then UTF-8 bytes.
                2 => {
                    let str_len = read_varint_u64(&mut cursor)? as usize;
                    let raw = take_bytes(&mut cursor, str_len)?;
                    if keep {
                        let s = std::str::from_utf8(raw)
                            .map_err(|e| corruption(format!("Invalid UTF-8: {}", e)))?;
                        values.push(DbValue::string(s));
                    } else {
                        values.push(DbValue::Null);
                    }
                }
                // Bytes: varint length, then raw bytes.
                3 => {
                    let bytes_len = read_varint_u64(&mut cursor)? as usize;
                    let raw = take_bytes(&mut cursor, bytes_len)?;
                    if keep {
                        values.push(DbValue::bytes(raw.to_vec()));
                    } else {
                        values.push(DbValue::Null);
                    }
                }
                // Bool: one byte.
                4 => {
                    let raw = take_bytes(&mut cursor, 1)?;
                    values.push(if keep {
                        DbValue::Bool(raw[0] != 0)
                    } else {
                        DbValue::Null
                    });
                }
                // Null: no payload.
                5 => values.push(DbValue::Null),
                // Date, Time, Timestamp, Decimal: deserialize directly using postcard
                6..=9 => {
                    let (val, remaining) = postcard::take_from_bytes::<DbValue>(pre_tag_cursor)
                        .map_err(|e| corruption(format!("Failed to deserialize DbValue: {}", e)))?;
                    cursor = remaining;
                    values.push(if keep { val } else { DbValue::Null });
                }
                other => {
                    return Err(corruption(format!(
                        "Unknown DbValue variant tag: {}",
                        other
                    )));
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

/// Builds a corruption error for a malformed row buffer.
fn corruption(msg: impl Into<String>) -> RelationalError {
    RelationalError::Storage(dtdb_storage::StorageError::Corruption(msg.into()))
}

/// Reads one postcard LEB128 varint (unsigned), advancing `cursor` past it.
fn read_varint_u64(cursor: &mut &[u8]) -> Result<u64> {
    let buf = *cursor;
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for i in 0..buf.len() {
        // A u64 varint is at most 10 bytes; anything longer is corruption.
        if i >= 10 {
            return Err(corruption("varint too long"));
        }
        let byte = buf[i];
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            *cursor = &buf[i + 1..];
            return Ok(value);
        }
        shift += 7;
    }
    Err(corruption("truncated varint"))
}

/// Reads one postcard zigzag-varint (signed), advancing `cursor` past it.
fn read_varint_i64(cursor: &mut &[u8]) -> Result<i64> {
    let z = read_varint_u64(cursor)?;
    // Inverse of postcard's zigzag mapping: even encodes >= 0, odd encodes < 0.
    Ok(((z >> 1) as i64) ^ -((z & 1) as i64))
}

/// Splits `n` bytes off the front of `cursor`, advancing it; errors if short.
fn take_bytes<'a>(cursor: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if cursor.len() < n {
        return Err(corruption("truncated row payload"));
    }
    let (head, tail) = cursor.split_at(n);
    *cursor = tail;
    Ok(head)
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

    /// Negative integers must survive the projected path: postcard encodes them
    /// with zigzag, so this guards the signed-varint decode specifically.
    #[test]
    fn test_from_bytes_projected_negative_ints() {
        let row = Row::new(vec![
            DbValue::Int(-1),
            DbValue::Int(i64::MIN),
            DbValue::Int(i64::MAX),
        ]);
        let bytes = row.to_bytes().unwrap();
        let parsed = Row::from_bytes_projected(&bytes, Some(&[0, 1, 2])).unwrap();
        assert_eq!(parsed, row);
    }
}
