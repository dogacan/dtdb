use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crate::{DbKey, DbValue, Result, StorageError};

/// Represents an operation entry in the Write-Ahead Log (WAL).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalEntry {
    Put { key: DbKey, value: DbValue },
    Delete { key: DbKey },
}

/// A Write-Ahead Log (WAL) provides durability by writing mutations to disk
/// before applying them to the in-memory MemTable.
///
/// If the database crashes, the log is replayed on startup to restore the MemTable.
pub struct Wal {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Wal {
    /// Opens an existing WAL file or creates a new one in append-only mode.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self { file, path })
    }

    /// Appends a `Put` operation to the log.
    pub fn append_put(&mut self, key: &DbKey, value: &DbValue) -> Result<()> {
        let entry = WalEntry::Put {
            key: key.clone(),
            value: value.clone(),
        };
        self.write_entry(&entry)
    }

    /// Appends a `Delete` operation to the log.
    pub fn append_delete(&mut self, key: &DbKey) -> Result<()> {
        let entry = WalEntry::Delete { key: key.clone() };
        self.write_entry(&entry)
    }

    /// Serializes and writes a `WalEntry` to the file, and forces it to disk.
    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        // Serialize using bincode, which converts the struct to a compact binary format.
        let bytes = bincode::serialize(entry)?;

        // Write the length of the binary payload as a 4-byte integer.
        // We use `.to_le_bytes()` (Little Endian) to ensure that the byte representation
        // remains consistent regardless of the host machine's native CPU endianness.
        let len = bytes.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;

        // Write the serialized data.
        self.file.write_all(&bytes)?;

        // CRITICAL FOR DURABILITY: `sync_all` forces the OS to flush its file system caches
        // directly to the physical storage media (similar to fsync in C). Without this,
        // data could remain in OS memory and be lost during a sudden power outage.
        self.file.sync_all()?;
        Ok(())
    }

    /// Reads all entries from a WAL file at the given path.
    ///
    /// This is called during database startup to perform crash recovery.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read the 4-byte length prefix.
            let mut len_bytes = [0u8; 4];
            match std::io::Read::read_exact(&mut reader, &mut len_bytes) {
                Ok(_) => {}
                // `UnexpectedEof` at the start of an entry read means we reached the end of the log file cleanly.
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read exactly `len` bytes of serialized payload.
            let mut bytes = vec![0u8; len];
            match std::io::Read::read_exact(&mut reader, &mut bytes) {
                Ok(_) => {}
                // If we get an EOF here, the file was truncated (a crash happened mid-write).
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(StorageError::Corruption(format!(
                        "WAL file was corrupted or truncated: expected {} bytes, hit EOF",
                        len
                    )));
                }
                Err(e) => return Err(StorageError::Io(e)),
            }

            // Deserialize the binary format back into our Rust enum.
            let entry: WalEntry = bincode::deserialize(&bytes)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}
