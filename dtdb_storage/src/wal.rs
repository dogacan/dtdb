use crate::{DbKey, DbValue, Result, StorageError};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

/// Represents an operation entry in the Write-Ahead Log (WAL).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalEntry {
    Put { key: DbKey, value: DbValue },
    Delete { key: DbKey },
    Batch(Vec<WalEntry>),
}

/// A Write-Ahead Log (WAL) provides durability by writing mutations to disk
/// before applying them to the in-memory MemTable.
///
/// If the database crashes, the log is replayed on startup to restore the MemTable.
pub struct Wal {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    sync_interval_ms: Option<u64>,
}

#[derive(Serialize)]
enum WalEntryRef<'a> {
    Put { key: &'a DbKey, value: &'a DbValue },
    Delete { key: &'a DbKey },
    Batch(&'a [WalEntry]),
}

impl Wal {
    /// Opens an existing WAL file or creates a new one in append-only mode.
    pub fn open(path: impl AsRef<Path>, sync_interval_ms: Option<u64>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            file,
            path,
            sync_interval_ms,
        })
    }

    /// Appends a `Put` operation to the log.
    pub fn append_put(&mut self, key: &DbKey, value: &DbValue) -> Result<()> {
        let entry = WalEntryRef::Put { key, value };
        self.write_entry_ref(&entry)
    }

    /// Appends a `Delete` operation to the log.
    pub fn append_delete(&mut self, key: &DbKey) -> Result<()> {
        let entry = WalEntryRef::Delete { key };
        self.write_entry_ref(&entry)
    }

    /// Appends a batch of operations to the log.
    pub fn append_batch(&mut self, entries: &[WalEntry]) -> Result<()> {
        let entry = WalEntryRef::Batch(entries);
        self.write_entry_ref(&entry)
    }

    /// Serializes and writes a `WalEntryRef` to the file, and forces it to disk.
    fn write_entry_ref(&mut self, entry: &WalEntryRef) -> Result<()> {
        // Serialize using bincode, which converts the struct to a compact binary format.
        let bytes = bincode::serialize(entry)?;

        // Write length, checksum, and data in a single buffer to avoid multiple syscalls.
        let len = bytes.len() as u32;
        let checksum = compute_checksum(&bytes);

        let mut buffer = Vec::with_capacity(4 + 4 + bytes.len());
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(&checksum.to_le_bytes());
        buffer.extend_from_slice(&bytes);

        self.file.write_all(&buffer)?;

        // CRITICAL FOR DURABILITY: `sync_all` forces the OS to flush its file system caches
        // directly to the physical storage media (similar to fsync in C). Without this,
        // data could remain in OS memory and be lost during a sudden power outage.
        if self.sync_interval_ms.is_none() || self.sync_interval_ms == Some(0) {
            self.file.sync_all()?;
        }
        Ok(())
    }

    /// Explicitly flushes WAL buffers to disk. Used for periodic background syncing.
    pub fn sync_all(&self) -> Result<()> {
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
            // 1. Read the 4-byte length prefix.
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

            // 2. Read the 4-byte checksum.
            let mut checksum_bytes = [0u8; 4];
            match std::io::Read::read_exact(&mut reader, &mut checksum_bytes) {
                Ok(_) => {}
                // Truncated checksum: trailing corruption, stop recovery
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    eprintln!("Warning: WAL ended with truncated checksum. Stopping recovery.");
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }
            let expected_checksum = u32::from_le_bytes(checksum_bytes);

            // 3. Read exactly `len` bytes of serialized payload.
            let mut bytes = vec![0u8; len];
            match std::io::Read::read_exact(&mut reader, &mut bytes) {
                Ok(_) => {}
                // Truncated payload: trailing corruption, stop recovery
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    eprintln!("Warning: WAL ended with truncated payload. Stopping recovery.");
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }

            // 4. Verify checksum.
            let actual_checksum = compute_checksum(&bytes);
            if actual_checksum != expected_checksum {
                eprintln!("Warning: WAL checksum mismatch. Stopping recovery.");
                break;
            }

            // 5. Deserialize the binary format back into our Rust enum.
            let entry: WalEntry = match bincode::deserialize(&bytes) {
                Ok(ent) => ent,
                Err(e) => {
                    eprintln!(
                        "Warning: Deserialization failed for WAL entry: {}. Stopping recovery.",
                        e
                    );
                    break;
                }
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Returns the current size of the WAL file in bytes.
    pub fn size(&self) -> Result<u64> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }
}

fn compute_checksum(data: &[u8]) -> u32 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(data);
    hasher.finish() as u32
}
