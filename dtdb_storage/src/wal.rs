use crate::{DbKey, DbValue, Result, StorageError};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

/// Upper bound on the size of a single WAL entry payload accepted by
/// recovery. Real entries are small; a length prefix above this cap is
/// treated as corruption and stops further recovery.
const MAX_WAL_ENTRY_BYTES: usize = 64 * 1024 * 1024;

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
    fsync_method: crate::FsyncMethod,
    /// Bytes written to the WAL, tracked in memory so the hot write path can
    /// consult the size for flush decisions without an `fstat` syscall per
    /// write. Seeded from the file's length on open.
    size_bytes: u64,
}

#[derive(Serialize)]
enum WalEntryRef<'a> {
    Put { key: &'a DbKey, value: &'a DbValue },
    Delete { key: &'a DbKey },
    Batch(&'a [WalEntry]),
}

impl Wal {
    /// Opens an existing WAL file or creates a new one in append-only mode.
    pub fn open(
        path: impl AsRef<Path>,
        sync_interval_ms: Option<u64>,
        fsync_method: crate::FsyncMethod,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size_bytes = file.metadata()?.len();

        Ok(Self {
            file,
            path,
            sync_interval_ms,
            fsync_method,
            size_bytes,
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
        self.size_bytes += buffer.len() as u64;

        // CRITICAL FOR DURABILITY: `sync_all` forces the OS to flush its file system caches
        // directly to the physical storage media (similar to fsync in C). Without this,
        // data could remain in OS memory and be lost during a sudden power outage.
        if self.sync_interval_ms.is_none() || self.sync_interval_ms == Some(0) {
            crate::sync_file(&self.file, self.fsync_method)?;
        }
        Ok(())
    }

    /// Explicitly flushes WAL buffers to disk. Used for periodic background syncing.
    pub fn sync_all(&self) -> Result<()> {
        crate::sync_file(&self.file, self.fsync_method)?;
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
        Self::recover_from_reader(BufReader::new(file))
    }

    /// Reader-based recovery used by `recover()` and by in-memory tests /
    /// fuzz harnesses that don't want to round-trip through the filesystem.
    pub fn recover_from_reader<R: std::io::Read>(mut reader: R) -> Result<Vec<WalEntry>> {
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

            // Sanity-bound the per-entry size before allocating. A corrupted
            // length prefix can otherwise drive a multi-gigabyte allocation
            // that fails (or OOMs) far downstream from the actual corruption.
            // 64 MiB is well above any plausible single WAL entry.
            if len > MAX_WAL_ENTRY_BYTES {
                tracing::warn!(
                    len,
                    cap = MAX_WAL_ENTRY_BYTES,
                    "WAL entry length exceeds cap; stopping recovery"
                );
                break;
            }

            // 2. Read the 4-byte checksum.
            let mut checksum_bytes = [0u8; 4];
            match std::io::Read::read_exact(&mut reader, &mut checksum_bytes) {
                Ok(_) => {}
                // Truncated checksum: trailing corruption, stop recovery
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("WAL ended with truncated checksum; stopping recovery");
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
                    tracing::warn!("WAL ended with truncated payload; stopping recovery");
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }

            // 4. Verify checksum.
            let actual_checksum = compute_checksum(&bytes);
            if actual_checksum != expected_checksum {
                tracing::warn!(
                    expected = expected_checksum,
                    actual = actual_checksum,
                    "WAL checksum mismatch; stopping recovery"
                );
                break;
            }

            // 5. Deserialize the binary format back into our Rust enum.
            let entry: WalEntry = match bincode::deserialize(&bytes) {
                Ok(ent) => ent,
                Err(e) => {
                    tracing::warn!(error = %e, "WAL entry deserialization failed; stopping recovery");
                    break;
                }
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Returns the current size of the WAL file in bytes.
    ///
    /// Served from an in-memory counter, so this is cheap to call on the write
    /// hot path (no `fstat` syscall).
    pub fn size(&self) -> Result<u64> {
        Ok(self.size_bytes)
    }
}

fn compute_checksum(data: &[u8]) -> u32 {
    xxhash_rust::xxh32::xxh32(data, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(i: i64) -> DbKey {
        DbKey::Int(i)
    }
    fn v(i: i64) -> DbValue {
        DbValue::Int(i)
    }

    /// Encode a single WAL frame (len + checksum + payload) the way the writer
    /// does, so recovery tests can hand-build streams with deliberate damage.
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&compute_checksum(payload).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn put_payload(key: &DbKey, value: &DbValue) -> Vec<u8> {
        bincode::serialize(&WalEntryRef::Put { key, value }).unwrap()
    }

    #[test]
    fn test_append_batch_round_trips() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("batch.wal");
        {
            let mut wal = Wal::open(&path, None, crate::FsyncMethod::Fullfsync).unwrap();
            wal.append_batch(&[
                WalEntry::Put {
                    key: k(1),
                    value: v(10),
                },
                WalEntry::Delete { key: k(2) },
            ])
            .unwrap();
        }
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            WalEntry::Batch(sub) => assert_eq!(sub.len(), 2),
            other => panic!("expected a batch entry, got {other:?}"),
        }
    }

    #[test]
    fn test_sync_all_and_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("size.wal");
        let mut wal = Wal::open(&path, Some(1000), crate::FsyncMethod::Fullfsync).unwrap();
        assert_eq!(wal.size().unwrap(), 0);
        wal.append_put(&k(1), &v(1)).unwrap();
        // With a non-zero sync interval, append doesn't fsync; do it explicitly.
        wal.sync_all().unwrap();
        assert!(wal.size().unwrap() > 0);
    }

    #[test]
    fn test_recover_missing_file_is_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let entries = Wal::recover(dir.path().join("does_not_exist.wal")).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_recover_stops_at_truncated_length_prefix() {
        // One good entry, then a stray byte that can't form a 4-byte length.
        let mut stream = frame(&put_payload(&k(1), &v(1)));
        stream.push(0x07);
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_stops_at_truncated_checksum() {
        // Good entry, then a length prefix with no checksum following it.
        let mut stream = frame(&put_payload(&k(1), &v(1)));
        stream.extend_from_slice(&8u32.to_le_bytes());
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_stops_at_truncated_payload() {
        // Good entry, then len + checksum claiming 16 bytes but only 4 present.
        let mut stream = frame(&put_payload(&k(1), &v(1)));
        stream.extend_from_slice(&16u32.to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());
        stream.extend_from_slice(&[1, 2, 3, 4]);
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_stops_at_oversized_length() {
        // A length prefix above the per-entry cap is treated as corruption.
        let mut stream = frame(&put_payload(&k(1), &v(1)));
        stream.extend_from_slice(&((MAX_WAL_ENTRY_BYTES as u32) + 1).to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_stops_at_checksum_mismatch() {
        let payload = put_payload(&k(1), &v(1));
        let good = frame(&payload);

        // A second frame whose stored checksum doesn't match the payload.
        let mut bad = Vec::new();
        bad.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bad.extend_from_slice(&compute_checksum(&payload).wrapping_add(1).to_le_bytes());
        bad.extend_from_slice(&payload);

        let stream = [good, bad].concat();
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_stops_at_undeserializable_payload() {
        let good = frame(&put_payload(&k(1), &v(1)));
        // Bytes with a valid checksum that bincode can't decode into a WalEntry
        // (a bogus enum discriminant well past the defined variants).
        let garbage = vec![0xFFu8; 8];
        let stream = [good, frame(&garbage)].concat();
        let entries = Wal::recover_from_reader(&stream[..]).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
