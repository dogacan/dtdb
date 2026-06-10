use crate::framed_log::{FramedLog, LogFormat};
use crate::{DbKey, DbValue, Result};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::Path;

/// On-disk format tag for the storage WAL. The magic distinguishes it from
/// other [`FramedLog`] files (e.g. the transaction log) so opening the wrong
/// kind fails loudly on the header check.
const WAL_FORMAT: LogFormat = LogFormat {
    magic: *b"DWAL",
    version: 1,
};

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
/// If the database crashes, the log is replayed on startup to restore the
/// MemTable. The framing, checksums, fsync policy, and corruption-bounded
/// recovery all live in the shared [`FramedLog`] primitive; this type only
/// adds the WAL-specific entry shape.
pub struct Wal {
    log: FramedLog<WalEntry>,
}

/// Borrowed view of a [`WalEntry`] used to serialize an append without cloning
/// the key/value into an owned `WalEntry`. It must serialize identically to the
/// owned form so recovery can decode it back into a `WalEntry`.
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
        let log = FramedLog::open(path, WAL_FORMAT, sync_interval_ms, fsync_method)?;
        Ok(Self { log })
    }

    /// Appends a `Put` operation to the log.
    pub fn append_put(&mut self, key: &DbKey, value: &DbValue) -> Result<()> {
        self.log.append(&WalEntryRef::Put { key, value })
    }

    /// Appends a `Delete` operation to the log.
    pub fn append_delete(&mut self, key: &DbKey) -> Result<()> {
        self.log.append(&WalEntryRef::Delete { key })
    }

    /// Appends a batch of operations to the log.
    pub fn append_batch(&mut self, entries: &[WalEntry]) -> Result<()> {
        self.log.append(&WalEntryRef::Batch(entries))
    }

    /// Build a complete framed WAL record from `entries` into `buf` (cleared
    /// first) and return it. Call in writer threads before joining the
    /// group-commit queue so the leader can use [`Wal::append_prebuilt_bytes`]
    /// and skip re-serializing the merged batch.
    pub fn build_frame(entries: &[WalEntry], buf: Vec<u8>) -> Result<Vec<u8>> {
        crate::framed_log::build_frame(&WalEntryRef::Batch(entries), buf)
    }

    /// Write a buffer of one or more concatenated pre-built frames (each from
    /// [`Wal::build_frame`]) in a single `write()` + fsync. Used by the
    /// group-commit leader.
    pub fn append_prebuilt_bytes(&mut self, framed: &[u8]) -> Result<()> {
        self.log.append_prebuilt_bytes(framed)
    }

    /// Explicitly flushes WAL buffers to disk. Used for periodic background
    /// syncing when a non-zero sync interval is configured.
    pub fn sync_all(&self) -> Result<()> {
        self.log.sync_all()
    }

    /// Reads all entries from a WAL file at the given path, for crash recovery.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalEntry>> {
        FramedLog::recover(path, WAL_FORMAT)
    }

    /// Reader-based recovery used by `recover()` and by in-memory tests / fuzz
    /// harnesses that don't want to round-trip through the filesystem.
    pub fn recover_from_reader<R: Read>(reader: R) -> Result<Vec<WalEntry>> {
        FramedLog::recover_from_reader(reader, WAL_FORMAT)
    }

    /// Returns the current size of the WAL file in bytes (header included).
    ///
    /// Served from an in-memory counter, so this is cheap to call on the write
    /// hot path (no `fstat` syscall).
    pub fn size(&self) -> Result<u64> {
        Ok(self.log.size())
    }
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
    fn test_put_and_delete_round_trip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ops.wal");
        {
            let mut wal = Wal::open(&path, None, crate::FsyncMethod::Fullfsync).unwrap();
            wal.append_put(&k(1), &v(10)).unwrap();
            wal.append_delete(&k(1)).unwrap();
        }
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(matches!(entries[0], WalEntry::Put { .. }));
        assert!(matches!(entries[1], WalEntry::Delete { .. }));
    }

    #[test]
    fn test_sync_all_and_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("size.wal");
        let mut wal = Wal::open(&path, Some(1000), crate::FsyncMethod::Fullfsync).unwrap();
        let empty = wal.size().unwrap();
        wal.append_put(&k(1), &v(1)).unwrap();
        // With a non-zero sync interval, append doesn't fsync; do it explicitly.
        wal.sync_all().unwrap();
        assert!(wal.size().unwrap() > empty);
    }

    #[test]
    fn test_recover_missing_file_is_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let entries = Wal::recover(dir.path().join("does_not_exist.wal")).unwrap();
        assert!(entries.is_empty());
    }
}
