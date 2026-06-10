//! A generic append-only log of length-framed, checksummed records.
//!
//! This is the shared "Layer 1" persistence primitive from ADR 0001
//! (`docs/adr/0001-unified-metadata-persistence.md`): one implementation of
//! framing, fsync policy, and corruption-bounded recovery, reused by every
//! append-only file in the database (the storage WAL, the relational
//! transaction log, and the manifest's edit log).
//!
//! ## On-disk format
//!
//! ```text
//! [ 8-byte header ][ frame ][ frame ]...
//!   header = magic[4] ++ version[1] ++ reserved[3]
//!   frame  = len: u32 LE ++ checksum: xxh32 LE ++ postcard(payload)[len]
//! ```
//!
//! The header lets us evolve the format and detect type confusion (e.g. a
//! transaction log accidentally opened as a storage WAL fails loudly on the
//! magic check rather than silently mis-parsing). Recovery replays frames in
//! order and stops at the first truncated, checksum-mismatched, or
//! undeserializable frame — the normal way a log that was being appended to at
//! the moment of a crash ends.
//!
//! TODO(framed-log-block-format): investigate a LevelDB-style fixed-size block
//! layout (e.g. 32 KiB blocks with per-fragment CRC and FULL/FIRST/MIDDLE/LAST
//! record types). That would give two things the current record-stream format
//! does not: corruption *containment* (a single bad record no longer ends
//! recovery for everything after it) and natural I/O block alignment, which
//! dovetails with the SSD block-padding idea deferred in ADR 0001. It is a
//! larger format change and deserves its own ADR before we take it.

use crate::{FsyncMethod, Result, StorageError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// Upper bound on a single frame payload accepted by recovery. Real entries are
/// small; a length prefix above this cap is treated as corruption and stops
/// further recovery before it can drive a huge allocation.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Size of the on-disk file header in bytes.
const HEADER_LEN: usize = 8;

/// Identifies the on-disk format of a [`FramedLog`]: a 4-byte magic naming the
/// log kind plus a format version. Stored in the file header and validated on
/// open and recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogFormat {
    pub magic: [u8; 4],
    pub version: u8,
}

impl LogFormat {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        // buf[5..8] reserved, left zero.
        buf
    }
}

/// An append-only log of `E`-typed records, framed and checksummed on disk.
///
/// `E` is the *owned* record type produced by recovery; appends accept any
/// `Serialize` value (so callers can hand in a borrowed/zero-copy view that
/// serializes identically, as the storage WAL does with its `WalEntryRef`).
pub struct FramedLog<E> {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    format: LogFormat,
    sync_interval_ms: Option<u64>,
    fsync_method: FsyncMethod,
    /// Total bytes in the file, including the header. Tracked in memory so the
    /// write hot path can make flush decisions without an `fstat` per append.
    size_bytes: u64,
    /// Reusable serialization buffer for [`append`](Self::append): holds
    /// `[len][checksum][payload]` so a record can be framed and written with a
    /// single `write` and zero per-append heap allocation.
    scratch: Vec<u8>,
    _marker: PhantomData<fn() -> E>,
}

impl<E: DeserializeOwned> FramedLog<E> {
    /// Opens an existing log or creates a new one in append-only mode.
    ///
    /// A new (or empty / partially-written-header) file is initialized with a
    /// fresh header; an existing file has its header validated against
    /// `format`.
    pub fn open(
        path: impl AsRef<Path>,
        format: LogFormat,
        sync_interval_ms: Option<u64>,
        fsync_method: FsyncMethod,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut size_bytes = file.metadata()?.len();

        if size_bytes < HEADER_LEN as u64 {
            // Empty file, or a header torn by a crash before any frame could be
            // written (no frame can exist without a complete header preceding
            // it). Either way there is no data to lose: (re)write the header.
            file.set_len(0)?;
            (&file).write_all(&format.encode())?;
            crate::sync_file(&file, fsync_method)?;
            size_bytes = HEADER_LEN as u64;
        } else {
            validate_header(&path, format)?;
        }

        Ok(Self {
            file,
            path,
            format,
            sync_interval_ms,
            fsync_method,
            size_bytes,
            scratch: Vec::new(),
            _marker: PhantomData,
        })
    }

    /// Write a buffer that already holds one or more complete pre-built frames
    /// (each produced by [`build_frame`]) in a single `write()` call, then
    /// fsync. The caller owns framing and concatenation; this only does the
    /// write, the in-memory size bookkeeping, and the sync — so the common
    /// single-frame case incurs no copy here at all.
    pub fn append_prebuilt_bytes(&mut self, framed: &[u8]) -> Result<()> {
        if framed.is_empty() {
            return Ok(());
        }
        (&self.file).write_all(framed)?;
        self.size_bytes += framed.len() as u64;
        if self.sync_interval_ms.is_none() || self.sync_interval_ms == Some(0) {
            crate::sync_file(&self.file, self.fsync_method)?;
        }
        Ok(())
    }

    /// Appends one record and (depending on sync policy) forces it to disk.
    pub fn append<S: Serialize>(&mut self, entry: &S) -> Result<()> {
        // Frame `[len][checksum][payload]` into a reusable scratch buffer and
        // emit it with a single `write`. Serializing the payload after an 8-byte
        // header placeholder, then backfilling len + checksum, keeps the whole
        // record contiguous with no per-append heap allocation.
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        scratch.extend_from_slice(&[0u8; 8]); // placeholder for len + checksum
        // On a serialization error `self.scratch` is left empty; it simply
        // re-grows on the next append.
        let mut scratch = postcard::to_extend(entry, scratch)?;

        let payload_len = (scratch.len() - 8) as u32;
        let checksum = compute_checksum(&scratch[8..]);
        scratch[0..4].copy_from_slice(&payload_len.to_le_bytes());
        scratch[4..8].copy_from_slice(&checksum.to_le_bytes());

        let write_res = self.file.write_all(&scratch);
        let record_len = scratch.len() as u64;
        scratch.clear();
        self.scratch = scratch; // return the buffer for reuse
        write_res?;
        self.size_bytes += record_len;

        // Per-append fsync unless a non-zero sync interval defers it to a
        // periodic background sync (see `sync_all`).
        if self.sync_interval_ms.is_none() || self.sync_interval_ms == Some(0) {
            crate::sync_file(&self.file, self.fsync_method)?;
        }
        Ok(())
    }

    /// Explicitly flushes buffered appends to disk. Used for periodic
    /// background syncing when a non-zero sync interval is configured.
    pub fn sync_all(&self) -> Result<()> {
        crate::sync_file(&self.file, self.fsync_method)?;
        Ok(())
    }

    /// Truncates the log back to an empty (header-only) state and fsyncs.
    ///
    /// Used after the log's records have been folded into durable state
    /// elsewhere (e.g. a transaction-log checkpoint) so the next epoch starts
    /// clean.
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        (&self.file).write_all(&self.format.encode())?;
        crate::sync_file(&self.file, self.fsync_method)?;
        self.size_bytes = HEADER_LEN as u64;
        Ok(())
    }

    /// Total size of the log file in bytes (header included), served from an
    /// in-memory counter so it is cheap on the write hot path.
    pub fn size(&self) -> u64 {
        self.size_bytes
    }

    /// Reads every record from the log at `path`, used during crash recovery.
    /// A missing file recovers as empty.
    pub fn recover(path: impl AsRef<Path>, format: LogFormat) -> Result<Vec<E>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        Self::recover_from_reader(BufReader::new(file), format)
    }

    /// Reader-based recovery used by [`FramedLog::recover`] and by in-memory
    /// tests/fuzz harnesses that don't round-trip through the filesystem.
    pub fn recover_from_reader<R: Read>(mut reader: R, format: LogFormat) -> Result<Vec<E>> {
        // Read and validate the header. An empty or partial header means a
        // freshly created (or crashed-during-creation) file with no frames.
        let mut header = [0u8; HEADER_LEN];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(StorageError::Io(e)),
        }
        if header[0..4] != format.magic {
            return Err(StorageError::Corruption(format!(
                "framed log magic mismatch: expected {:?}, found {:?}",
                format.magic,
                &header[0..4]
            )));
        }
        if header[4] != format.version {
            return Err(StorageError::Corruption(format!(
                "unsupported framed log version {} (expected {})",
                header[4], format.version
            )));
        }

        let mut entries = Vec::new();
        loop {
            // 1. Length prefix. A clean EOF here is the normal end of the log.
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Bound the per-frame size before allocating; a corrupted length
            // prefix would otherwise drive a huge allocation far downstream
            // from the actual corruption.
            if len > MAX_FRAME_BYTES {
                tracing::warn!(
                    len,
                    cap = MAX_FRAME_BYTES,
                    "framed log entry length exceeds cap; stopping recovery"
                );
                break;
            }

            // 2. Checksum.
            let mut checksum_bytes = [0u8; 4];
            match reader.read_exact(&mut checksum_bytes) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("framed log ended with truncated checksum; stopping recovery");
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }
            let expected_checksum = u32::from_le_bytes(checksum_bytes);

            // 3. Payload.
            let mut bytes = vec![0u8; len];
            match reader.read_exact(&mut bytes) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("framed log ended with truncated payload; stopping recovery");
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
                    "framed log checksum mismatch; stopping recovery"
                );
                break;
            }

            // 5. Deserialize.
            match postcard::from_bytes(&bytes) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(error = %e, "framed log entry deserialization failed; stopping recovery");
                    break;
                }
            }
        }

        Ok(entries)
    }
}

/// Reads and validates just the header of an existing log file.
fn validate_header(path: &Path, format: LogFormat) -> Result<()> {
    let mut file = File::open(path)?;
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)?;
    if header[0..4] != format.magic {
        return Err(StorageError::Corruption(format!(
            "framed log magic mismatch in {}: expected {:?}, found {:?}",
            path.display(),
            format.magic,
            &header[0..4]
        )));
    }
    if header[4] != format.version {
        return Err(StorageError::Corruption(format!(
            "unsupported framed log version {} in {} (expected {})",
            header[4],
            path.display(),
            format.version
        )));
    }
    Ok(())
}

pub(crate) fn compute_checksum(data: &[u8]) -> u32 {
    xxhash_rust::xxh32::xxh32(data, 0)
}

/// Serialize `entry` into a complete framed record (`[len][checksum][payload]`)
/// and return the buffer. `buf` is the reusable backing allocation (cleared on
/// entry); on serialization error the returned `Err` carries the reason and `buf`
/// is consumed. Called by writer threads to pre-serialize their WAL entry before
/// joining the group-commit queue so the leader can skip serialization entirely.
pub fn build_frame<S: Serialize>(entry: &S, mut buf: Vec<u8>) -> Result<Vec<u8>> {
    buf.clear();
    buf.extend_from_slice(&[0u8; 8]);
    let mut buf = postcard::to_extend(entry, buf)?;
    let payload_len = (buf.len() - 8) as u32;
    let checksum = compute_checksum(&buf[8..]);
    buf[0..4].copy_from_slice(&payload_len.to_le_bytes());
    buf[4..8].copy_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    const TEST_FORMAT: LogFormat = LogFormat {
        magic: *b"DTST",
        version: 1,
    };

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum TestRec {
        A(u64),
        B(String),
    }

    /// Encode a single frame (len + checksum + payload) the way `append` does,
    /// so recovery tests can hand-build streams with deliberate damage.
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&compute_checksum(payload).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn rec_payload(rec: &TestRec) -> Vec<u8> {
        postcard::to_allocvec(rec).unwrap()
    }

    /// Prepend a valid `TEST_FORMAT` header to a body of frames.
    fn with_header(body: &[u8]) -> Vec<u8> {
        let mut out = TEST_FORMAT.encode().to_vec();
        out.extend_from_slice(body);
        out
    }

    fn recover(stream: &[u8]) -> Vec<TestRec> {
        FramedLog::<TestRec>::recover_from_reader(stream, TEST_FORMAT).unwrap()
    }

    #[test]
    fn test_append_and_recover_round_trips() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        {
            let mut log =
                FramedLog::<TestRec>::open(&path, TEST_FORMAT, None, FsyncMethod::Fullfsync)
                    .unwrap();
            log.append(&TestRec::A(7)).unwrap();
            log.append(&TestRec::B("hi".into())).unwrap();
        }
        let recovered = FramedLog::<TestRec>::recover(&path, TEST_FORMAT).unwrap();
        assert_eq!(recovered, vec![TestRec::A(7), TestRec::B("hi".into())]);
    }

    #[test]
    fn test_reset_clears_records_but_keeps_a_valid_log() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.log");
        let mut log =
            FramedLog::<TestRec>::open(&path, TEST_FORMAT, None, FsyncMethod::Fullfsync).unwrap();
        log.append(&TestRec::A(1)).unwrap();
        log.reset().unwrap();
        assert_eq!(log.size(), HEADER_LEN as u64);
        log.append(&TestRec::A(2)).unwrap();
        drop(log);
        let recovered = FramedLog::<TestRec>::recover(&path, TEST_FORMAT).unwrap();
        assert_eq!(recovered, vec![TestRec::A(2)]);
    }

    #[test]
    fn test_recover_missing_file_is_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let recovered =
            FramedLog::<TestRec>::recover(dir.path().join("nope.log"), TEST_FORMAT).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_empty_or_partial_header_is_empty() {
        assert!(recover(&[]).is_empty());
        assert!(recover(&[0x44, 0x54]).is_empty()); // 2 of 8 header bytes
    }

    #[test]
    fn test_recover_rejects_magic_mismatch() {
        let mut stream = b"XXXX".to_vec();
        stream.push(1);
        stream.extend_from_slice(&[0, 0, 0]);
        stream.extend_from_slice(&frame(&rec_payload(&TestRec::A(1))));
        let err = FramedLog::<TestRec>::recover_from_reader(&stream[..], TEST_FORMAT);
        assert!(matches!(err, Err(StorageError::Corruption(_))));
    }

    #[test]
    fn test_recover_rejects_version_mismatch() {
        let mut stream = TEST_FORMAT.magic.to_vec();
        stream.push(99); // wrong version
        stream.extend_from_slice(&[0, 0, 0]);
        let err = FramedLog::<TestRec>::recover_from_reader(&stream[..], TEST_FORMAT);
        assert!(matches!(err, Err(StorageError::Corruption(_))));
    }

    #[test]
    fn test_recover_stops_at_truncated_length_prefix() {
        let mut body = frame(&rec_payload(&TestRec::A(1)));
        body.push(0x07);
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }

    #[test]
    fn test_recover_stops_at_truncated_checksum() {
        let mut body = frame(&rec_payload(&TestRec::A(1)));
        body.extend_from_slice(&8u32.to_le_bytes());
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }

    #[test]
    fn test_recover_stops_at_truncated_payload() {
        let mut body = frame(&rec_payload(&TestRec::A(1)));
        body.extend_from_slice(&16u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }

    #[test]
    fn test_recover_stops_at_oversized_length() {
        let mut body = frame(&rec_payload(&TestRec::A(1)));
        body.extend_from_slice(&((MAX_FRAME_BYTES as u32) + 1).to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }

    #[test]
    fn test_recover_stops_at_checksum_mismatch() {
        let payload = rec_payload(&TestRec::A(1));
        let good = frame(&payload);
        let mut bad = Vec::new();
        bad.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bad.extend_from_slice(&compute_checksum(&payload).wrapping_add(1).to_le_bytes());
        bad.extend_from_slice(&payload);
        let body = [good, bad].concat();
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }

    #[test]
    fn test_recover_stops_at_undeserializable_payload() {
        let good = frame(&rec_payload(&TestRec::A(1)));
        // Valid checksum over bytes postcard can't decode into TestRec (a bogus
        // enum discriminant well past the defined variants).
        let garbage = vec![0xFFu8; 8];
        let body = [good, frame(&garbage)].concat();
        assert_eq!(recover(&with_header(&body)), vec![TestRec::A(1)]);
    }
}
