//! Fuzz target: SSTable reader must handle corruption cleanly — never panic,
//! never read out of bounds, always surface a typed `StorageError`.
//!
//! Strategy:
//!   1. Build a valid SSTable from a small set of (key, value) pairs.
//!   2. Apply a random mutation (bit-flip, truncate, zero-fill, or footer
//!      corruption) chosen from the bolero input.
//!   3. Attempt to open + scan + point-lookup keys and assert nothing panics.
//!
//! Surface: `dtdb_storage::sstable::SstableReader::open` / `get` / `scan_raw`.

use bolero::{TypeGenerator, check};
use dtdb_storage::sstable::{SstableReader, SstableWriter};
use dtdb_storage::{CompressionType, DbKey, DbValue};
use std::io::{Read, Seek, SeekFrom, Write};

/// Bolero-generated mutation applied to a freshly-written SSTable file.
#[derive(Debug, TypeGenerator)]
enum Mutation {
    /// Flip a single bit at `offset % file_len`.
    BitFlip { offset: u32, bit: u8 },
    /// Truncate the file to `len % (file_len + 1)` bytes.
    Truncate { len: u32 },
    /// Zero out a window of bytes.
    ZeroFill { offset: u32, len: u32 },
    /// Corrupt the 24-byte footer (last 24 bytes) at a chosen byte.
    FooterByte { idx: u8, value: u8 },
    /// Leave the file untouched — verifies the happy path through the same
    /// reader code each iteration.
    None,
}

#[derive(Debug, TypeGenerator)]
struct Input {
    /// Up to ~32 keys, deduplicated and sorted before being written.
    keys: Vec<i32>,
    compression_lz4: bool,
    mutation: Mutation,
}

fn build_sstable(path: &std::path::Path, keys: &[i32], lz4: bool) -> dtdb_storage::Result<()> {
    let compression = if lz4 {
        CompressionType::Lz4
    } else {
        CompressionType::Uncompressed
    };
    let mut sorted: Vec<i32> = keys.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut w = SstableWriter::create(path, 256, compression, sorted.len(), vec![])?;
    for k in &sorted {
        w.append(&DbKey::Int(*k as i64), Some(&DbValue::Int(*k as i64 * 7)))?;
    }
    w.finish()
}

fn apply_mutation(path: &std::path::Path, m: &Mutation) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    match m {
        Mutation::BitFlip { offset, bit } => {
            let pos = (*offset as u64) % len;
            file.seek(SeekFrom::Start(pos))?;
            let mut b = [0u8; 1];
            file.read_exact(&mut b)?;
            b[0] ^= 1 << (bit % 8);
            file.seek(SeekFrom::Start(pos))?;
            file.write_all(&b)?;
        }
        Mutation::Truncate { len: t } => {
            let n = (*t as u64) % (len + 1);
            file.set_len(n)?;
        }
        Mutation::ZeroFill { offset, len: zlen } => {
            let pos = (*offset as u64) % len;
            let n = (*zlen as u64).min(len - pos);
            file.seek(SeekFrom::Start(pos))?;
            file.write_all(&vec![0u8; n as usize])?;
        }
        Mutation::FooterByte { idx, value } => {
            if len < 24 {
                return Ok(());
            }
            let pos = len - 24 + (*idx as u64 % 24);
            file.seek(SeekFrom::Start(pos))?;
            file.write_all(&[*value])?;
        }
        Mutation::None => {}
    }
    file.sync_all()
}

#[test]
#[ignore = "fuzz target — run with `cargo test -p dtdb_fuzz -- --ignored` or scripts/run_fuzz.sh"]
fn fuzz_sstable_reader() {
    check!()
        .with_iterations(dtdb_fuzz::bolero_iterations())
        .with_type::<Input>()
        .for_each(|input: &Input| {
            let dir = match tempfile::TempDir::new() {
                Ok(d) => d,
                Err(_) => return,
            };
            let path = dir.path().join("table.sst");
            if build_sstable(&path, &input.keys, input.compression_lz4).is_err() {
                return;
            }
            if apply_mutation(&path, &input.mutation).is_err() {
                return;
            }

            // Open + drive the reader. All failure modes must be typed errors.
            let reader = match SstableReader::open(&path, 0, 0, None) {
                Ok(r) => r,
                Err(_) => return, // typed error is fine
            };

            // Point-lookups for keys we wrote and keys we did not.
            for k in input.keys.iter().take(8) {
                let _ = reader.get(&DbKey::Int(*k as i64));
            }
            let _ = reader.get(&DbKey::Int(i64::MIN));
            let _ = reader.get(&DbKey::Int(i64::MAX));

            // Full range scan.
            let _ = reader.scan_raw(&DbKey::Int(i64::MIN), &DbKey::Int(i64::MAX));
        });
}
