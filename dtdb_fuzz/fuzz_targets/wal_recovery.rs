//! Fuzz target: WAL recovery must never panic, must be deterministic, and must
//! be truncation-safe (recovering a prefix of bytes yields a prefix of the full
//! recovery's entries).
//!
//! Surface: `dtdb_storage::wal::Wal::recover_from_reader` — the in-memory
//! variant of `recover()` used by both the on-disk recovery path during
//! startup and this fuzz harness. Driving it via `Cursor` keeps the per-iter
//! cost in the microsecond range instead of millisecond-range disk I/O.

use bolero::check;
use dtdb_storage::WalEntry;
use dtdb_storage::wal::Wal;
use std::io::Cursor;

fn recover_bytes(bytes: &[u8]) -> dtdb_storage::Result<Vec<WalEntry>> {
    Wal::recover_from_reader(Cursor::new(bytes))
}

fn entries_eq(a: &[WalEntry], b: &[WalEntry]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| {
        // Both sides come from the same recover() path, so a round-trip
        // through bincode gives a stable byte-level comparison without
        // needing PartialEq on WalEntry.
        bincode::serialize(x).ok() == bincode::serialize(y).ok()
    })
}

#[test]
#[ignore = "fuzz target — run with `cargo test -p dtdb_fuzz -- --ignored` or scripts/run_fuzz.sh"]
fn fuzz_wal_recover() {
    check!()
        .with_iterations(dtdb_fuzz::bolero_iterations())
        .with_type::<Vec<u8>>()
        .for_each(|bytes: &Vec<u8>| {
            // 1. Never panics, returns Ok or typed error. Two consecutive
            //    recoveries of the same bytes must agree (determinism).
            let first = recover_bytes(bytes);
            let second = recover_bytes(bytes);
            match (&first, &second) {
                (Ok(a), Ok(b)) => assert!(
                    entries_eq(a, b),
                    "WAL recover is non-deterministic for the same bytes"
                ),
                (Err(_), Err(_)) => {}
                _ => panic!("WAL recover is non-deterministic (ok vs err) for the same bytes"),
            }

            // 2. Truncation safety: a recovery of a prefix must be a prefix
            //    of the full recovery.
            if let Ok(full) = first
                && !bytes.is_empty()
            {
                let n = bytes.len() / 2;
                if let Ok(t) = recover_bytes(&bytes[..n]) {
                    assert!(
                        t.len() <= full.len(),
                        "truncated recovery returned more entries than full"
                    );
                    assert!(
                        entries_eq(&t, &full[..t.len()]),
                        "truncated recovery is not a prefix of full recovery (n={n}, full_len={})",
                        bytes.len()
                    );
                }
            }
        });
}
