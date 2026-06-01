//! Engine-level tests for the reference-counted scan-iterator payloads
//! (ADR 0004). The unit tests in `lib.rs` cover the type-level invariants
//! (Arc sharing, ordering, serde format); these cover the two engine-level
//! guarantees the ADR rests on: scan snapshots are stable under concurrent
//! mutation, and constructing a scan iterator does not hold the memtable read
//! lock for the duration of the scan.

use dtdb_storage::{
    CompressionType, DbKey, DbValue, EngineOptions, ScanIterator, StorageEngine,
};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

/// Options with a memtable large enough that the test data never flushes, so
/// the scan is served entirely from the in-memory `BTreeMap` snapshot — the
/// hot path ADR 0004 optimizes.
fn in_memory_engine() -> (TempDir, Arc<StorageEngine>) {
    let tmp = TempDir::new().unwrap();
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 64 * 1024 * 1024,
        ..Default::default()
    };
    let engine = Arc::new(StorageEngine::open(tmp.path(), options).unwrap());
    (tmp, engine)
}

fn k(s: &str) -> DbKey {
    DbKey::string(s)
}

fn drain(mut it: ScanIterator) -> Vec<(DbKey, DbValue)> {
    let mut out = Vec::new();
    while let Some(pair) = it.next().unwrap() {
        out.push(pair);
    }
    out
}

/// A scan iterator snapshots the memtable range on construction. Overwriting
/// and deleting those keys afterwards must not change what the live iterator
/// yields — the captured `Arc` payloads keep the original buffers valid and
/// unmutated even though the memtable entries have been replaced.
#[test]
fn scan_snapshot_is_isolated_from_later_mutations() {
    let (_tmp, engine) = in_memory_engine();

    engine.put(k("k1"), DbValue::string("original-1")).unwrap();
    engine.put(k("k2"), DbValue::string("original-2")).unwrap();
    engine.put(k("k3"), DbValue::string("original-3")).unwrap();

    // Snapshot the range, then mutate every key behind the iterator's back.
    let it = engine.scan_iter(&k("k1"), &k("k3")).unwrap();
    engine
        .put(k("k2"), DbValue::string("CLOBBERED"))
        .unwrap();
    engine.delete(k("k3")).unwrap();
    engine.put(k("k4"), DbValue::string("inserted")).unwrap();

    let got = drain(it);
    assert_eq!(
        got,
        vec![
            (k("k1"), DbValue::string("original-1")),
            (k("k2"), DbValue::string("original-2")),
            (k("k3"), DbValue::string("original-3")),
        ],
        "scan must yield the snapshot taken at construction, not later writes"
    );

    // A fresh scan, by contrast, observes the mutations.
    let fresh = drain(engine.scan_iter(&k("k1"), &k("k4")).unwrap());
    assert_eq!(
        fresh,
        vec![
            (k("k1"), DbValue::string("original-1")),
            (k("k2"), DbValue::string("CLOBBERED")),
            (k("k4"), DbValue::string("inserted")),
        ],
        "k3 was deleted; k2 was overwritten; k4 was inserted"
    );
}

/// Constructing a `ScanIterator` must release the memtable read lock (it
/// snapshots, then drops the guard). If it instead held the lock for the scan's
/// lifetime, a concurrent writer would block until the iterator is dropped —
/// the deadlock ADR 0004 calls out. We assert the writer makes progress while
/// the iterator is still alive, with a timeout so a regression fails fast
/// instead of hanging the suite.
#[test]
fn scan_iterator_does_not_block_concurrent_writers() {
    let (_tmp, engine) = in_memory_engine();
    for i in 0..16 {
        engine
            .put(k(&format!("key{i:02}")), DbValue::string("v"))
            .unwrap();
    }

    // Hold a live iterator across the whole writer interaction.
    let it = engine.scan_iter(&k("key00"), &k("key15")).unwrap();

    let writer = engine.clone();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        writer
            .put(k("key99"), DbValue::string("written-during-scan"))
            .unwrap();
        let _ = tx.send(());
    });

    rx.recv_timeout(Duration::from_secs(10))
        .expect("writer blocked while a scan iterator was alive — read lock not released");
    handle.join().unwrap();

    // The iterator is still usable and reflects its construction-time snapshot
    // (the concurrent write to key99 is outside the snapshotted range anyway).
    let got = drain(it);
    assert_eq!(got.len(), 16);
}
