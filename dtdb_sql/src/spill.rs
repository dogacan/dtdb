//! Reusable disk-spill primitives for bounded-memory operators.
//!
//! Operators that would otherwise buffer their full input in RAM (external sort,
//! spilling aggregation, sort-based distinct/set-ops, sort-merge join) share the
//! machinery here: byte estimation for budgeting, count-prefixed `bincode` run
//! files keyed by a `Vec<DbValue>` sort key, and a k-way merge over those runs.

use dtdb_storage::DbValue;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Stable type-rank used to give cross-type comparisons a deterministic order.
/// Nulls sort first, matching `compare_values`'s treatment of NULL as least.
fn type_rank(v: &DbValue) -> u8 {
    match v {
        DbValue::Null => 0,
        DbValue::Bool(_) => 1,
        DbValue::Int(_) => 2,
        DbValue::Float(_) => 3,
        DbValue::String(_) => 4,
        DbValue::Bytes(_) => 5,
    }
}

fn total_f64(a: f64, b: f64) -> Ordering {
    match a.partial_cmp(&b) {
        Some(ord) => ord,
        // Only reached when at least one operand is NaN. Treat all NaNs as equal
        // and order them after every non-NaN value.
        None => match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            _ => Ordering::Less,
        },
    }
}

/// Total ordering over `DbValue` for spill-internal sorting and merging.
///
/// Unlike `expr::compare_values` (which errors on NaN and compares numbers across
/// the Int/Float boundary), this is infallible and orders by variant first, so two
/// values compare `Equal` exactly when `DbValue`'s `Eq` considers them equal. That
/// equivalence is what makes sort-based grouping/dedup match the hash-based path.
pub fn total_compare(l: &DbValue, r: &DbValue) -> Ordering {
    let (lr, rr) = (type_rank(l), type_rank(r));
    if lr != rr {
        return lr.cmp(&rr);
    }
    match (l, r) {
        (DbValue::Null, DbValue::Null) => Ordering::Equal,
        (DbValue::Bool(a), DbValue::Bool(b)) => a.cmp(b),
        (DbValue::Int(a), DbValue::Int(b)) => a.cmp(b),
        (DbValue::Float(a), DbValue::Float(b)) => total_f64(*a, *b),
        (DbValue::String(a), DbValue::String(b)) => a.cmp(b),
        (DbValue::Bytes(a), DbValue::Bytes(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// Approximate in-memory footprint of a single `DbValue`, counting heap bytes.
pub fn estimate_value_size(val: &DbValue) -> usize {
    std::mem::size_of::<DbValue>()
        + match val {
            DbValue::String(s) => s.len(),
            DbValue::Bytes(b) => b.len(),
            _ => 0,
        }
}

/// Approximate in-memory footprint of a keyed row, used for spill budgeting.
pub fn estimate_row_size(keys: &[DbValue], values: &[DbValue]) -> usize {
    let mut size = std::mem::size_of::<Vec<DbValue>>() * 2;
    for val in values {
        size += estimate_value_size(val);
    }
    for val in keys {
        size += estimate_value_size(val);
    }
    size
}

/// Sorts an in-memory buffer of `(key, payload)` entries by the given per-key
/// directions (`true` = ascending).
pub fn sort_entries<P>(buffer: &mut [(Vec<DbValue>, P)], directions: &[bool]) {
    buffer.sort_by(|a, b| compare_keys(&a.0, &b.0, directions));
}

fn compare_keys(a: &[DbValue], b: &[DbValue], directions: &[bool]) -> Ordering {
    for (idx, &asc) in directions.iter().enumerate() {
        let ord = total_compare(&a[idx], &b[idx]);
        if ord != Ordering::Equal {
            return if asc { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// A sorted run of `(key, payload)` entries stored as a temporary file on disk.
/// The file is deleted automatically when the `Run` is dropped.
pub struct Run<P> {
    path: PathBuf,
    reader: Option<BufReader<File>>,
    peeked: Option<(Vec<DbValue>, P)>,
    remaining: usize,
}

impl<P: DeserializeOwned> Run<P> {
    fn open(&mut self) -> Result<(), String> {
        let file = File::open(&self.path)
            .map_err(|e| format!("Failed to open temp file {:?}: {}", self.path, e))?;
        let mut reader = BufReader::new(file);
        let count: u64 = bincode::deserialize_from(&mut reader)
            .map_err(|e| format!("Failed to read count from run file: {}", e))?;
        self.remaining = count as usize;
        self.reader = Some(reader);
        self.advance()?;
        Ok(())
    }

    fn advance(&mut self) -> Result<(), String> {
        if self.remaining == 0 {
            self.peeked = None;
            self.reader = None; // Close file handle early.
            return Ok(());
        }

        let reader = self.reader.as_mut().ok_or("Advance called on closed run")?;
        let keys: Vec<DbValue> = bincode::deserialize_from(&mut *reader)
            .map_err(|e| format!("Failed to deserialize keys: {}", e))?;
        let payload: P = bincode::deserialize_from(&mut *reader)
            .map_err(|e| format!("Failed to deserialize payload: {}", e))?;

        self.peeked = Some((keys, payload));
        self.remaining -= 1;
        Ok(())
    }
}

impl<P> Drop for Run<P> {
    fn drop(&mut self) {
        self.reader = None; // Release the file handle before removing the file.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Serializes a buffer of `(key, payload)` entries to a fresh temp file and returns
/// a `Run` handle over it. The caller is responsible for having sorted the buffer.
pub fn write_run<P: Serialize>(
    temp_dir: &Path,
    prefix: &str,
    buffer: &[(Vec<DbValue>, P)],
) -> Result<Run<P>, String> {
    static RUN_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let run_id = RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = temp_dir.join(format!("{}_{}_{}.bin", prefix, std::process::id(), run_id));

    let file =
        File::create(&path).map_err(|e| format!("Failed to create temp file {:?}: {}", path, e))?;
    let mut writer = BufWriter::new(file);

    bincode::serialize_into(&mut writer, &(buffer.len() as u64))
        .map_err(|e| format!("Serialization error: {}", e))?;

    for (keys, payload) in buffer {
        bincode::serialize_into(&mut writer, keys)
            .map_err(|e| format!("Serialization error: {}", e))?;
        bincode::serialize_into(&mut writer, payload)
            .map_err(|e| format!("Serialization error: {}", e))?;
    }

    writer
        .flush()
        .map_err(|e| format!("Failed to flush temp file: {}", e))?;

    Ok(Run {
        path,
        reader: None,
        peeked: None,
        remaining: buffer.len(),
    })
}

struct HeapEntry<P> {
    sort_keys: Vec<DbValue>,
    payload: P,
    run_index: usize,
    directions: std::rc::Rc<Vec<bool>>,
}

impl<P> Ord for HeapEntry<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse so the smallest key (per direction)
        // is popped first.
        compare_keys(&self.sort_keys, &other.sort_keys, &self.directions).reverse()
    }
}

impl<P> PartialOrd for HeapEntry<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> PartialEq for HeapEntry<P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<P> Eq for HeapEntry<P> {}

/// Streaming k-way merge over sorted runs, yielding `(key, payload)` pairs in the
/// order defined by `directions`.
pub struct KWayMerge<P> {
    heap: BinaryHeap<HeapEntry<P>>,
    runs: Vec<Run<P>>,
    directions: std::rc::Rc<Vec<bool>>,
}

impl<P: DeserializeOwned> KWayMerge<P> {
    /// Opens every run and primes the merge heap. `directions` gives the sort
    /// direction for each key column (`true` = ascending).
    pub fn new(mut runs: Vec<Run<P>>, directions: Vec<bool>) -> Result<Self, String> {
        let directions = std::rc::Rc::new(directions);
        let mut heap = BinaryHeap::new();

        for (idx, run) in runs.iter_mut().enumerate() {
            run.open()?;
            if let Some((sort_keys, payload)) = run.peeked.take() {
                heap.push(HeapEntry {
                    sort_keys,
                    payload,
                    run_index: idx,
                    directions: std::rc::Rc::clone(&directions),
                });
            }
        }

        Ok(Self {
            heap,
            runs,
            directions,
        })
    }

    /// Returns the next `(key, payload)` in merged order, or `None` when drained.
    ///
    /// Named `next` to match the pull-based operator convention; it is fallible and
    /// borrows externally-owned runs, so it cannot implement `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(Vec<DbValue>, P)>, String> {
        let entry = match self.heap.pop() {
            Some(e) => e,
            None => return Ok(None),
        };

        let run_idx = entry.run_index;
        self.runs[run_idx].advance()?;

        if let Some((sort_keys, payload)) = self.runs[run_idx].peeked.take() {
            self.heap.push(HeapEntry {
                sort_keys,
                payload,
                run_index: run_idx,
                directions: std::rc::Rc::clone(&self.directions),
            });
        }

        Ok(Some((entry.sort_keys, entry.payload)))
    }
}
