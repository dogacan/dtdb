//! State maintained as a base snapshot plus a replayed log of edits.
//!
//! This is the "Layer 2" persistence primitive from ADR 0001
//! (`docs/adr/0001-unified-metadata-persistence.md`): a piece of state that is
//! cheap to mutate (append one small edit) but periodically compacted into a
//! fresh snapshot so the log can't grow without bound. It is the
//! checkpoint+redo-log / snapshot+delta pattern, modeled on RocksDB's
//! MANIFEST/CURRENT. The manifest is its one client.
//!
//! ## On-disk layout
//!
//! A [`SnapshotLog`] owns a directory containing:
//!
//! ```text
//!   CURRENT          -> decimal generation number of the live snapshot/log
//!   snapshot.<gen>   -> postcard of the full state at generation <gen>
//!   log.<gen>        -> FramedLog of edit batches applied since snapshot.<gen>
//! ```
//!
//! `CURRENT` is written with [`crate::atomic_write`], so flipping it is the
//! single atomic commit point of a compaction. Because the snapshot and log are
//! versioned together and only become live once `CURRENT` names their
//! generation, recovery can never replay a log against a snapshot that already
//! contains its edits — the crash window that a truncate-in-place design would
//! have.

use crate::framed_log::{FramedLog, LogFormat};
use crate::{FsyncMethod, Result, StorageError, atomic_write};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};

/// State that can be rebuilt from a base snapshot plus a stream of edits.
///
/// The snapshot is the serialized state itself; an edit is an incremental
/// mutation that [`Snapshotable::apply`] folds into the in-memory state.
pub trait Snapshotable: Default + Serialize + DeserializeOwned {
    type Edit: Serialize + DeserializeOwned;
    fn apply(&mut self, edit: &Self::Edit);
}

/// A piece of [`Snapshotable`] state persisted as `snapshot + edit log`,
/// compacted when the log grows past a threshold.
pub struct SnapshotLog<S: Snapshotable> {
    dir: PathBuf,
    state: S,
    /// Each frame is a *batch* of edits, so a logical update touching several
    /// edits costs a single append (and a single fsync).
    log: FramedLog<Vec<S::Edit>>,
    generation: u64,
    log_format: LogFormat,
    fsync_method: FsyncMethod,
    compact_threshold_bytes: u64,
}

impl<S: Snapshotable> SnapshotLog<S> {
    /// Opens the snapshot log rooted at `dir`, creating an empty one if none
    /// exists. `log_format` tags the edit log's file header; `compact_threshold_bytes`
    /// is the edit-log size past which [`SnapshotLog::append`] auto-compacts.
    pub fn open(
        dir: impl AsRef<Path>,
        log_format: LogFormat,
        fsync_method: FsyncMethod,
        compact_threshold_bytes: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let current_path = dir.join("CURRENT");

        if current_path.exists() {
            let generation = read_current(&current_path)?;

            // Load the base snapshot, then replay the edit batches on top.
            let snap_bytes = fs::read(snapshot_path(&dir, generation))?;
            let mut state: S = postcard::from_bytes(&snap_bytes)?;
            let batches =
                FramedLog::<Vec<S::Edit>>::recover(log_path(&dir, generation), log_format)?;
            for batch in &batches {
                for edit in batch {
                    state.apply(edit);
                }
            }

            // Treat opening as a compaction (generation rollover) to ensure that the
            // old generation log file (which has been closed) is never opened for write again.
            let new_gen = generation + 1;
            atomic_write(
                &snapshot_path(&dir, new_gen),
                &postcard::to_allocvec(&state)?,
                fsync_method,
            )?;
            let log = FramedLog::open(log_path(&dir, new_gen), log_format, None, fsync_method)?;
            write_current(&current_path, new_gen, fsync_method)?;

            let this = Self {
                dir,
                state,
                log,
                generation: new_gen,
                log_format,
                fsync_method,
                compact_threshold_bytes,
            };
            this.remove_stale_generations()?;
            Ok(this)
        } else {
            // Fresh: write snapshot.0 of the default state, an empty log.0, then
            // publish generation 0 via CURRENT (the atomic commit point).
            let generation = 0;
            let state = S::default();
            atomic_write(
                &snapshot_path(&dir, generation),
                &postcard::to_allocvec(&state)?,
                fsync_method,
            )?;
            let log = FramedLog::open(log_path(&dir, generation), log_format, None, fsync_method)?;
            write_current(&current_path, generation, fsync_method)?;
            Ok(Self {
                dir,
                state,
                log,
                generation,
                log_format,
                fsync_method,
                compact_threshold_bytes,
            })
        }
    }

    /// The current in-memory state.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Applies and durably records a single edit.
    pub fn append(&mut self, edit: S::Edit) -> Result<()> {
        self.append_batch(vec![edit])
    }

    /// Applies and durably records several edits as one atomic batch (one
    /// append, one fsync). Auto-compacts if the edit log has grown past the
    /// configured threshold.
    pub fn append_batch(&mut self, edits: Vec<S::Edit>) -> Result<()> {
        for edit in &edits {
            self.state.apply(edit);
        }
        self.log.append(&edits)?;
        if self.log.size() >= self.compact_threshold_bytes {
            self.compact()?;
        }
        Ok(())
    }

    /// Writes a fresh snapshot of the current state and starts an empty log,
    /// publishing the new generation atomically via `CURRENT`.
    pub fn compact(&mut self) -> Result<()> {
        let new_gen = self.generation + 1;

        // 1. Durably write the new snapshot and an empty log for it.
        atomic_write(
            &snapshot_path(&self.dir, new_gen),
            &postcard::to_allocvec(&self.state)?,
            self.fsync_method,
        )?;
        let new_log = FramedLog::open(
            log_path(&self.dir, new_gen),
            self.log_format,
            None,
            self.fsync_method,
        )?;

        // 2. The atomic commit point: CURRENT now names the new generation.
        write_current(&self.dir.join("CURRENT"), new_gen, self.fsync_method)?;

        // 3. Past this point the old generation is unreferenced; drop it.
        let old_gen = self.generation;
        self.generation = new_gen;
        self.log = new_log;
        let _ = fs::remove_file(snapshot_path(&self.dir, old_gen));
        let _ = fs::remove_file(log_path(&self.dir, old_gen));
        Ok(())
    }

    /// Removes snapshot/log files left behind by a compaction that crashed
    /// before (or after) the `CURRENT` flip, keeping only the live generation.
    fn remove_stale_generations(&self) -> Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let parsed_gen = name
                .strip_prefix("snapshot.")
                .or_else(|| name.strip_prefix("log."))
                .and_then(|g| g.parse::<u64>().ok());
            if let Some(parsed_gen) = parsed_gen
                && parsed_gen != self.generation
            {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(())
    }
}

fn snapshot_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("snapshot.{generation}"))
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("log.{generation}"))
}

fn read_current(path: &Path) -> Result<u64> {
    let text = fs::read_to_string(path)?;
    text.trim()
        .parse::<u64>()
        .map_err(|e| StorageError::Corruption(format!("invalid CURRENT generation {text:?}: {e}")))
}

fn write_current(path: &Path, generation: u64, fsync_method: FsyncMethod) -> Result<()> {
    atomic_write(path, generation.to_string().as_bytes(), fsync_method)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashSet;

    const TEST_FORMAT: LogFormat = LogFormat {
        magic: *b"DSTL",
        version: 1,
    };

    #[derive(Default, Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct IntSet {
        values: HashSet<i64>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    enum SetEdit {
        Add(i64),
        Remove(i64),
    }

    impl Snapshotable for IntSet {
        type Edit = SetEdit;
        fn apply(&mut self, edit: &SetEdit) {
            match edit {
                SetEdit::Add(v) => {
                    self.values.insert(*v);
                }
                SetEdit::Remove(v) => {
                    self.values.remove(v);
                }
            }
        }
    }

    /// A high threshold so auto-compaction doesn't fire mid-test.
    const NO_AUTO_COMPACT: u64 = u64::MAX;

    fn open(dir: &Path, threshold: u64) -> SnapshotLog<IntSet> {
        SnapshotLog::open(dir, TEST_FORMAT, FsyncMethod::Fullfsync, threshold).unwrap()
    }

    #[test]
    fn test_append_then_reopen_replays_edits() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("snap");
        {
            let mut log = open(&dir, NO_AUTO_COMPACT);
            log.append(SetEdit::Add(1)).unwrap();
            log.append(SetEdit::Add(2)).unwrap();
            log.append(SetEdit::Remove(1)).unwrap();
            assert_eq!(log.state().values, HashSet::from([2]));
        }
        let log = open(&dir, NO_AUTO_COMPACT);
        assert_eq!(log.state().values, HashSet::from([2]));
    }

    #[test]
    fn test_append_batch_is_atomic_and_replayed() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("snap");
        {
            let mut log = open(&dir, NO_AUTO_COMPACT);
            log.append_batch(vec![SetEdit::Add(1), SetEdit::Add(2), SetEdit::Add(3)])
                .unwrap();
        }
        let log = open(&dir, NO_AUTO_COMPACT);
        assert_eq!(log.state().values, HashSet::from([1, 2, 3]));
    }

    #[test]
    fn test_compaction_preserves_state_and_advances_generation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("snap");
        let mut log = open(&dir, NO_AUTO_COMPACT);
        log.append(SetEdit::Add(10)).unwrap();
        log.append(SetEdit::Add(20)).unwrap();
        assert_eq!(log.generation, 0);

        log.compact().unwrap();
        assert_eq!(log.generation, 1);
        assert_eq!(log.state().values, HashSet::from([10, 20]));

        // The old generation's files are gone; the new ones are present.
        assert!(!snapshot_path(&dir, 0).exists());
        assert!(!log_path(&dir, 0).exists());
        assert!(snapshot_path(&dir, 1).exists());

        // Further edits land on the new log and survive a reopen.
        log.append(SetEdit::Remove(10)).unwrap();
        drop(log);
        let reopened = open(&dir, NO_AUTO_COMPACT);
        assert_eq!(reopened.generation, 2);
        assert_eq!(reopened.state().values, HashSet::from([20]));
    }

    #[test]
    fn test_auto_compaction_fires_past_threshold() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("snap");
        // Tiny threshold so a couple of appends trigger compaction.
        let mut log = open(&dir, 16);
        for i in 0..50 {
            log.append(SetEdit::Add(i)).unwrap();
        }
        assert!(
            log.generation > 0,
            "auto-compaction should have advanced the generation"
        );
        let expected: HashSet<i64> = (0..50).collect();
        assert_eq!(log.state().values, expected);

        drop(log);
        let reopened = open(&dir, 16);
        assert_eq!(reopened.state().values, expected);
        // Only the live generation's files remain.
        let snapshots = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().unwrap().starts_with("snapshot."))
            .count();
        assert_eq!(snapshots, 1, "stale snapshots should be swept on reopen");
    }
}
