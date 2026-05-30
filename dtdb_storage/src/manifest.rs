use crate::framed_log::LogFormat;
use crate::snapshot_log::Snapshotable;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// On-disk format tag for the manifest's edit log (see [`crate::SnapshotLog`]).
/// The magic distinguishes it from other framed logs (WAL, transaction log).
pub const MANIFEST_LOG_FORMAT: LogFormat = LogFormat {
    magic: *b"DMAN",
    version: 1,
};

/// The set of SSTables currently live in a storage engine, keyed by
/// `(level, id)`. Persisted as a [`crate::SnapshotLog`]: a base snapshot of the
/// set plus an append-only log of add/remove edits, rather than a full rewrite
/// on every flush and compaction.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Manifest {
    pub active_sstables: HashSet<(usize, u64)>, // (level, id)
}

/// An incremental change to the active-SSTable set.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ManifestEdit {
    AddSstable { level: usize, id: u64 },
    RemoveSstable { level: usize, id: u64 },
}

impl Snapshotable for Manifest {
    type Edit = ManifestEdit;

    fn apply(&mut self, edit: &ManifestEdit) {
        match edit {
            ManifestEdit::AddSstable { level, id } => {
                self.active_sstables.insert((*level, *id));
            }
            ManifestEdit::RemoveSstable { level, id } => {
                self.active_sstables.remove(&(*level, *id));
            }
        }
    }
}
