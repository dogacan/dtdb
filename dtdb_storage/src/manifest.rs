use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manifest {
    pub active_sstables: HashSet<(usize, u64)>, // (level, id)
}

impl Manifest {
    /// Loads the active SSTable manifest from disk.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)?;
        let manifest: Manifest = bincode::deserialize(&bytes)?;
        Ok(manifest)
    }

    /// Saves the active SSTable manifest atomically by writing to a temp file and renaming it.
    pub fn save(&self, path: &Path) -> Result<()> {
        let temp_path = path.with_extension("tmp");
        let bytes = bincode::serialize(self)?;
        {
            let mut file = File::create(&temp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&temp_path, path)?;
        Ok(())
    }
}
