use crate::{BlockCache, CompressionType, DbKey, DbValue, Result, StorageError};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

const MAGIC_NUMBER: &[u8; 8] = b"DTDB_SST";
const FOOTER_SIZE: u64 = 24; // 8 bytes index_offset + 8 bytes index_len + 8 bytes magic number

/// IndexEntry represents metadata for a compressed block in the SSTable file.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexEntry {
    /// The first key in this block, used to decide if a search key belongs in this block.
    pub first_key: DbKey,
    /// Byte offset in the file where the block starts.
    pub offset: u64,
    /// Compressed size of the block in bytes.
    pub size: u64,
    /// xxh32 checksum over the on-disk (compressed) block bytes. Zero means
    /// "no checksum recorded" — used to remain readable against SSTables
    /// produced by older builds that didn't store one.
    #[serde(default)]
    pub checksum: u32,
}

/// StatsBlock tracks statistics for the keys and values written to this SSTable.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatsBlock {
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub total_uncompressed_bytes: u64,
    pub min_key: Option<DbKey>,
    pub max_key: Option<DbKey>,
}

/// IndexBlock represents the index block at the end of the SSTable file, including metadata.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexBlock {
    pub entries: Vec<IndexEntry>,
    pub compression: CompressionType,
    pub stats: StatsBlock,
    #[serde(default)]
    pub bloom_filter: Option<crate::bloom::BloomFilter>,
    pub layout: Vec<u8>,
}

/// SstableWriter builds an SSTable file on disk.
///
/// It collects key-value pairs in a block buffer. When the buffer size
/// exceeds a threshold, it compresses the block using LZ4 and writes it to disk.
pub struct SstableWriter {
    file: Option<File>,
    offset: u64,
    block_size_limit: usize,
    current_block: Vec<(DbKey, Option<DbValue>)>,
    current_block_uncompressed_bytes: usize,
    index: Vec<IndexEntry>,
    compression: CompressionType,
    final_path: std::path::PathBuf,
    temp_path: std::path::PathBuf,
    num_entries: u64,
    num_tombstones: u64,
    total_uncompressed_bytes: u64,
    min_key: Option<DbKey>,
    max_key: Option<DbKey>,
    bloom_filter: Option<crate::bloom::BloomFilter>,
    layout: Vec<u8>,
}

impl SstableWriter {
    /// Creates a new SstableWriter writing to the file at the given path.
    pub fn create(
        path: impl AsRef<Path>,
        block_size_limit: usize,
        compression: CompressionType,
        expected_entries: usize,
        layout: Vec<u8>,
    ) -> Result<Self> {
        let final_path = path.as_ref().to_path_buf();
        let mut temp_path = final_path.clone();
        if let Some(ext) = temp_path.extension() {
            let mut new_ext = ext.to_os_string();
            new_ext.push(".tmp");
            temp_path.set_extension(new_ext);
        } else {
            temp_path.set_extension("tmp");
        }

        let file = File::create(&temp_path)?;
        let bloom_filter = if expected_entries > 0 {
            Some(crate::bloom::BloomFilter::new(expected_entries, 0.01))
        } else {
            None
        };

        Ok(Self {
            file: Some(file),
            offset: 0,
            block_size_limit,
            current_block: Vec::new(),
            current_block_uncompressed_bytes: 0,
            index: Vec::new(),
            compression,
            final_path,
            temp_path,
            num_entries: 0,
            num_tombstones: 0,
            total_uncompressed_bytes: 0,
            min_key: None,
            max_key: None,
            bloom_filter,
            layout,
        })
    }

    /// Appends a key-value entry (or a tombstone, if value is None) to the SSTable.
    /// Keys MUST be appended in strictly sorted order.
    pub fn append(&mut self, key: &DbKey, value: Option<&DbValue>) -> Result<()> {
        // Simple heuristic for estimated uncompressed size in memory.
        let key_sz = key.byte_size();
        let val_sz = match &value {
            Some(DbValue::Int(_)) => 8,
            Some(DbValue::Float(_)) => 8,
            Some(DbValue::String(s)) => s.len(),
            Some(DbValue::Bytes(b)) => b.len(),
            Some(DbValue::Null) => 1,
            Some(DbValue::Bool(_)) => 1,
            None => 1,
        };

        // Update stats
        if let Some(ref mut filter) = self.bloom_filter {
            filter.insert(key);
        }
        self.num_entries += 1;
        if value.is_none() {
            self.num_tombstones += 1;
        }
        self.total_uncompressed_bytes += (key_sz + val_sz) as u64;
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }

        if self.current_block.is_empty() {
            // First key of the block becomes the index key.
            self.index.push(IndexEntry {
                first_key: key.clone(),
                offset: self.offset,
                size: 0,
                checksum: 0,
            });
        }

        self.current_block.push((key.clone(), value.cloned()));
        self.current_block_uncompressed_bytes += key_sz + val_sz;

        // Flush block to disk if it exceeds the limit.
        if self.current_block_uncompressed_bytes >= self.block_size_limit {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Compresses and writes the current block buffer to disk.
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        self.max_key = Some(self.current_block.last().unwrap().0.clone());

        // Serialize block
        let raw_bytes = bincode::serialize(&self.current_block)?;

        // Compress block if compression is Lz4
        let block_bytes = match self.compression {
            CompressionType::Lz4 => lz4_flex::compress_prepend_size(&raw_bytes),
            CompressionType::Uncompressed => raw_bytes,
        };
        let block_len = block_bytes.len() as u64;

        // Compute checksum over the bytes that hit disk so a corrupted block
        // is caught after decompression-time errors miss subtle bitrot.
        let block_checksum = xxhash_rust::xxh32::xxh32(&block_bytes, 0);

        // Write block to file
        self.file.as_mut().unwrap().write_all(&block_bytes)?;

        // Update the last index entry with the correct size and checksum
        if let Some(entry) = self.index.last_mut() {
            entry.size = block_len;
            entry.checksum = block_checksum;
        }

        self.offset += block_len;
        self.current_block.clear();
        self.current_block_uncompressed_bytes = 0;

        Ok(())
    }

    /// Flushes any pending blocks and writes the index block and footer to disk, finishing the file.
    pub fn finish(mut self) -> Result<()> {
        let res = self.finish_internal();
        if res.is_err() {
            let _ = std::fs::remove_file(&self.temp_path);
        }
        res
    }

    fn finish_internal(&mut self) -> Result<()> {
        // Flush remaining elements in current block
        self.flush_block()?;

        // Take the file out of Option to close it after writing
        let mut file = self.file.take().ok_or_else(|| {
            StorageError::Corruption("SSTable writer already finished".to_string())
        })?;

        // Write Index block (uncompressed for easy parsing during recovery/startup)
        let index_offset = self.offset;
        let stats = StatsBlock {
            num_entries: self.num_entries,
            num_tombstones: self.num_tombstones,
            total_uncompressed_bytes: self.total_uncompressed_bytes,
            min_key: self.min_key.clone(),
            max_key: self.max_key.clone(),
        };
        let index_block = IndexBlock {
            entries: self.index.clone(),
            compression: self.compression,
            stats,
            bloom_filter: self.bloom_filter.clone(),
            layout: self.layout.clone(),
        };
        let index_bytes = bincode::serialize(&index_block)?;
        let index_len = index_bytes.len() as u64;

        file.write_all(&index_bytes)?;
        self.offset += index_len;

        // Write Footer: [index_offset (u64)][index_len (u64)][magic (8 bytes)]
        file.write_all(&index_offset.to_le_bytes())?;
        file.write_all(&index_len.to_le_bytes())?;
        file.write_all(MAGIC_NUMBER)?;

        file.sync_all()?;
        drop(file);

        // Rename the temporary file atomically to final path
        std::fs::rename(&self.temp_path, &self.final_path)?;
        crate::fsync_parent_dir(&self.final_path)?;

        Ok(())
    }
}

/// SstableReader opens and performs lookups/scans on an SSTable file.
pub struct SstableReader {
    file: File,
    pub(crate) index: Vec<IndexEntry>,
    pub compression: CompressionType,
    pub id: u64,
    pub level: usize,
    pub path: std::path::PathBuf,
    pub last_key: DbKey,
    block_cache: Option<Arc<BlockCache>>,
    pub stats: StatsBlock,
    pub bloom_filter: Option<crate::bloom::BloomFilter>,
    pub file_size: u64,
    pub layout: Vec<u8>,
}

impl SstableReader {
    /// Opens the SSTable file at the given path and loads its index block.
    pub fn open(
        path: impl AsRef<Path>,
        id: u64,
        level: usize,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = File::open(&path_buf)?;
        let file_len = file.metadata()?.len();

        if file_len < FOOTER_SIZE {
            return Err(StorageError::Corruption(
                "SSTable file size too small to contain footer".to_string(),
            ));
        }

        // 1. Read the Footer at the end of the file.
        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE))?;

        let mut footer_bytes = [0u8; 24];
        file.read_exact(&mut footer_bytes)?;

        let index_offset = u64::from_le_bytes(footer_bytes[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer_bytes[8..16].try_into().unwrap());
        let magic = &footer_bytes[16..24];

        if magic != MAGIC_NUMBER {
            return Err(StorageError::Corruption(
                "Invalid SSTable magic number (file might not be a dtdb sstable)".to_string(),
            ));
        }

        // Bound-check the index slice against the file. A corrupted footer
        // can otherwise drive a multi-exabyte allocation below.
        let footer_start = file_len - FOOTER_SIZE;
        if index_offset > footer_start || index_len > footer_start - index_offset {
            return Err(StorageError::Corruption(format!(
                "SSTable footer points outside file (file_len={file_len}, index_offset={index_offset}, index_len={index_len})"
            )));
        }

        // 2. Read and deserialize the Index block.
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;

        let index_block: IndexBlock = bincode::deserialize(&index_bytes)?;

        // The index stats record the max key for every non-empty SSTable, so we
        // take `last_key` straight from there and never touch a data block at
        // open time. A non-empty index with no recorded max key is corruption.
        let last_key = if index_block.entries.is_empty() {
            DbKey::Int(0) // Placeholder; an empty SSTable has no keys to report.
        } else {
            index_block.stats.max_key.clone().ok_or_else(|| {
                StorageError::Corruption(
                    "SSTable index is non-empty but stats.max_key is missing".to_string(),
                )
            })?
        };

        let reader = Self {
            file,
            index: index_block.entries,
            compression: index_block.compression,
            id,
            level,
            path: path_buf,
            last_key,
            block_cache,
            stats: index_block.stats,
            bloom_filter: index_block.bloom_filter,
            file_size: file_len,
            layout: index_block.layout,
        };

        Ok(reader)
    }

    pub fn first_key(&self) -> Option<&DbKey> {
        self.index.first().map(|e| &e.first_key)
    }

    pub fn last_key(&self) -> &DbKey {
        &self.last_key
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Performs a point lookup for a key.
    ///
    /// Returns:
    /// - `Ok(Some(Some(value)))` if the key exists with a value.
    /// - `Ok(Some(None))` if the key exists as a tombstone (deleted).
    /// - `Ok(None)` if the key does not exist in this SSTable.
    pub fn get(&self, key: &DbKey) -> Result<Option<Option<DbValue>>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        if let Some(ref bloom) = self.bloom_filter
            && !bloom.contains(key)
        {
            return Ok(None);
        }

        // 1. Binary search the index to locate the block containing the key.
        // We find the insertion point. The block that contains the key is the one
        // whose `first_key` is the largest key that is <= our search key.
        let block_idx = match self
            .index
            .binary_search_by(|entry| entry.first_key.cmp(key))
        {
            Ok(idx) => idx, // Exact match on first key of block.
            Err(idx) => {
                if idx == 0 {
                    // The key is smaller than the first key of the first block.
                    return Ok(None);
                }
                idx - 1
            }
        };

        // 2. Read and decompress the block.
        let block = self.read_block(block_idx)?;

        // 3. Search for the key inside the sorted block.
        match block.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(idx) => Ok(Some(block[idx].1.clone())),
            Err(_) => Ok(None),
        }
    }

    /// Reads and decompresses a block by its index, utilizing the block cache if present.
    #[allow(clippy::type_complexity)]
    pub(crate) fn read_block(&self, idx: usize) -> Result<Arc<Vec<(DbKey, Option<DbValue>)>>> {
        if let Some(ref cache) = self.block_cache {
            let key = (self.id, idx);
            if let Some(block) = cache.get(&key) {
                return Ok(block);
            }
        }

        crate::record_physical_block_read();

        let entry = &self.index[idx];
        // Defensively bound entry.size against the file. A corrupted index
        // would otherwise drive an unbounded allocation here.
        if entry.offset > self.file_size || entry.size > self.file_size - entry.offset {
            return Err(StorageError::Corruption(format!(
                "SSTable block extent outside file (sst id={}, block {}, offset {}, size {}, file_size {})",
                self.id, idx, entry.offset, entry.size, self.file_size
            )));
        }
        let mut block_bytes = vec![0u8; entry.size as usize];
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(&mut block_bytes, entry.offset)?;

        // Verify the on-disk block checksum before decompression. A zero
        // checksum means the block was written by a build that pre-dates
        // per-block checksums; skip verification rather than reject the file.
        if entry.checksum != 0 {
            let actual = xxhash_rust::xxh32::xxh32(&block_bytes, 0);
            if actual != entry.checksum {
                return Err(StorageError::Corruption(format!(
                    "SSTable block checksum mismatch (sst id={}, block {}, expected {:#010x}, actual {:#010x})",
                    self.id, idx, entry.checksum, actual
                )));
            }
        }

        let raw_bytes = match self.compression {
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&block_bytes)
                .map_err(|e| StorageError::Compression(e.to_string()))?,
            CompressionType::Uncompressed => block_bytes,
        };

        let block: Vec<(DbKey, Option<DbValue>)> = bincode::deserialize(&raw_bytes)?;
        let block_arc = Arc::new(block);

        if let Some(ref cache) = self.block_cache {
            let key = (self.id, idx);
            cache.insert(key, block_arc.clone());
        }

        Ok(block_arc)
    }

    /// Performs a range scan between `start` and `end` (both inclusive) matching the filter.
    pub fn scan<F>(&self, start: &DbKey, end: &DbKey, filter: F) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let mut results = Vec::new();
        if self.index.is_empty() {
            return Ok(results);
        }

        // 1. Locate the block where the range scan should start.
        let start_block_idx = match self
            .index
            .binary_search_by(|entry| entry.first_key.cmp(start))
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    0
                } else {
                    idx - 1
                }
            }
        };

        // 2. Scan sequentially through blocks until the block first_key exceeds `end`.
        for block_idx in start_block_idx..self.index.len() {
            let entry = &self.index[block_idx];
            if entry.first_key > *end {
                break; // No subsequent blocks can contain keys in our range.
            }

            let block = self.read_block(block_idx)?;
            for (k, v) in block.iter() {
                if k >= start
                    && k <= end
                    && let Some(val) = v
                    && filter(k, val)
                {
                    results.push((k.clone(), val.clone()));
                }
            }
        }

        Ok(results)
    }

    /// Performs a raw range scan returning all entries (including tombstones) in the range.
    pub fn scan_raw(&self, start: &DbKey, end: &DbKey) -> Result<Vec<(DbKey, Option<DbValue>)>> {
        let mut results = Vec::new();
        if self.index.is_empty() {
            return Ok(results);
        }

        let start_block_idx = match self
            .index
            .binary_search_by(|entry| entry.first_key.cmp(start))
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    0
                } else {
                    idx - 1
                }
            }
        };

        for block_idx in start_block_idx..self.index.len() {
            let entry = &self.index[block_idx];
            if entry.first_key > *end {
                break;
            }

            let block = self.read_block(block_idx)?;
            for (k, v) in block.iter() {
                if k >= start && k <= end {
                    results.push((k.clone(), v.clone()));
                }
            }
        }

        Ok(results)
    }

    /// Reads all entries from the SSTable file. Used for compaction.
    pub fn read_all(&self) -> Result<Vec<(DbKey, Option<DbValue>)>> {
        let mut entries = Vec::new();
        for idx in 0..self.index.len() {
            let block = self.read_block(idx)?;
            entries.extend(block.iter().cloned());
        }
        Ok(entries)
    }
}
