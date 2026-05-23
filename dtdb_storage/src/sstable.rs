use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::{DbKey, DbValue, Result, StorageError, CompressionType};

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
}

/// IndexBlock represents the index block at the end of the SSTable file, including metadata.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexBlock {
    pub entries: Vec<IndexEntry>,
    pub compression: CompressionType,
}

/// SstableWriter builds an SSTable file on disk.
///
/// It collects key-value pairs in a block buffer. When the buffer size
/// exceeds a threshold, it compresses the block using LZ4 and writes it to disk.
pub struct SstableWriter {
    file: File,
    offset: u64,
    block_size_limit: usize,
    current_block: Vec<(DbKey, Option<DbValue>)>,
    current_block_uncompressed_bytes: usize,
    index: Vec<IndexEntry>,
    compression: CompressionType,
}

impl SstableWriter {
    /// Creates a new SstableWriter writing to the file at the given path.
    pub fn create(path: impl AsRef<Path>, block_size_limit: usize, compression: CompressionType) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            offset: 0,
            block_size_limit,
            current_block: Vec::new(),
            current_block_uncompressed_bytes: 0,
            index: Vec::new(),
            compression,
        })
    }

    /// Appends a key-value entry (or a tombstone, if value is None) to the SSTable.
    /// Keys MUST be appended in strictly sorted order.
    pub fn append(&mut self, key: DbKey, value: Option<DbValue>) -> Result<()> {
        // Simple heuristic for estimated uncompressed size in memory.
        let key_sz = match &key {
            DbKey::Int(_) => 8,
            DbKey::String(s) => s.len(),
        };
        let val_sz = match &value {
            Some(DbValue::Int(_)) => 8,
            Some(DbValue::Float(_)) => 8,
            Some(DbValue::String(s)) => s.len(),
            Some(DbValue::Bytes(b)) => b.len(),
            None => 1,
        };

        if self.current_block.is_empty() {
            // First key of the block becomes the index key.
            self.index.push(IndexEntry {
                first_key: key.clone(),
                offset: self.offset,
                size: 0,
            });
        }

        self.current_block.push((key, value));
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

        // Serialize block
        let raw_bytes = bincode::serialize(&self.current_block)?;

        // Compress block if compression is Lz4
        let block_bytes = match self.compression {
            CompressionType::Lz4 => lz4_flex::compress_prepend_size(&raw_bytes),
            CompressionType::Uncompressed => raw_bytes,
        };
        let block_len = block_bytes.len() as u64;

        // Write block to file
        self.file.write_all(&block_bytes)?;

        // Update the last index entry with the correct size
        if let Some(entry) = self.index.last_mut() {
            entry.size = block_len;
        }

        self.offset += block_len;
        self.current_block.clear();
        self.current_block_uncompressed_bytes = 0;

        Ok(())
    }

    /// Flushes any pending blocks and writes the index block and footer to disk, finishing the file.
    pub fn finish(mut self) -> Result<()> {
        // Flush remaining elements in current block
        self.flush_block()?;

        // Write Index block (uncompressed for easy parsing during recovery/startup)
        let index_offset = self.offset;
        let index_block = IndexBlock {
            entries: self.index,
            compression: self.compression,
        };
        let index_bytes = bincode::serialize(&index_block)?;
        let index_len = index_bytes.len() as u64;

        self.file.write_all(&index_bytes)?;
        self.offset += index_len;

        // Write Footer: [index_offset (u64)][index_len (u64)][magic (8 bytes)]
        self.file.write_all(&index_offset.to_le_bytes())?;
        self.file.write_all(&index_len.to_le_bytes())?;
        self.file.write_all(MAGIC_NUMBER)?;

        self.file.sync_all()?;
        Ok(())
    }
}

/// SstableReader opens and performs lookups/scans on an SSTable file.
pub struct SstableReader {
    file: File,
    index: Vec<IndexEntry>,
    pub compression: CompressionType,
}

impl SstableReader {
    /// Opens the SSTable file at the given path and loads its index block.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < FOOTER_SIZE {
            return Err(StorageError::Corruption(
                "SSTable file size too small to contain footer".to_string()
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
                "Invalid SSTable magic number (file might not be a dtdb sstable)".to_string()
            ));
        }

        // 2. Read and deserialize the Index block.
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;

        let index_block: IndexBlock = match bincode::deserialize(&index_bytes) {
            Ok(ib) => ib,
            Err(_) => {
                // Fallback to old format: just Vec<IndexEntry> with Lz4 compression
                let entries: Vec<IndexEntry> = bincode::deserialize(&index_bytes)?;
                IndexBlock {
                    entries,
                    compression: CompressionType::Lz4,
                }
            }
        };

        Ok(Self {
            file,
            index: index_block.entries,
            compression: index_block.compression,
        })
    }

    /// Performs a point lookup for a key.
    ///
    /// Returns:
    /// - `Ok(Some(Some(value)))` if the key exists with a value.
    /// - `Ok(Some(None))` if the key exists as a tombstone (deleted).
    /// - `Ok(None)` if the key does not exist in this SSTable.
    pub fn get(&mut self, key: &DbKey) -> Result<Option<Option<DbValue>>> {
        if self.index.is_empty() {
            return Ok(None);
        }

        // 1. Binary search the index to locate the block containing the key.
        // We find the insertion point. The block that contains the key is the one
        // whose `first_key` is the largest key that is <= our search key.
        let block_idx = match self.index.binary_search_by(|entry| entry.first_key.cmp(key)) {
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

    /// Reads and decompresses a block by its index.
    fn read_block(&mut self, idx: usize) -> Result<Vec<(DbKey, Option<DbValue>)>> {
        let entry = &self.index[idx];
        self.file.seek(SeekFrom::Start(entry.offset))?;

        let mut block_bytes = vec![0u8; entry.size as usize];
        self.file.read_exact(&mut block_bytes)?;

        let raw_bytes = match self.compression {
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&block_bytes)
                .map_err(|e| StorageError::Compression(e.to_string()))?,
            CompressionType::Uncompressed => block_bytes,
        };

        let block: Vec<(DbKey, Option<DbValue>)> = bincode::deserialize(&raw_bytes)?;
        Ok(block)
    }

    /// Performs a range scan between `start` and `end` (both inclusive) matching the filter.
    pub fn scan<F>(&mut self, start: &DbKey, end: &DbKey, filter: F) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let mut results = Vec::new();
        if self.index.is_empty() {
            return Ok(results);
        }

        // 1. Locate the block where the range scan should start.
        let start_block_idx = match self.index.binary_search_by(|entry| entry.first_key.cmp(start)) {
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
            for (k, v) in block {
                if k >= *start && k <= *end {
                    if let Some(val) = v {
                        if filter(&k, &val) {
                            results.push((k, val));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Performs a raw range scan returning all entries (including tombstones) in the range.
    pub fn scan_raw(&mut self, start: &DbKey, end: &DbKey) -> Result<Vec<(DbKey, Option<DbValue>)>> {
        let mut results = Vec::new();
        if self.index.is_empty() {
            return Ok(results);
        }

        let start_block_idx = match self.index.binary_search_by(|entry| entry.first_key.cmp(start)) {
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
            for (k, v) in block {
                if k >= *start && k <= *end {
                    results.push((k, v));
                }
            }
        }

        Ok(results)
    }

    /// Reads all entries from the SSTable file. Used for compaction.
    pub fn read_all(&mut self) -> Result<Vec<(DbKey, Option<DbValue>)>> {
        let mut entries = Vec::new();
        for idx in 0..self.index.len() {
            let block = self.read_block(idx)?;
            entries.extend(block);
        }
        Ok(entries)
    }
}

