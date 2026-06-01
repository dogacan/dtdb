use crate::sstable::SstableReader;
use crate::{DbKey, DbValue, Result};
use std::collections::BinaryHeap;
use std::sync::Arc;

/// A lazy iterator over an SSTable that reads one block at a time.
pub struct SstableBlockIterator {
    reader: Arc<SstableReader>,
    current_block_idx: usize,
    current_entries: Arc<Vec<(DbKey, Option<DbValue>)>>,
    entry_idx: usize,
    end: Option<DbKey>,
    exhausted: bool,
    pub priority: usize, // lower = higher priority (newest = 0)
}

impl SstableBlockIterator {
    pub fn reader(&self) -> &Arc<SstableReader> {
        &self.reader
    }

    pub fn new(reader: Arc<SstableReader>, start: Option<&DbKey>, priority: usize) -> Result<Self> {
        Self::new_with_end(reader, start, None, priority)
    }

    /// Creates a new block iterator restricted to the inclusive range `[start, end]`.
    /// When `end` is provided, the iterator avoids loading any block whose first key
    /// is beyond `end`, and stops yielding entries once a key past `end` is reached.
    pub fn new_with_end(
        reader: Arc<SstableReader>,
        start: Option<&DbKey>,
        end: Option<&DbKey>,
        priority: usize,
    ) -> Result<Self> {
        let mut current_block_idx = 0;
        let mut entry_idx = 0;
        let mut current_entries = Arc::new(Vec::new());

        if !reader.index.is_empty() {
            if let Some(start_key) = start {
                current_block_idx = match reader
                    .index
                    .binary_search_by(|entry| entry.first_key.cmp(start_key))
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
                current_entries = reader.read_block(current_block_idx)?;
                entry_idx = match current_entries.binary_search_by(|(k, _)| k.cmp(start_key)) {
                    Ok(idx) => idx,
                    Err(idx) => idx,
                };
            } else {
                current_entries = reader.read_block(0)?;
            }
        }

        let mut it = Self {
            reader,
            current_block_idx,
            current_entries,
            entry_idx,
            end: end.cloned(),
            exhausted: false,
            priority,
        };

        if !it.reader.index.is_empty() && it.entry_idx >= it.current_entries.len() {
            it.advance()?;
        }
        it.check_end_bound();

        Ok(it)
    }

    fn check_end_bound(&mut self) {
        if self.exhausted {
            return;
        }
        if let (Some(end), Some((k, _))) = (&self.end, self.current_entries.get(self.entry_idx))
            && k > end
        {
            self.exhausted = true;
        }
    }

    /// Returns the current (key, value) without advancing, or None if exhausted.
    pub fn peek(&self) -> Option<&(DbKey, Option<DbValue>)> {
        if self.exhausted {
            None
        } else {
            self.current_entries.get(self.entry_idx)
        }
    }

    /// Advances to the next entry, loading the next block if needed.
    pub fn advance(&mut self) -> Result<()> {
        if self.exhausted {
            return Ok(());
        }
        self.entry_idx += 1;
        while self.entry_idx >= self.current_entries.len() {
            self.current_block_idx += 1;
            if self.current_block_idx < self.reader.index.len() {
                // Skip loading blocks beyond the requested end bound entirely.
                if let Some(end) = &self.end
                    && &self.reader.index[self.current_block_idx].first_key > end
                {
                    self.exhausted = true;
                    return Ok(());
                }
                self.current_entries = self.reader.read_block(self.current_block_idx)?;
                self.entry_idx = 0;
            } else {
                break;
            }
        }
        self.check_end_bound();
        Ok(())
    }
}

struct HeapEntry {
    key: DbKey,
    value: Option<DbValue>,
    source_idx: usize,
    priority: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.priority == other.priority
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap order:
        // 1. Smaller keys have higher priority (come first)
        match other.key.cmp(&self.key) {
            std::cmp::Ordering::Equal => {
                // 2. Smaller priority number (newer) comes first
                other.priority.cmp(&self.priority)
            }
            ord => ord,
        }
    }
}

/// A k-way merge iterator using a BinaryHeap.
pub struct MergeIterator {
    sources: Vec<SstableBlockIterator>,
    heap: BinaryHeap<HeapEntry>,
    last_key: Option<DbKey>,
}

impl MergeIterator {
    pub fn new(sources: Vec<SstableBlockIterator>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        for (idx, source) in sources.iter().enumerate() {
            if let Some((k, v)) = source.peek() {
                heap.push(HeapEntry {
                    key: k.clone(),
                    value: v.clone(),
                    source_idx: idx,
                    priority: source.priority,
                });
            }
        }
        Ok(Self {
            sources,
            heap,
            last_key: None,
        })
    }

    pub fn get_reader(&self, idx: usize) -> &Arc<SstableReader> {
        &self.sources[idx].reader
    }

    /// Pulls the next unique entry from the merged streams.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(DbKey, Option<DbValue>, usize)>> {
        while let Some(entry) = self.heap.pop() {
            let source = &mut self.sources[entry.source_idx];
            source.advance()?;
            if let Some((next_k, next_v)) = source.peek() {
                self.heap.push(HeapEntry {
                    key: next_k.clone(),
                    value: next_v.clone(),
                    source_idx: entry.source_idx,
                    priority: source.priority,
                });
            }

            if let Some(ref last) = self.last_key
                && &entry.key == last
            {
                continue;
            }

            self.last_key = Some(entry.key.clone());
            return Ok(Some((entry.key, entry.value, entry.source_idx)));
        }
        Ok(None)
    }
}
