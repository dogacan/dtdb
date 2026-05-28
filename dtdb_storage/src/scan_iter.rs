use crate::merge_iter::SstableBlockIterator;
use crate::{DbKey, DbValue, Result};
use std::collections::{BinaryHeap, HashSet};

#[derive(Debug)]
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
        // Min-heap order: smaller keys first, then smaller priority (newer) first
        match other.key.cmp(&self.key) {
            std::cmp::Ordering::Equal => other.priority.cmp(&self.priority),
            ord => ord,
        }
    }
}

/// An owned streaming iterator that merges the memtable range with SSTable block iterators.
///
/// Implements Option (A) — Owned iterator.
/// The iterator takes Arc references/cloned lists and snapshots the memtable data into a sorted
/// vector on construction (only the range, not the full memtable), avoiding lifetime parameters
/// on the iterator and simplifying the code while still avoiding full database materialization.
pub struct ScanIterator {
    mem_entries: Vec<(DbKey, Option<DbValue>)>,
    mem_idx: usize,
    sst_iters: Vec<SstableBlockIterator>,
    heap: BinaryHeap<HeapEntry>,
    seen: HashSet<DbKey>,
    end: DbKey,
}

impl ScanIterator {
    pub fn new(
        mem_entries: Vec<(DbKey, Option<DbValue>)>,
        sst_iters: Vec<SstableBlockIterator>,
        end: DbKey,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in sst_iters.iter().enumerate() {
            if let Some((k, v)) = iter.peek() {
                heap.push(HeapEntry {
                    key: k.clone(),
                    value: v.clone(),
                    source_idx: idx,
                    priority: iter.priority,
                });
            }
        }
        Ok(Self {
            mem_entries,
            mem_idx: 0,
            sst_iters,
            heap,
            seen: HashSet::new(),
            end,
        })
    }

    /// Advances the iterator and returns the next (key, value) pair.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(DbKey, DbValue)>> {
        loop {
            let has_mem = self.mem_idx < self.mem_entries.len();
            let has_heap = !self.heap.is_empty();

            if !has_mem && !has_heap {
                return Ok(None);
            }

            let choose_mem = if has_mem && has_heap {
                let mem_key = &self.mem_entries[self.mem_idx].0;
                let heap_key = &self.heap.peek().unwrap().key;
                mem_key <= heap_key
            } else {
                has_mem
            };

            if choose_mem {
                let (k, v) = &self.mem_entries[self.mem_idx];
                self.mem_idx += 1;

                if k > &self.end {
                    return Ok(None);
                }

                if self.seen.insert(k.clone())
                    && let Some(val) = v
                {
                    return Ok(Some((k.clone(), val.clone())));
                }
            } else {
                let entry = self.heap.pop().unwrap();

                // If we've passed `end` for this source, drop it entirely
                // instead of draining the rest of the SSTable.
                if entry.key > self.end {
                    continue;
                }

                let source = &mut self.sst_iters[entry.source_idx];
                source.advance()?;
                if let Some((next_k, next_v)) = source.peek() {
                    self.heap.push(HeapEntry {
                        key: next_k.clone(),
                        value: next_v.clone(),
                        source_idx: entry.source_idx,
                        priority: source.priority,
                    });
                }

                if self.seen.insert(entry.key.clone())
                    && let Some(val) = entry.value
                {
                    return Ok(Some((entry.key, val)));
                }
            }
        }
    }
}
