use crate::merge_iter::SstableBlockIterator;
use crate::{DbKey, DbValue, Result};
use std::collections::BinaryHeap;

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

use std::sync::Arc;

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
    // The merge below always emits keys in non-decreasing order (it picks the
    // smallest of the memtable cursor and the heap top each step), so every
    // copy of a given key is contiguous. Tracking only the last key consumed
    // is therefore enough to drop older duplicates — no need for an O(range)
    // `HashSet` that retains the whole scan in memory.
    last_key: Option<DbKey>,
    end: DbKey,
    rewriter: Arc<dyn crate::ValueRewriter>,
    target_layout: Vec<u8>,
}

impl ScanIterator {
    pub fn new(
        mem_entries: Vec<(DbKey, Option<DbValue>)>,
        sst_iters: Vec<SstableBlockIterator>,
        end: DbKey,
        rewriter: Arc<dyn crate::ValueRewriter>,
        target_layout: Vec<u8>,
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
            last_key: None,
            end,
            rewriter,
            target_layout,
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

                // Record the key as consumed even when it's a tombstone, so any
                // older copy from a lower-priority source is suppressed.
                if self.last_key.as_ref() != Some(k) {
                    self.last_key = Some(k.clone());
                    if let Some(val) = v {
                        return Ok(Some((k.clone(), val.clone())));
                    }
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

                if self.last_key.as_ref() != Some(&entry.key) {
                    self.last_key = Some(entry.key.clone());
                    if let Some(val) = entry.value {
                        let source = &self.sst_iters[entry.source_idx];
                        let src_layout = &source.reader().layout;
                        let rewritten_val = if src_layout != &self.target_layout {
                            self.rewriter
                                .rewrite(src_layout, &self.target_layout, &val)?
                        } else {
                            val
                        };
                        return Ok(Some((entry.key, rewritten_val)));
                    }
                }
            }
        }
    }
}
