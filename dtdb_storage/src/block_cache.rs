use crate::{DbKey, DbValue};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

type CacheValue = Arc<Vec<(DbKey, Option<DbValue>)>>;

pub struct BlockCache {
    shards: Vec<Mutex<LruCache<(u64, usize), CacheValue>>>,
}

fn block_byte_size(block: &[(DbKey, Option<DbValue>)]) -> usize {
    let mut size = 0;
    for (k, v) in block {
        size += k.byte_size();
        size += v.as_ref().map_or(1, DbValue::byte_size);
    }
    size
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let num_shards = 16;
        let shard_capacity = capacity.div_ceil(num_shards);
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Mutex::new(LruCache::new(shard_capacity)));
        }
        Self { shards }
    }

    fn get_shard(&self, key: &(u64, usize)) -> usize {
        let hash = (key.0 ^ (key.1 as u64)) as usize;
        hash % self.shards.len()
    }

    pub fn get(&self, key: &(u64, usize)) -> Option<CacheValue> {
        let shard_idx = self.get_shard(key);
        let mut guard = self.shards[shard_idx].lock().unwrap();
        guard.get(key).cloned()
    }

    pub fn insert(&self, key: (u64, usize), value: CacheValue) {
        let shard_idx = self.get_shard(&key);
        let size_bytes = block_byte_size(&value);
        let mut guard = self.shards[shard_idx].lock().unwrap();
        guard.insert(key, value, size_bytes);
    }
}

pub struct LruCache<K, V> {
    map: HashMap<K, usize>,
    nodes: Vec<LruNode<K, V>>,
    head: Option<usize>,
    tail: Option<usize>,
    free: Vec<usize>,
    capacity_bytes: usize,
    current_bytes: usize,
}

struct LruNode<K, V> {
    key: Option<K>,
    value: Option<V>,
    prev: Option<usize>,
    next: Option<usize>,
    size: usize,
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    pub fn new(capacity_bytes: usize) -> Self {
        assert!(capacity_bytes > 0);
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            free: Vec::new(),
            capacity_bytes,
            current_bytes: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&idx) = self.map.get(key) {
            self.detach(idx);
            self.attach_head(idx);
            self.nodes[idx].value.as_ref()
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V, size: usize) {
        if let Some(&idx) = self.map.get(&key) {
            let old_size = self.nodes[idx].size;
            self.current_bytes = self
                .current_bytes
                .saturating_sub(old_size)
                .saturating_add(size);
            self.nodes[idx].value = Some(value);
            self.nodes[idx].size = size;
            self.detach(idx);
            self.attach_head(idx);
            self.evict_if_needed();
            return;
        }

        self.current_bytes = self.current_bytes.saturating_add(size);

        let idx = if let Some(free_idx) = self.free.pop() {
            free_idx
        } else {
            let new_idx = self.nodes.len();
            self.nodes.push(LruNode {
                key: None,
                value: None,
                prev: None,
                next: None,
                size: 0,
            });
            new_idx
        };

        self.nodes[idx].key = Some(key.clone());
        self.nodes[idx].value = Some(value);
        self.nodes[idx].size = size;
        self.map.insert(key, idx);
        self.attach_head(idx);

        self.evict_if_needed();
    }

    fn evict_if_needed(&mut self) {
        while self.current_bytes > self.capacity_bytes && self.tail.is_some() {
            let tail_idx = self.tail.unwrap();
            let node_size = self.nodes[tail_idx].size;
            self.current_bytes = self.current_bytes.saturating_sub(node_size);

            self.detach(tail_idx);
            if let Some(ref old_key) = self.nodes[tail_idx].key {
                self.map.remove(old_key);
            }
            self.nodes[tail_idx].key = None;
            self.nodes[tail_idx].value = None;
            self.nodes[tail_idx].size = 0;
            self.free.push(tail_idx);
        }
    }

    fn detach(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }

        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }

        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }

    fn attach_head(&mut self, idx: usize) {
        if let Some(h) = self.head {
            self.nodes[idx].next = Some(h);
            self.nodes[h].prev = Some(idx);
            self.head = Some(idx);
        } else {
            self.head = Some(idx);
            self.tail = Some(idx);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1, 1);
        cache.insert("b", 2, 1);
        cache.insert("c", 3, 1);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1, 1);
        cache.insert("b", 2, 1);
        cache.insert("c", 3, 1);

        // "a" is accessed, making it most recently used, order is now a -> c -> b (head to tail) or a -> c -> b?
        // Wait, let's see. head is "c", then "b", then "a".
        // Wait, get(&"a") detaches "a" and attaches to head. So "a" is head, "c" is next, "b" is tail.
        assert_eq!(cache.get(&"a"), Some(&1));

        // Insert "d", which should evict the tail, which is "b"
        cache.insert("d", 4, 1);

        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.get(&"d"), Some(&4));
    }

    #[test]
    fn test_lru_cache_update() {
        let mut cache = LruCache::new(2);

        cache.insert("a", 1, 1);
        cache.insert("a", 10, 1);

        assert_eq!(cache.get(&"a"), Some(&10));
    }

    #[test]
    fn test_lru_cache_reuses_freed_slots() {
        // Capacity for 3 unit-sized entries. After an eviction frees a node
        // slot, a later insert must reuse it from the free list rather than
        // growing `nodes` unbounded.
        let mut cache = LruCache::new(3);
        cache.insert("a", 1, 1);
        cache.insert("b", 2, 1);
        cache.insert("c", 1, 1);
        assert_eq!(cache.len(), 3);
        assert!(!cache.is_empty());

        // Insert "d": a node is pushed (nodes -> 4), then eviction trims the
        // tail ("a") and pushes its slot onto the free list.
        cache.insert("d", 4, 1);
        assert_eq!(cache.nodes.len(), 4);
        assert_eq!(cache.free.len(), 1);

        // Insert "e": this must pop the freed slot instead of pushing a new
        // node, so `nodes` stays at 4 (the eviction of "b" then frees its own
        // slot, but no new node is allocated).
        cache.insert("e", 5, 1);
        assert_eq!(cache.nodes.len(), 4, "freed slot should have been reused");

        assert_eq!(cache.get(&"d"), Some(&4));
        assert_eq!(cache.get(&"e"), Some(&5));
        assert_eq!(cache.get(&"a"), None);
    }

    #[test]
    fn test_lru_cache_len_and_is_empty() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        cache.insert("a", 1, 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_block_byte_size_accounts_for_all_value_types() {
        use chrono::{NaiveDate, NaiveTime};
        use rust_decimal::Decimal;

        // A block carrying one entry of every DbValue variant (plus a
        // tombstone) exercises each arm of `block_byte_size`. A large capacity
        // means nothing is evicted, so a subsequent get returns the block.
        let block: CacheValue = Arc::new(vec![
            (DbKey::Int(1), Some(DbValue::Int(1))),
            (DbKey::Int(2), Some(DbValue::Float(1.5))),
            (DbKey::Int(3), Some(DbValue::string("hi"))),
            (DbKey::Int(4), Some(DbValue::bytes(vec![1, 2, 3]))),
            (DbKey::Int(5), Some(DbValue::Null)),
            (DbKey::Int(6), Some(DbValue::Bool(true))),
            (
                DbKey::Int(7),
                Some(DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap())),
            ),
            (
                DbKey::Int(8),
                Some(DbValue::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap())),
            ),
            (
                DbKey::Int(9),
                Some(DbValue::Timestamp(
                    NaiveDate::from_ymd_opt(2026, 6, 4)
                        .unwrap()
                        .and_hms_opt(1, 2, 3)
                        .unwrap(),
                )),
            ),
            (DbKey::Int(10), Some(DbValue::Decimal(Decimal::new(150, 2)))),
            (DbKey::Int(11), None), // tombstone
        ]);
        let expected = block_byte_size(&block);
        assert!(expected > 0);

        let cache = BlockCache::new(1024 * 1024);
        cache.insert((0, 0), block.clone());
        assert_eq!(cache.get(&(0, 0)), Some(block));
    }
}
