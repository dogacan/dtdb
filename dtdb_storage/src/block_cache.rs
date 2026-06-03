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
        size += match v {
            Some(DbValue::Int(_)) => 8,
            Some(DbValue::Float(_)) => 8,
            Some(DbValue::String(s)) => s.len(),
            Some(DbValue::Bytes(b)) => b.len(),
            Some(DbValue::Null) => 1,
            Some(DbValue::Bool(_)) => 1,
            Some(DbValue::Date(_)) => 4,
            Some(DbValue::Time(_)) => 8,
            Some(DbValue::Timestamp(_)) => 8,
            Some(DbValue::Decimal(_)) => 16,
            None => 1,
        };
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
}
