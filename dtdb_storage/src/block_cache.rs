use crate::{DbKey, DbValue};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub type BlockCache = Mutex<LruCache<(u64, usize), Arc<Vec<(DbKey, Option<DbValue>)>>>>;

pub struct LruCache<K, V> {
    map: HashMap<K, usize>,
    nodes: Vec<LruNode<K, V>>,
    head: Option<usize>,
    tail: Option<usize>,
    free: Vec<usize>,
    capacity: usize,
}

struct LruNode<K, V> {
    key: Option<K>,
    value: Option<V>,
    prev: Option<usize>,
    next: Option<usize>,
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            free: Vec::new(),
            capacity,
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

    pub fn insert(&mut self, key: K, value: V) {
        if let Some(&idx) = self.map.get(&key) {
            self.nodes[idx].value = Some(value);
            self.detach(idx);
            self.attach_head(idx);
            return;
        }

        let idx = if self.map.len() >= self.capacity {
            // Evict tail
            let tail_idx = self.tail.expect("cache not empty");
            self.detach(tail_idx);
            if let Some(ref old_key) = self.nodes[tail_idx].key {
                self.map.remove(old_key);
            }
            tail_idx
        } else if let Some(free_idx) = self.free.pop() {
            free_idx
        } else {
            let new_idx = self.nodes.len();
            self.nodes.push(LruNode {
                key: None,
                value: None,
                prev: None,
                next: None,
            });
            new_idx
        };

        self.nodes[idx].key = Some(key.clone());
        self.nodes[idx].value = Some(value);
        self.map.insert(key, idx);
        self.attach_head(idx);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        // "a" is accessed, making it most recently used, order is now a -> c -> b (head to tail) or a -> c -> b?
        // Wait, let's see. head is "c", then "b", then "a".
        // Wait, get(&"a") detaches "a" and attaches to head. So "a" is head, "c" is next, "b" is tail.
        assert_eq!(cache.get(&"a"), Some(&1));

        // Insert "d", which should evict the tail, which is "b"
        cache.insert("d", 4);

        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.get(&"d"), Some(&4));
    }

    #[test]
    fn test_lru_cache_update() {
        let mut cache = LruCache::new(2);

        cache.insert("a", 1);
        cache.insert("a", 10);

        assert_eq!(cache.get(&"a"), Some(&10));
    }
}
