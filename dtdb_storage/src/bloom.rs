use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// A simple, self-contained Bloom Filter for checking set membership probabilistically.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BloomFilter {
    bit_vec: Vec<u8>,
    num_hashes: usize,
    bits_count: usize,
}

impl BloomFilter {
    /// Creates a new BloomFilter for `num_items` with optimal size and hash count.
    pub fn new(num_items: usize, false_positive_rate: f64) -> Self {
        if num_items == 0 {
            return Self {
                bit_vec: Vec::new(),
                num_hashes: 0,
                bits_count: 0,
            };
        }

        // Optimal size m = - (n * ln(p)) / (ln(2))^2
        let ln2_sq = 2.0f64.ln().powi(2);
        let m_float = -((num_items as f64) * false_positive_rate.ln()) / ln2_sq;
        let bits_count = (m_float.ceil() as usize).max(8);

        // Optimal hash count k = (m / n) * ln(2)
        let k_float = ((bits_count as f64) / (num_items as f64)) * 2.0f64.ln();
        let num_hashes = (k_float.round() as usize).clamp(1, 30);

        // Round bits_count to be a multiple of 8 to simplify bit_vec sizing
        let bytes_count = bits_count.div_ceil(8);
        let actual_bits_count = bytes_count * 8;

        Self {
            bit_vec: vec![0u8; bytes_count],
            num_hashes,
            bits_count: actual_bits_count,
        }
    }

    /// Inserts a key into the BloomFilter.
    pub fn insert<T: Hash>(&mut self, key: &T) {
        if self.bits_count == 0 {
            return;
        }
        let (hash1, hash2) = self.get_hashes(key);
        for i in 0..self.num_hashes {
            // double hashing formula: hash_i = (hash1 + i * hash2) % bits_count
            let hash_i =
                hash1.wrapping_add((i as u64).wrapping_mul(hash2)) % (self.bits_count as u64);
            let bit_idx = hash_i as usize;
            let byte_idx = bit_idx / 8;
            let bit_offset = bit_idx % 8;
            self.bit_vec[byte_idx] |= 1 << bit_offset;
        }
    }

    /// Checks if a key might be in the BloomFilter.
    ///
    /// Returns `false` if the key is definitely not in the set, and `true`
    /// if the key is probably in the set (subject to false_positive_rate).
    pub fn contains<T: Hash>(&self, key: &T) -> bool {
        if self.bits_count == 0 {
            return false;
        }
        let (hash1, hash2) = self.get_hashes(key);
        for i in 0..self.num_hashes {
            let hash_i =
                hash1.wrapping_add((i as u64).wrapping_mul(hash2)) % (self.bits_count as u64);
            let bit_idx = hash_i as usize;
            let byte_idx = bit_idx / 8;
            let bit_offset = bit_idx % 8;
            if (self.bit_vec[byte_idx] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Helper to compute two independent 64-bit hashes using Xxh64.
    fn get_hashes<T: Hash>(&self, key: &T) -> (u64, u64) {
        use xxhash_rust::xxh64::Xxh64;

        let mut h1 = Xxh64::new(0);
        key.hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = Xxh64::new(1234567890u64);
        key.hash(&mut h2);
        let hash2 = h2.finish();

        (hash1, hash2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(10, 0.01);
        bloom.insert(&"key_a");
        bloom.insert(&"key_b");

        assert!(bloom.contains(&"key_a"));
        assert!(bloom.contains(&"key_b"));
        assert!(!bloom.contains(&"key_c"));
    }
}
