use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Tokenizer trait splits a text string into a vector of search tokens.
pub trait Tokenizer: Send + Sync + 'static {
    fn tokenize(&self, text: &str) -> Vec<String>;
}

/// SimpleTokenizer tokenizes a string by splitting on whitespace and lowercasing the results.
#[derive(Debug, Clone, Default)]
pub struct SimpleTokenizer;

impl Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|s| s.to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

static GLOBAL_TOKENIZERS: OnceLock<RwLock<HashMap<String, Arc<dyn Tokenizer>>>> = OnceLock::new();

/// Retrieve a registered tokenizer by name.
pub fn get_tokenizer(name: &str) -> Option<Arc<dyn Tokenizer>> {
    let map = GLOBAL_TOKENIZERS.get_or_init(|| {
        let mut m: HashMap<String, Arc<dyn Tokenizer>> = HashMap::new();
        m.insert("simple".to_string(), Arc::new(SimpleTokenizer));
        RwLock::new(m)
    });
    map.read().unwrap().get(name).cloned()
}

/// Register a custom tokenizer.
pub fn register_global_tokenizer(name: &str, tokenizer: Arc<dyn Tokenizer>) {
    let map = GLOBAL_TOKENIZERS.get_or_init(|| {
        let mut m: HashMap<String, Arc<dyn Tokenizer>> = HashMap::new();
        m.insert("simple".to_string(), Arc::new(SimpleTokenizer));
        RwLock::new(m)
    });
    map.write().unwrap().insert(name.to_string(), tokenizer);
}
