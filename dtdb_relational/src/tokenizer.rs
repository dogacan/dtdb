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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_tokenizer_splits_lowercases_and_drops_empties() {
        let t = SimpleTokenizer;
        assert_eq!(
            t.tokenize("Hello   World  RUST"),
            vec!["hello".to_string(), "world".to_string(), "rust".to_string()]
        );
        // Whitespace-only input yields no tokens.
        assert!(t.tokenize("   \t\n ").is_empty());
        assert!(t.tokenize("").is_empty());
    }

    #[test]
    fn builtin_simple_tokenizer_is_registered() {
        let t = get_tokenizer("simple").expect("simple tokenizer must be registered");
        assert_eq!(t.tokenize("A b"), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn unknown_tokenizer_is_none() {
        assert!(get_tokenizer("does-not-exist-xyz").is_none());
    }

    #[test]
    fn register_global_tokenizer_makes_it_retrievable() {
        // A tokenizer that splits on commas, for a distinct observable behavior.
        #[derive(Debug)]
        struct CommaTokenizer;
        impl Tokenizer for CommaTokenizer {
            fn tokenize(&self, text: &str) -> Vec<String> {
                text.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
        }

        register_global_tokenizer("comma-test", Arc::new(CommaTokenizer));
        let t = get_tokenizer("comma-test").expect("just-registered tokenizer must exist");
        assert_eq!(t.tokenize("a, b ,c"), vec!["a", "b", "c"]);

        // Re-registering under the same name overwrites the previous entry.
        register_global_tokenizer("comma-test", Arc::new(SimpleTokenizer));
        let t = get_tokenizer("comma-test").unwrap();
        assert_eq!(t.tokenize("a, b"), vec!["a,".to_string(), "b".to_string()]);
    }
}
