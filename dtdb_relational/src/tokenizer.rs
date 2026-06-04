use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// A sound compilation of a LIKE pattern into index tokens, produced by a
/// tokenizer that can accelerate LIKE (see [`Tokenizer::plan_like`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikePlan {
    /// Tokens that must ALL be present (ANDed) for a row to possibly match.
    /// Every token is guaranteed to be produced by the tokenizer for any value
    /// the pattern matches, so the resulting candidate set is a sound superset.
    pub required: Vec<String>,
    /// Whether candidate rows still need the original LIKE re-checked. Always
    /// `true` for n-gram tokenizers: token membership is necessary but not
    /// sufficient (the tokens may occur at non-matching positions).
    pub needs_recheck: bool,
}

/// Tokenizer trait splits a text string into a vector of search tokens.
pub trait Tokenizer: Send + Sync + 'static {
    fn tokenize(&self, text: &str) -> Vec<String>;

    /// Compile a LIKE `pattern` into a sound set of required tokens, or `None`
    /// if this tokenizer cannot soundly accelerate it. Default: `None`.
    ///
    /// Soundness contract: for every string `s` that LIKE-matches `pattern`,
    /// `tokenize(s)` must contain every token in the returned `required` set
    /// (so the token intersection over-approximates the matches and never
    /// drops one). Implement this only for tokenizers whose tokens are a
    /// faithful function of substrings, such as character n-grams; a lossy
    /// tokenizer (stemming, accent/punctuation folding) would violate the
    /// contract and cause false negatives.
    fn plan_like(&self, _pattern: &str) -> Option<LikePlan> {
        None
    }
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
    // SimpleTokenizer deliberately does NOT implement `plan_like`: word-level
    // tokens cannot answer substring LIKE queries (e.g. `%ell%`), so it keeps
    // the `None` default and such queries fall back to a full scan + filter.
}

/// TrigramTokenizer tokenizes a string into its overlapping, lowercased
/// character 3-grams (e.g. "hello" -> "hel", "ell", "llo"). Strings shorter
/// than three characters produce no tokens. Because the token set is exactly
/// the set of length-3 substrings, it can soundly accelerate substring LIKE
/// queries via [`Tokenizer::plan_like`].
#[derive(Debug, Clone, Default)]
pub struct TrigramTokenizer;

impl TrigramTokenizer {
    const N: usize = 3;
}

impl Tokenizer for TrigramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.to_lowercase().chars().collect();
        if chars.len() < Self::N {
            return Vec::new();
        }
        chars
            .windows(Self::N)
            .map(|w| w.iter().collect::<String>())
            .collect()
    }

    fn plan_like(&self, pattern: &str) -> Option<LikePlan> {
        // Split the pattern into maximal runs of literal characters. `%` and
        // `_` both terminate a run (dtdb LIKE has no ESCAPE, so both are always
        // wildcards). A run shorter than N yields no trigram and is skipped.
        // Reusing `tokenize` per run guarantees the query trigrams are
        // normalized identically to the indexed ones.
        let mut required: Vec<String> = pattern
            .split(['%', '_'])
            .flat_map(|run| self.tokenize(run))
            .collect();
        if required.is_empty() {
            // No run was long enough to produce a trigram (e.g. "%ab%",
            // "%a%b%"): the index cannot help, so decline.
            return None;
        }
        required.sort_unstable();
        required.dedup();
        Some(LikePlan {
            required,
            needs_recheck: true,
        })
    }
}

static GLOBAL_TOKENIZERS: OnceLock<RwLock<HashMap<String, Arc<dyn Tokenizer>>>> = OnceLock::new();

/// The built-in tokenizers, registered under their canonical names before any
/// custom registration.
fn default_tokenizers() -> HashMap<String, Arc<dyn Tokenizer>> {
    let mut m: HashMap<String, Arc<dyn Tokenizer>> = HashMap::new();
    m.insert("simple".to_string(), Arc::new(SimpleTokenizer));
    m.insert("trigram".to_string(), Arc::new(TrigramTokenizer));
    m
}

/// Retrieve a registered tokenizer by name.
pub fn get_tokenizer(name: &str) -> Option<Arc<dyn Tokenizer>> {
    let map = GLOBAL_TOKENIZERS.get_or_init(|| RwLock::new(default_tokenizers()));
    map.read().unwrap().get(name).cloned()
}

/// Register a custom tokenizer.
pub fn register_global_tokenizer(name: &str, tokenizer: Arc<dyn Tokenizer>) {
    let map = GLOBAL_TOKENIZERS.get_or_init(|| RwLock::new(default_tokenizers()));
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

    #[test]
    fn trigram_tokenizer_emits_lowercased_overlapping_3grams() {
        let t = TrigramTokenizer;
        assert_eq!(t.tokenize("hello"), vec!["hel", "ell", "llo"]);
        // Lowercased, so index-time and query-time trigrams agree.
        assert_eq!(t.tokenize("HeLlo"), t.tokenize("hello"));
        // Fewer than 3 characters yields no trigram.
        assert!(t.tokenize("ab").is_empty());
        assert!(t.tokenize("").is_empty());
        // A repeated trigram appears once per position; the index writer groups
        // these into a single (token -> positions) entry.
        assert_eq!(t.tokenize("aaaa"), vec!["aaa", "aaa"]);
    }

    #[test]
    fn builtin_trigram_tokenizer_is_registered() {
        let t = get_tokenizer("trigram").expect("trigram tokenizer must be registered");
        assert_eq!(t.tokenize("abcd"), vec!["abc", "bcd"]);
    }

    #[test]
    fn trigram_plan_like_compiles_substring_patterns() {
        let t = TrigramTokenizer;

        // Infix: the literal run "foobar" contributes its trigrams, sorted and
        // deduped, and a recheck is always required.
        let plan = t.plan_like("%foobar%").expect("foobar has trigrams");
        assert_eq!(plan.required, vec!["bar", "foo", "oba", "oob"]);
        assert!(plan.needs_recheck);

        // `_` and `%` both split runs; "foo" and "bar" each yield one trigram.
        let plan = t.plan_like("%foo_bar%").expect("foo and bar have trigrams");
        assert_eq!(plan.required, vec!["bar", "foo"]);

        // A prefix pattern still compiles (the optimizer may prefer the exact
        // range scan, but plan_like remains a sound fallback).
        let plan = t.plan_like("hello%").expect("hello has trigrams");
        assert_eq!(plan.required, vec!["ell", "hel", "llo"]);

        // No run reaches length 3: the index cannot help, so decline.
        assert_eq!(t.plan_like("%ab%"), None);
        assert_eq!(t.plan_like("%a%b%"), None);
        assert_eq!(t.plan_like("%%"), None);
    }

    #[test]
    fn simple_tokenizer_declines_like_acceleration() {
        // The default `plan_like` returns None: word-level tokens cannot answer
        // substring LIKE, so such queries must fall back to a full scan.
        assert_eq!(SimpleTokenizer.plan_like("%ell%"), None);
    }
}
