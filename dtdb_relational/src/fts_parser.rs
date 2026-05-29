use crate::tokenizer::Tokenizer;
use std::collections::VecDeque;

/// FullTextQuery represents a parsed boolean search query tree.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FullTextQuery {
    Token(String),
    And(Box<FullTextQuery>, Box<FullTextQuery>),
    Or(Box<FullTextQuery>, Box<FullTextQuery>),
    Phrase(Vec<String>),
}

#[derive(Debug, PartialEq, Eq)]
enum LexToken {
    LParen,
    RParen,
    AndOp,
    OrOp,
    Word(String),
    PhraseString(String),
}

impl FullTextQuery {
    /// Parses a boolean search query string into a query tree.
    /// Terms are normalized using the provided tokenizer.
    pub fn parse(text: &str, tokenizer: &dyn Tokenizer) -> Result<Self, String> {
        // 1. Lexing phase
        let mut lex_tokens = VecDeque::new();
        let mut chars = text.chars().peekable();

        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
            } else if c == '(' {
                lex_tokens.push_back(LexToken::LParen);
                chars.next();
            } else if c == ')' {
                lex_tokens.push_back(LexToken::RParen);
                chars.next();
            } else if c == '"' {
                chars.next(); // consume opening quote
                let mut phrase_content = String::new();
                let mut found_closing = false;
                for next_c in chars.by_ref() {
                    if next_c == '"' {
                        found_closing = true;
                        break;
                    }
                    phrase_content.push(next_c);
                }
                if !found_closing {
                    return Err(
                        "Unterminated phrase query (missing closing double quote)".to_string()
                    );
                }
                lex_tokens.push_back(LexToken::PhraseString(phrase_content));
            } else {
                let mut term = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c.is_whitespace() || next_c == '(' || next_c == ')' || next_c == '"' {
                        break;
                    }
                    term.push(next_c);
                    chars.next();
                }
                let term_upper = term.to_uppercase();
                if term_upper == "AND" {
                    lex_tokens.push_back(LexToken::AndOp);
                } else if term_upper == "OR" {
                    lex_tokens.push_back(LexToken::OrOp);
                } else {
                    let sub_tokens = tokenizer.tokenize(&term);
                    for tok in sub_tokens {
                        lex_tokens.push_back(LexToken::Word(tok));
                    }
                }
            }
        }

        if lex_tokens.is_empty() {
            return Err("Empty search query".to_string());
        }

        // 2. Parsing phase using recursive descent
        let mut parser = Parser {
            tokens: lex_tokens,
            tokenizer,
        };
        let ast = parser.parse_or()?;
        if !parser.tokens.is_empty() {
            return Err(format!("Unexpected token: {:?}", parser.tokens[0]));
        }
        Ok(ast)
    }
}

struct Parser<'a> {
    tokens: VecDeque<LexToken>,
    tokenizer: &'a dyn Tokenizer,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&LexToken> {
        self.tokens.front()
    }

    fn next_token(&mut self) -> Option<LexToken> {
        self.tokens.pop_front()
    }

    fn parse_or(&mut self) -> Result<FullTextQuery, String> {
        let mut node = self.parse_and()?;
        while let Some(LexToken::OrOp) = self.peek() {
            self.next_token();
            let right = self.parse_and()?;
            node = FullTextQuery::Or(Box::new(node), Box::new(right));
        }
        Ok(node)
    }

    fn parse_and(&mut self) -> Result<FullTextQuery, String> {
        let mut node = self.parse_primary()?;
        loop {
            match self.peek() {
                Some(LexToken::AndOp) => {
                    self.next_token();
                    let right = self.parse_primary()?;
                    node = FullTextQuery::And(Box::new(node), Box::new(right));
                }
                Some(LexToken::Word(_))
                | Some(LexToken::PhraseString(_))
                | Some(LexToken::LParen) => {
                    let right = self.parse_primary()?;
                    node = FullTextQuery::And(Box::new(node), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(node)
    }

    fn parse_primary(&mut self) -> Result<FullTextQuery, String> {
        match self.next_token() {
            Some(LexToken::Word(w)) => Ok(FullTextQuery::Token(w)),
            Some(LexToken::PhraseString(s)) => {
                let tokens = self.tokenizer.tokenize(&s);
                if tokens.is_empty() {
                    // An empty quoted phrase (e.g. `""` or `"   "`) cannot meaningfully
                    // match anything. Allowing it through leads to inconsistent behavior
                    // across the two evaluation paths: the storage-index path returns
                    // no matches while the row-by-row evaluator treats it as "always
                    // true", so the same query returns different answers for rows in
                    // the write buffer vs. rows on disk.
                    return Err("Empty FTS phrase is not allowed".to_string());
                }
                Ok(FullTextQuery::Phrase(tokens))
            }
            Some(LexToken::LParen) => {
                let node = self.parse_or()?;
                match self.next_token() {
                    Some(LexToken::RParen) => Ok(node),
                    _ => Err("Expected closing parenthesis ')'".to_string()),
                }
            }
            Some(other) => Err(format!("Expected term or '(' but found {:?}", other)),
            None => Err("Unexpected end of query".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::SimpleTokenizer;

    #[test]
    fn empty_phrase_is_rejected() {
        let tokenizer = SimpleTokenizer;
        // The bare empty quoted phrase produces zero tokens — must error rather
        // than silently parse to `Phrase([])`, whose semantics differ between
        // the index-driven and row-driven evaluators.
        let err = FullTextQuery::parse("\"\"", &tokenizer).unwrap_err();
        assert!(err.contains("Empty FTS phrase"));

        // Whitespace-only or punctuation-only phrases also tokenize to nothing.
        let err = FullTextQuery::parse("\"   \"", &tokenizer).unwrap_err();
        assert!(err.contains("Empty FTS phrase"));
    }

    #[test]
    fn non_empty_phrase_still_parses() {
        let tokenizer = SimpleTokenizer;
        let q = FullTextQuery::parse("\"hello world\"", &tokenizer).unwrap();
        match q {
            FullTextQuery::Phrase(tokens) => {
                assert_eq!(tokens, vec!["hello".to_string(), "world".to_string()]);
            }
            other => panic!("expected Phrase, got {other:?}"),
        }
    }

    #[test]
    fn single_token_parses() {
        let tokenizer = SimpleTokenizer;
        let q = FullTextQuery::parse("Rust", &tokenizer).unwrap();
        // SimpleTokenizer lowercases the term.
        assert_eq!(q, FullTextQuery::Token("rust".to_string()));
    }

    #[test]
    fn explicit_and_operator() {
        let tokenizer = SimpleTokenizer;
        let q = FullTextQuery::parse("foo AND bar", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::And(
                Box::new(FullTextQuery::Token("foo".to_string())),
                Box::new(FullTextQuery::Token("bar".to_string())),
            )
        );
    }

    #[test]
    fn explicit_or_operator() {
        let tokenizer = SimpleTokenizer;
        let q = FullTextQuery::parse("foo OR bar", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::Or(
                Box::new(FullTextQuery::Token("foo".to_string())),
                Box::new(FullTextQuery::Token("bar".to_string())),
            )
        );
    }

    #[test]
    fn operators_are_case_insensitive() {
        let tokenizer = SimpleTokenizer;
        let lower = FullTextQuery::parse("foo and bar", &tokenizer).unwrap();
        let mixed = FullTextQuery::parse("foo And bar", &tokenizer).unwrap();
        let upper = FullTextQuery::parse("foo AND bar", &tokenizer).unwrap();
        assert_eq!(lower, upper);
        assert_eq!(mixed, upper);
    }

    #[test]
    fn implicit_and_between_adjacent_terms() {
        let tokenizer = SimpleTokenizer;
        // Two bare terms with no operator are joined by an implicit AND.
        let q = FullTextQuery::parse("foo bar", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::And(
                Box::new(FullTextQuery::Token("foo".to_string())),
                Box::new(FullTextQuery::Token("bar".to_string())),
            )
        );
    }

    #[test]
    fn implicit_and_before_phrase_and_paren() {
        let tokenizer = SimpleTokenizer;
        // A bare term followed directly by a phrase, then by a parenthesized
        // group, exercises the implicit-AND branches for PhraseString and LParen.
        let q = FullTextQuery::parse("foo \"bar baz\" (qux)", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::And(
                Box::new(FullTextQuery::And(
                    Box::new(FullTextQuery::Token("foo".to_string())),
                    Box::new(FullTextQuery::Phrase(vec![
                        "bar".to_string(),
                        "baz".to_string()
                    ])),
                )),
                Box::new(FullTextQuery::Token("qux".to_string())),
            )
        );
    }

    #[test]
    fn or_has_lower_precedence_than_and() {
        let tokenizer = SimpleTokenizer;
        // `a AND b OR c` should parse as `(a AND b) OR c`.
        let q = FullTextQuery::parse("a AND b OR c", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::Or(
                Box::new(FullTextQuery::And(
                    Box::new(FullTextQuery::Token("a".to_string())),
                    Box::new(FullTextQuery::Token("b".to_string())),
                )),
                Box::new(FullTextQuery::Token("c".to_string())),
            )
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        let tokenizer = SimpleTokenizer;
        // `a AND (b OR c)` forces the OR to bind first.
        let q = FullTextQuery::parse("a AND (b OR c)", &tokenizer).unwrap();
        assert_eq!(
            q,
            FullTextQuery::And(
                Box::new(FullTextQuery::Token("a".to_string())),
                Box::new(FullTextQuery::Or(
                    Box::new(FullTextQuery::Token("b".to_string())),
                    Box::new(FullTextQuery::Token("c".to_string())),
                )),
            )
        );
    }

    #[test]
    fn empty_query_is_rejected() {
        let tokenizer = SimpleTokenizer;
        let err = FullTextQuery::parse("   ", &tokenizer).unwrap_err();
        assert!(err.contains("Empty search query"), "got: {err}");
    }

    #[test]
    fn unterminated_phrase_is_rejected() {
        let tokenizer = SimpleTokenizer;
        let err = FullTextQuery::parse("\"hello world", &tokenizer).unwrap_err();
        assert!(err.contains("Unterminated phrase"), "got: {err}");
    }

    #[test]
    fn unclosed_parenthesis_is_rejected() {
        let tokenizer = SimpleTokenizer;
        let err = FullTextQuery::parse("(foo OR bar", &tokenizer).unwrap_err();
        assert!(err.contains("closing parenthesis"), "got: {err}");
    }

    #[test]
    fn trailing_unexpected_token_is_rejected() {
        let tokenizer = SimpleTokenizer;
        // A stray closing paren leaves an unconsumed token after parsing.
        let err = FullTextQuery::parse("foo)", &tokenizer).unwrap_err();
        assert!(err.contains("Unexpected token"), "got: {err}");
    }

    #[test]
    fn leading_operator_is_rejected() {
        let tokenizer = SimpleTokenizer;
        // An expression cannot start with a binary operator: parse_primary sees
        // the AndOp and reports it as an unexpected primary token.
        let err = FullTextQuery::parse("AND foo", &tokenizer).unwrap_err();
        assert!(err.contains("Expected term"), "got: {err}");
    }

    #[test]
    fn dangling_operator_at_end_is_rejected() {
        let tokenizer = SimpleTokenizer;
        // `foo OR` has nothing on the right-hand side of the operator.
        let err = FullTextQuery::parse("foo OR", &tokenizer).unwrap_err();
        assert!(err.contains("Unexpected end of query"), "got: {err}");
    }

    #[test]
    fn empty_parentheses_are_rejected() {
        let tokenizer = SimpleTokenizer;
        // `()` has no inner expression for parse_or to consume.
        let err = FullTextQuery::parse("()", &tokenizer).unwrap_err();
        assert!(err.contains("Expected term"), "got: {err}");
    }

    #[test]
    fn query_is_serde_roundtrippable() {
        let tokenizer = SimpleTokenizer;
        let q = FullTextQuery::parse("a AND (b OR c)", &tokenizer).unwrap();
        let bytes = bincode::serialize(&q).unwrap();
        let decoded: FullTextQuery = bincode::deserialize(&bytes).unwrap();
        assert_eq!(q, decoded);
    }
}
