use crate::tokenizer::Tokenizer;
use std::collections::VecDeque;

/// FullTextQuery represents a parsed boolean search query tree.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FullTextQuery {
    Token(String),
    And(Box<FullTextQuery>, Box<FullTextQuery>),
    Or(Box<FullTextQuery>, Box<FullTextQuery>),
}

#[derive(Debug, PartialEq, Eq)]
enum LexToken {
    LParen,
    RParen,
    AndOp,
    OrOp,
    Word(String),
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
            } else {
                let mut term = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c.is_whitespace() || next_c == '(' || next_c == ')' {
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
        let mut parser = Parser { tokens: lex_tokens };
        let ast = parser.parse_or()?;
        if !parser.tokens.is_empty() {
            return Err(format!("Unexpected token: {:?}", parser.tokens[0]));
        }
        Ok(ast)
    }
}

struct Parser {
    tokens: VecDeque<LexToken>,
}

impl Parser {
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
                Some(LexToken::Word(_)) | Some(LexToken::LParen) => {
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
