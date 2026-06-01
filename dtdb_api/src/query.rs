use dtdb_storage::DbValue;

#[derive(Debug, Clone, PartialEq)]
pub struct SqlQuery {
    text: String,
    bindings: Vec<(String, DbValue)>,
}

impl SqlQuery {
    /// Creates a new SqlQuery with the given text.
    pub fn new(text: String) -> Self {
        Self {
            text,
            bindings: Vec::new(),
        }
    }

    /// Binds a parameter value by name.
    pub fn bind(mut self, name: impl Into<String>, value: impl Into<DbValue>) -> Self {
        self.bindings.push((name.into(), value.into()));
        self
    }

    /// Interpolates the bound parameters into the SQL query string safely,
    /// avoiding substitutions inside quotes and correctly escaping string parameters.
    pub fn interpolate(&self) -> Result<String, String> {
        let mut result = String::new();
        let mut chars = self.text.chars().peekable();
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while let Some(c) = chars.next() {
            if c == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
                result.push(c);
            } else if c == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
                result.push(c);
            } else if c == '@' && !in_single_quote && !in_double_quote {
                // Read placeholder name (alphanumeric + underscore)
                let mut name = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c.is_alphanumeric() || next_c == '_' {
                        name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if name.is_empty() {
                    result.push('@');
                } else {
                    // Find the bound value
                    if let Some((_, val)) = self.bindings.iter().find(|(k, _)| k == &name) {
                        // Format the value as a safe SQL literal
                        match val {
                            DbValue::Int(i) => result.push_str(&i.to_string()),
                            DbValue::Float(f) => result.push_str(&f.to_string()),
                            DbValue::Bool(b) => result.push_str(&b.to_string()),
                            DbValue::String(s) => {
                                // Escape single quotes by doubling them (SQL standard)
                                let escaped = s.replace('\'', "''");
                                result.push('\'');
                                result.push_str(&escaped);
                                result.push('\'');
                            }
                            DbValue::Bytes(b) => {
                                // Format bytes as hex literal x'0102ff'
                                let mut s = String::from("x'");
                                for byte in b.iter() {
                                    s.push_str(&format!("{:02x}", byte));
                                }
                                s.push('\'');
                                result.push_str(&s);
                            }
                            DbValue::Null => {
                                result.push_str("NULL");
                            }
                        }
                    } else {
                        return Err(format!("Unbound query parameter: @{}", name));
                    }
                }
            } else {
                result.push(c);
            }
        }

        Ok(result)
    }

    /// Accessor for query text
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Accessor for query bindings
    pub fn bindings(&self) -> &[(String, DbValue)] {
        &self.bindings
    }
}
