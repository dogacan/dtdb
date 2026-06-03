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
                            DbValue::Date(d) => {
                                result.push_str(&format!("DATE '{}'", d));
                            }
                            DbValue::Time(t) => {
                                result.push_str(&format!("TIME '{}'", t));
                            }
                            DbValue::Timestamp(ts) => {
                                result.push_str(&format!("TIMESTAMP '{}'", ts));
                            }
                            DbValue::Decimal(dec) => {
                                result.push_str(&dec.to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn interpolate_temporal_and_decimal_params() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        let time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        let ts = NaiveDateTime::new(date, time);
        let dec = Decimal::from_str("1.50").unwrap();

        let q = SqlQuery::new("INSERT INTO t VALUES (@d, @t, @ts, @dec)".to_string())
            .bind("d", date)
            .bind("t", time)
            .bind("ts", ts)
            .bind("dec", dec);

        // Date/Time/Timestamp render as typed SQL literals; Decimal renders as a
        // bare numeric literal preserving its scale.
        assert_eq!(
            q.interpolate().unwrap(),
            "INSERT INTO t VALUES (DATE '2026-06-03', TIME '14:30:00', \
             TIMESTAMP '2026-06-03 14:30:00', 1.50)"
        );
    }

    #[test]
    fn interpolate_handles_all_scalar_kinds() {
        let q = SqlQuery::new("VALUES (@i, @f, @b, @s, @nul)".to_string())
            .bind("i", 42i64)
            .bind("f", 1.5f64)
            .bind("b", true)
            .bind("s", "O'Brien")
            .bind("nul", DbValue::Null);

        // String params are single-quoted with embedded quotes doubled per the
        // SQL standard, so injection via the value is not possible.
        assert_eq!(
            q.interpolate().unwrap(),
            "VALUES (42, 1.5, true, 'O''Brien', NULL)"
        );
    }

    #[test]
    fn interpolate_renders_bytes_as_hex_literal() {
        let q = SqlQuery::new("SELECT @raw".to_string()).bind("raw", vec![0x01u8, 0x02, 0xff]);
        assert_eq!(q.interpolate().unwrap(), "SELECT x'0102ff'");
    }

    #[test]
    fn interpolate_skips_placeholders_inside_quotes() {
        // An @name sequence inside a string literal must be left untouched, while
        // the one outside is substituted.
        let q = SqlQuery::new("SELECT '@d' , @d".to_string())
            .bind("d", NaiveDate::from_ymd_opt(2026, 6, 3).unwrap());
        assert_eq!(q.interpolate().unwrap(), "SELECT '@d' , DATE '2026-06-03'");
    }

    #[test]
    fn interpolate_errors_on_unbound_param() {
        let q = SqlQuery::new("SELECT @missing".to_string());
        let err = q.interpolate().unwrap_err();
        assert!(err.contains("missing"), "unexpected error: {err}");
    }
}
