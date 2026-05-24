use serde::{Deserialize, Serialize};
use dtdb_storage::DbValue;
use dtdb_relational::{Row, Schema};

/// Operator represents binary operations in SQL WHERE conditions.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    NotEq,
    And,
    Or,
    Like,
    Add,
    Sub,
    Mul,
    Div,
}

/// Expr represents a scalar expression in SQL (column names, literals, binary operations).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(DbValue),
    BinaryOp {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
}

impl Expr {
    /// Evaluates the expression against a Row and its Schema.
    pub fn eval(&self, row: &Row, schema: &Schema) -> Result<DbValue, String> {
        match self {
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Column(name) => {
                // Find column in schema.
                // We support both exact match "users.name" and suffix match "name" to make queries convenient.
                let idx = schema
                    .columns
                    .iter()
                    .position(|col| {
                        if col.name == *name {
                            true
                        } else if let Some(pos) = name.rfind('.') {
                            col.name == name[pos + 1..]
                        } else if let Some(col_pos) = col.name.rfind('.') {
                            col.name[col_pos + 1..] == *name
                        } else {
                            false
                        }
                    })
                    .ok_or_else(|| format!("Column not found in schema: '{}'", name))?;

                row.get_by_index(idx)
                    .cloned()
                    .ok_or_else(|| format!("Index {} out of bounds for row values", idx))
            }
            Expr::BinaryOp { left, op, right } => {
                let l_val = left.eval(row, schema)?;
                let r_val = right.eval(row, schema)?;

                match op {
                    Operator::And => {
                        let l_bool = to_bool(&l_val)?;
                        let r_bool = to_bool(&r_val)?;
                        Ok(DbValue::Int(if l_bool && r_bool { 1 } else { 0 }))
                    }
                    Operator::Or => {
                        let l_bool = to_bool(&l_val)?;
                        let r_bool = to_bool(&r_val)?;
                        Ok(DbValue::Int(if l_bool || r_bool { 1 } else { 0 }))
                    }
                    Operator::Like => {
                        let text = to_string_val(&l_val)?;
                        let pattern = to_string_val(&r_val)?;
                        let matched = like_match(&text, &pattern);
                        Ok(DbValue::Int(if matched { 1 } else { 0 }))
                    }
                    Operator::Add => {
                        eval_arithmetic(&l_val, &r_val, |a, b| a + b, |a, b| a + b)
                    }
                    Operator::Sub => {
                        eval_arithmetic(&l_val, &r_val, |a, b| a - b, |a, b| a - b)
                    }
                    Operator::Mul => {
                        eval_arithmetic(&l_val, &r_val, |a, b| a * b, |a, b| a * b)
                    }
                    Operator::Div => {
                        match r_val {
                            DbValue::Int(0) => Err("Division by zero".to_string()),
                            DbValue::Float(f) if f == 0.0 => Err("Division by zero".to_string()),
                            _ => eval_arithmetic(&l_val, &r_val, |a, b| a / b, |a, b| a / b),
                        }
                    }
                    other_op => {
                        let ordering = compare_values(&l_val, &r_val)?;
                        let matched = match other_op {
                            Operator::Eq => ordering == std::cmp::Ordering::Equal,
                            Operator::NotEq => ordering != std::cmp::Ordering::Equal,
                            Operator::Gt => ordering == std::cmp::Ordering::Greater,
                            Operator::Lt => ordering == std::cmp::Ordering::Less,
                            Operator::GtEq => {
                                ordering == std::cmp::Ordering::Greater
                                    || ordering == std::cmp::Ordering::Equal
                            }
                            Operator::LtEq => {
                                ordering == std::cmp::Ordering::Less
                                    || ordering == std::cmp::Ordering::Equal
                            }
                            _ => unreachable!(),
                        };
                        Ok(DbValue::Int(if matched { 1 } else { 0 }))
                    }
                }
            }
        }
    }
}

/// Coerces a DbValue to a boolean (Int not equal to 0).
fn to_bool(val: &DbValue) -> Result<bool, String> {
    match val {
        DbValue::Int(v) => Ok(*v != 0),
        other => Err(format!("Cannot convert value to boolean: {:?}", other)),
    }
}

/// Coerces a DbValue to a String.
fn to_string_val(val: &DbValue) -> Result<String, String> {
    match val {
        DbValue::String(s) => Ok(s.clone()),
        other => Err(format!("Expected string value, got: {:?}", other)),
    }
}

/// Helper to compare two DbValues, applying implicit type coercion for numeric types.
///
/// For example, comparing an Int with a Float automatically converts the Int to Float.
fn compare_values(l: &DbValue, r: &DbValue) -> Result<std::cmp::Ordering, String> {
    match (l, r) {
        (DbValue::Int(lv), DbValue::Int(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Float(lv), DbValue::Float(rv)) => {
            lv.partial_cmp(rv).ok_or_else(|| "NaN float comparison".to_string())
        }
        (DbValue::Int(lv), DbValue::Float(rv)) => {
            (*lv as f64).partial_cmp(rv).ok_or_else(|| "NaN float comparison".to_string())
        }
        (DbValue::Float(lv), DbValue::Int(rv)) => {
            lv.partial_cmp(&(*rv as f64)).ok_or_else(|| "NaN float comparison".to_string())
        }
        (DbValue::String(lv), DbValue::String(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Bytes(lv), DbValue::Bytes(rv)) => Ok(lv.cmp(rv)),
        (expected, actual) => Err(format!(
            "Type mismatch: cannot compare {:?} and {:?}",
            expected, actual
        )),
    }
}

/// Implements SQL LIKE recursive matching.
///
/// - `%` matches zero or more of any characters.
/// - `_` matches exactly one of any character.
fn like_match(text: &str, pattern: &str) -> bool {
    let t_chars: Vec<char> = text.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_recursive(&t_chars, &p_chars, 0, 0)
}

fn like_match_recursive(t: &[char], p: &[char], t_idx: usize, p_idx: usize) -> bool {
    if p_idx == p.len() {
        return t_idx == t.len();
    }

    if p[p_idx] == '%' {
        // Try skipping the '%' or matching one or more characters.
        for i in t_idx..=t.len() {
            if like_match_recursive(t, p, i, p_idx + 1) {
                return true;
            }
        }
        return false;
    }

    if t_idx == t.len() {
        return false;
    }

    if p[p_idx] == '_' || p[p_idx] == t[t_idx] {
        return like_match_recursive(t, p, t_idx + 1, p_idx + 1);
    }

    false
}

/// Helper to evaluate arithmetic operations on DbValues with promotion logic.
fn eval_arithmetic<FI, FF>(
    l: &DbValue,
    r: &DbValue,
    int_op: FI,
    float_op: FF,
) -> Result<DbValue, String>
where
    FI: FnOnce(i64, i64) -> i64,
    FF: FnOnce(f64, f64) -> f64,
{
    match (l, r) {
        (DbValue::Int(lv), DbValue::Int(rv)) => {
            Ok(DbValue::Int(int_op(*lv, *rv)))
        }
        (DbValue::Float(lv), DbValue::Float(rv)) => {
            Ok(DbValue::Float(float_op(*lv, *rv)))
        }
        (DbValue::Int(lv), DbValue::Float(rv)) => {
            Ok(DbValue::Float(float_op(*lv as f64, *rv)))
        }
        (DbValue::Float(lv), DbValue::Int(rv)) => {
            Ok(DbValue::Float(float_op(*lv, *rv as f64)))
        }
        (expected, actual) => Err(format!(
            "Cannot perform arithmetic on non-numeric types: {:?} and {:?}",
            expected, actual
        )),
    }
}

