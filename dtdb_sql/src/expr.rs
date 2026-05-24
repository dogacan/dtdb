use dtdb_relational::{Row, Schema};
use dtdb_storage::DbValue;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
}

impl Expr {
    /// Recursively collects all column names referenced in this expression.
    pub fn collect_columns(&self, columns: &mut HashSet<String>) {
        match self {
            Expr::Column(name) => {
                columns.insert(name.clone());
            }
            Expr::Literal(_) => {}
            Expr::BinaryOp { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.collect_columns(columns);
                }
                for cond in conditions {
                    cond.collect_columns(columns);
                }
                for res in results {
                    res.collect_columns(columns);
                }
                if let Some(el) = else_result {
                    el.collect_columns(columns);
                }
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.collect_columns(columns);
                }
            }
        }
    }

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

                // Handle logical AND/OR with three-valued logic
                if matches!(op, Operator::And | Operator::Or) {
                    return match op {
                        Operator::And => {
                            let l_null = matches!(l_val, DbValue::Null);
                            let r_null = matches!(r_val, DbValue::Null);
                            if l_null && r_null {
                                return Ok(DbValue::Null);
                            }
                            let l_bool = if l_null { None } else { Some(to_bool(&l_val)?) };
                            let r_bool = if r_null { None } else { Some(to_bool(&r_val)?) };
                            match (l_bool, r_bool) {
                                (Some(false), _) | (_, Some(false)) => Ok(DbValue::Int(0)),
                                (Some(true), Some(true)) => Ok(DbValue::Int(1)),
                                _ => Ok(DbValue::Null),
                            }
                        }
                        Operator::Or => {
                            let l_null = matches!(l_val, DbValue::Null);
                            let r_null = matches!(r_val, DbValue::Null);
                            if l_null && r_null {
                                return Ok(DbValue::Null);
                            }
                            let l_bool = if l_null { None } else { Some(to_bool(&l_val)?) };
                            let r_bool = if r_null { None } else { Some(to_bool(&r_val)?) };
                            match (l_bool, r_bool) {
                                (Some(true), _) | (_, Some(true)) => Ok(DbValue::Int(1)),
                                (Some(false), Some(false)) => Ok(DbValue::Int(0)),
                                _ => Ok(DbValue::Null),
                            }
                        }
                        _ => unreachable!(),
                    };
                } else if matches!(l_val, DbValue::Null) || matches!(r_val, DbValue::Null) {
                    // Propagate NULL for arithmetic, comparison, and LIKE operations
                    return Ok(DbValue::Null);
                }

                match op {
                    Operator::And => unreachable!(),
                    Operator::Or => unreachable!(),
                    Operator::Like => {
                        let text = to_string_val(&l_val)?;
                        let pattern = to_string_val(&r_val)?;
                        let matched = like_match(&text, &pattern);
                        Ok(DbValue::Int(if matched { 1 } else { 0 }))
                    }
                    Operator::Add => eval_arithmetic(&l_val, &r_val, |a, b| a + b, |a, b| a + b),
                    Operator::Sub => eval_arithmetic(&l_val, &r_val, |a, b| a - b, |a, b| a - b),
                    Operator::Mul => eval_arithmetic(&l_val, &r_val, |a, b| a * b, |a, b| a * b),
                    Operator::Div => match r_val {
                        DbValue::Int(0) => Err("Division by zero".to_string()),
                        DbValue::Float(0.0) => Err("Division by zero".to_string()),
                        _ => eval_arithmetic(&l_val, &r_val, |a, b| a / b, |a, b| a / b),
                    },
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
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if conditions.len() != results.len() {
                    return Err(
                        "CASE expression conditions and results length mismatch".to_string()
                    );
                }

                let mut matched_idx = None;
                if let Some(op_expr) = operand {
                    let op_val = op_expr.eval(row, schema)?;
                    for (i, cond_expr) in conditions.iter().enumerate() {
                        let cond_val = cond_expr.eval(row, schema)?;
                        if let Ok(ordering) = compare_values(&op_val, &cond_val)
                            && ordering == std::cmp::Ordering::Equal
                        {
                            matched_idx = Some(i);
                            break;
                        }
                    }
                } else {
                    for (i, cond_expr) in conditions.iter().enumerate() {
                        let cond_val = cond_expr.eval(row, schema)?;
                        if to_bool(&cond_val)? {
                            matched_idx = Some(i);
                            break;
                        }
                    }
                }

                if let Some(idx) = matched_idx {
                    results[idx].eval(row, schema)
                } else if let Some(else_expr) = else_result {
                    else_expr.eval(row, schema)
                } else {
                    Ok(DbValue::Null)
                }
            }
            Expr::Function { name, args } => {
                let name_upper = name.to_uppercase();
                match name_upper.as_str() {
                    "LENGTH" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "LENGTH expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        Ok(DbValue::Int(s.chars().count() as i64))
                    }
                    "SUBSTR" | "SUBSTRING" => {
                        if args.len() != 2 && args.len() != 3 {
                            return Err(format!(
                                "SUBSTR expects 2 or 3 arguments, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        let start_val = args[1].eval(row, schema)?;
                        let start = match start_val {
                            DbValue::Int(i) => i,
                            other => {
                                return Err(format!(
                                    "SUBSTR start index must be integer, got {:?}",
                                    other
                                ));
                            }
                        };

                        let chars: Vec<char> = s.chars().collect();
                        let n = chars.len() as i64;
                        let start_idx = if start > 0 {
                            start - 1
                        } else if start == 0 {
                            -1
                        } else {
                            n + start
                        };

                        if args.len() == 2 {
                            let start_rust = start_idx.max(0) as usize;
                            if start_rust >= chars.len() {
                                Ok(DbValue::String("".to_string()))
                            } else {
                                Ok(DbValue::String(chars[start_rust..].iter().collect()))
                            }
                        } else {
                            let len_val = args[2].eval(row, schema)?;
                            let length = match len_val {
                                DbValue::Int(i) => i,
                                other => {
                                    return Err(format!(
                                        "SUBSTR length must be integer, got {:?}",
                                        other
                                    ));
                                }
                            };
                            if length <= 0 {
                                Ok(DbValue::String("".to_string()))
                            } else {
                                let end_idx = start_idx + length;
                                let active_start = start_idx.max(0) as usize;
                                let active_end = end_idx.clamp(0, n) as usize;
                                if active_start < active_end && active_start < chars.len() {
                                    Ok(DbValue::String(
                                        chars[active_start..active_end].iter().collect(),
                                    ))
                                } else {
                                    Ok(DbValue::String("".to_string()))
                                }
                            }
                        }
                    }
                    "COALESCE" => {
                        if args.is_empty() {
                            return Err("COALESCE expects at least 1 argument".to_string());
                        }
                        let mut final_val = None;
                        for arg_expr in args {
                            let val = arg_expr.eval(row, schema)?;
                            if !matches!(val, DbValue::Null) {
                                return Ok(val);
                            }
                            final_val = Some(val);
                        }
                        Ok(final_val.unwrap_or(DbValue::Null))
                    }
                    other => Err(format!("Unsupported scalar function: {}", other)),
                }
            }
        }
    }
}

fn coerce_to_string(val: &DbValue) -> String {
    match val {
        DbValue::String(s) => s.clone(),
        DbValue::Int(i) => i.to_string(),
        DbValue::Float(f) => f.to_string(),
        DbValue::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        DbValue::Null => "NULL".to_string(),
    }
}

/// Coerces a DbValue to a boolean (Int not equal to 0, or Null treated as false).
fn to_bool(val: &DbValue) -> Result<bool, String> {
    match val {
        DbValue::Int(v) => Ok(*v != 0),
        DbValue::Null => Ok(false),
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
        (DbValue::Null, DbValue::Null) => Ok(std::cmp::Ordering::Equal),
        (DbValue::Null, _) => Ok(std::cmp::Ordering::Less),
        (_, DbValue::Null) => Ok(std::cmp::Ordering::Greater),
        (DbValue::Int(lv), DbValue::Int(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Float(lv), DbValue::Float(rv)) => lv
            .partial_cmp(rv)
            .ok_or_else(|| "NaN float comparison".to_string()),
        (DbValue::Int(lv), DbValue::Float(rv)) => (*lv as f64)
            .partial_cmp(rv)
            .ok_or_else(|| "NaN float comparison".to_string()),
        (DbValue::Float(lv), DbValue::Int(rv)) => lv
            .partial_cmp(&(*rv as f64))
            .ok_or_else(|| "NaN float comparison".to_string()),
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
        (DbValue::Int(lv), DbValue::Int(rv)) => Ok(DbValue::Int(int_op(*lv, *rv))),
        (DbValue::Float(lv), DbValue::Float(rv)) => Ok(DbValue::Float(float_op(*lv, *rv))),
        (DbValue::Int(lv), DbValue::Float(rv)) => Ok(DbValue::Float(float_op(*lv as f64, *rv))),
        (DbValue::Float(lv), DbValue::Int(rv)) => Ok(DbValue::Float(float_op(*lv, *rv as f64))),
        (expected, actual) => Err(format!(
            "Cannot perform arithmetic on non-numeric types: {:?} and {:?}",
            expected, actual
        )),
    }
}
