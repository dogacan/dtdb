use crate::expr::{Expr, Operator};
use crate::logical::LogicalPlan;
use dtdb_relational::{DataType, Database, Schema};
use dtdb_storage::DbKey;
use dtdb_storage::DbValue;
use std::collections::HashSet;
use std::sync::Arc;

/// Logical Optimizer applies cost-based and rule-based optimizations to a LogicalPlan.
pub struct Optimizer {
    database: Arc<Database>,
}

impl Optimizer {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Optimizes a LogicalPlan recursively.
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut query_columns = HashSet::new();
        plan.collect_columns(&mut query_columns);
        self.optimize_node(plan, &query_columns)
    }

    fn optimize_node(&self, plan: LogicalPlan, query_columns: &HashSet<String>) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { source, predicate } => {
                let opt_source = self.optimize_node(*source, query_columns);
                match opt_source {
                    LogicalPlan::Scan {
                        table_name,
                        schema,
                        range: _,
                    } => {
                        let best_source = self.select_best_scan_path(
                            &table_name,
                            &schema,
                            &predicate,
                            query_columns,
                        );
                        LogicalPlan::Filter {
                            source: Box::new(best_source),
                            predicate,
                        }
                    }
                    other => LogicalPlan::Filter {
                        source: Box::new(other),
                        predicate,
                    },
                }
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
            } => LogicalPlan::Projection {
                source: Box::new(self.optimize_node(*source, query_columns)),
                expressions,
                field_names,
            },
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.optimize_node(*left, query_columns)),
                right: Box::new(self.optimize_node(*right, query_columns)),
                condition,
                join_type,
            },
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
            } => LogicalPlan::Aggregate {
                source: Box::new(self.optimize_node(*source, query_columns)),
                group_by,
                aggrs,
                field_names,
            },
            LogicalPlan::Sort { source, keys } => LogicalPlan::Sort {
                source: Box::new(self.optimize_node(*source, query_columns)),
                keys,
            },
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => LogicalPlan::Limit {
                source: Box::new(self.optimize_node(*source, query_columns)),
                limit,
                offset,
            },
            LogicalPlan::SetOp {
                left,
                right,
                op,
                all,
            } => LogicalPlan::SetOp {
                left: Box::new(self.optimize_node(*left, query_columns)),
                right: Box::new(self.optimize_node(*right, query_columns)),
                op,
                all,
            },
            other => other,
        }
    }

    fn select_best_scan_path(
        &self,
        table_name: &str,
        schema: &Schema,
        predicate: &Expr,
        query_columns: &HashSet<String>,
    ) -> LogicalPlan {
        let stats_opt = self.database.get_table_statistics(table_name);

        // 1. Calculate Full Scan Cost
        let full_scan_plan = LogicalPlan::Scan {
            table_name: table_name.to_string(),
            schema: schema.clone(),
            range: None,
        };
        let mut min_cost = self.estimate_scan_cost(
            table_name,
            schema,
            stats_opt.as_ref(),
            &full_scan_plan,
            query_columns,
        );
        let mut best_plan = full_scan_plan;

        // 2. Calculate PK Range Scan Cost
        if let Some((start, end)) = extract_key_range(predicate, schema) {
            let pk_plan = LogicalPlan::Scan {
                table_name: table_name.to_string(),
                schema: schema.clone(),
                range: Some((start.clone(), end.clone())),
            };
            let cost = self.estimate_scan_cost(
                table_name,
                schema,
                stats_opt.as_ref(),
                &pk_plan,
                query_columns,
            );
            if cost < min_cost {
                min_cost = cost;
                best_plan = pk_plan;
            }
        }

        // 3. Calculate Secondary Index Scan Costs
        for index in &schema.indexes {
            if let Some(col_name) = index.columns.first()
                && let Some(col) = schema
                    .columns
                    .iter()
                    .find(|c| c.name == *col_name || c.name.ends_with(&format!(".{}", col_name)))
                && let Some((start, end)) =
                    extract_bounds_for_column(predicate, &col.name, &col.data_type)
            {
                let idx_plan = LogicalPlan::IndexScan {
                    table_name: table_name.to_string(),
                    index_name: index.name.clone(),
                    schema: schema.clone(),
                    range: Some((start.clone(), end.clone())),
                };
                let cost = self.estimate_index_scan_cost(
                    table_name,
                    &index.name,
                    schema,
                    stats_opt.as_ref(),
                    &start,
                    &end,
                    query_columns,
                );
                if cost < min_cost {
                    min_cost = cost;
                    best_plan = idx_plan;
                }
            }
        }

        best_plan
    }

    fn get_needed_locality_groups(
        &self,
        schema: &Schema,
        query_columns: &HashSet<String>,
    ) -> HashSet<String> {
        let mut needed_groups = HashSet::new();
        for col in &schema.columns {
            let matches_query = query_columns.iter().any(|q_col| {
                q_col == &col.name
                    || q_col.ends_with(&format!(".{}", col.name))
                    || col.name.ends_with(&format!(".{}", q_col))
            });
            if matches_query {
                needed_groups.insert(col.locality_group.as_deref().unwrap_or("").to_string());
            }
        }
        if needed_groups.is_empty() {
            needed_groups.insert("".to_string());
        }
        needed_groups
    }

    fn estimate_scan_cost(
        &self,
        _table_name: &str,
        schema: &Schema,
        stats_opt: Option<&dtdb_relational::TableStatistics>,
        plan: &LogicalPlan,
        query_columns: &HashSet<String>,
    ) -> f64 {
        let range = match plan {
            LogicalPlan::Scan { range, .. } => range,
            _ => &None,
        };

        let needed_groups = self.get_needed_locality_groups(schema, query_columns);
        let mut base_cost = 0.0;
        let row_count = stats_opt.map(|s| s.row_count).unwrap_or(0);

        if let Some(stats) = stats_opt {
            for group in &needed_groups {
                if let Some(g_stats) = stats.locality_group_stats.get(group) {
                    base_cost += g_stats.total_sstable_size as f64 * 1.0; // SEQ_READ_FACTOR
                } else {
                    base_cost += 1000.0;
                }
            }
            base_cost += row_count as f64 * 0.1; // FILTER_CPU_FACTOR
        } else {
            base_cost = 1000.0;
        }

        if let Some((start, end)) = range {
            let mut s = 0.05; // default closed range
            if start == end {
                s = 1.0 / (row_count.max(1) as f64);
            } else {
                let is_start_unbounded = is_key_unbounded_min(start);
                let is_end_unbounded = is_key_unbounded_max(end);
                if is_start_unbounded && is_end_unbounded {
                    s = 1.0;
                } else if is_start_unbounded || is_end_unbounded {
                    s = 0.33;
                }
            }
            base_cost * s
        } else {
            base_cost
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn estimate_index_scan_cost(
        &self,
        _table_name: &str,
        index_name: &str,
        schema: &Schema,
        stats_opt: Option<&dtdb_relational::TableStatistics>,
        start: &DbKey,
        end: &DbKey,
        query_columns: &HashSet<String>,
    ) -> f64 {
        let row_count = stats_opt.map(|s| s.row_count).unwrap_or(0);

        let mut s_idx = 0.05; // default closed range
        if start == end {
            if let Some(stats) = stats_opt
                && let Some(idx_stats) = stats.index_stats.get(index_name)
            {
                s_idx = idx_stats.avg_rows_per_value / (row_count.max(1) as f64);
            } else {
                s_idx = 1.0 / (row_count.max(1) as f64);
            }
        } else {
            let is_start_unbounded = is_key_unbounded_min(start);
            let is_end_unbounded = is_key_unbounded_max(end);
            if is_start_unbounded && is_end_unbounded {
                s_idx = 1.0;
            } else if is_start_unbounded || is_end_unbounded {
                s_idx = 0.33;
            }
        }

        let n_match = s_idx * (row_count as f64);

        let index_entries = stats_opt
            .and_then(|s| s.index_stats.get(index_name))
            .map(|i| i.entry_count)
            .unwrap_or(row_count);
        let index_io = (index_entries as f64 * 16.0 * s_idx * 1.0).max(10.0);

        let needed_groups = self.get_needed_locality_groups(schema, query_columns);
        let mut table_io = 0.0;
        if let Some(stats) = stats_opt {
            for group in &needed_groups {
                if let Some(g_stats) = stats.locality_group_stats.get(group) {
                    let random_factor = 10.0
                        * (g_stats.total_sstable_size as f64 / (row_count.max(1) as f64)).max(10.0);
                    table_io += n_match * random_factor;
                } else {
                    table_io += n_match * 100.0;
                }
            }
        } else {
            table_io += n_match * 50.0;
        }

        index_io + table_io + n_match * 0.1
    }
}

fn is_key_unbounded_min(key: &DbKey) -> bool {
    match key {
        DbKey::Int(v) => *v == i64::MIN,
        DbKey::String(s) => s.is_empty(),
        DbKey::Bool(b) => !*b,
        DbKey::Composite(parts) => parts.first().is_none_or(is_key_unbounded_min),
    }
}

fn is_key_unbounded_max(key: &DbKey) -> bool {
    match key {
        DbKey::Int(v) => *v == i64::MAX,
        DbKey::String(s) => s == "\u{10ffff}",
        DbKey::Bool(b) => *b,
        DbKey::Composite(parts) => parts.first().is_none_or(is_key_unbounded_max),
    }
}

fn get_column_comparison<'a>(
    left: &'a Expr,
    right: &'a Expr,
    col_name: &str,
) -> Option<&'a DbValue> {
    match (left, right) {
        (Expr::Column(name), Expr::Literal(lit))
            if name == col_name
                || name.ends_with(&format!(".{}", col_name))
                || col_name.ends_with(&format!(".{}", name)) =>
        {
            Some(lit)
        }
        (Expr::Literal(lit), Expr::Column(name))
            if name == col_name
                || name.ends_with(&format!(".{}", col_name))
                || col_name.ends_with(&format!(".{}", name)) =>
        {
            Some(lit)
        }
        _ => None,
    }
}

fn val_to_key(val: &DbValue) -> Option<DbKey> {
    match val {
        DbValue::Int(v) => Some(DbKey::Int(*v)),
        DbValue::String(s) => Some(DbKey::String(s.clone())),
        _ => None,
    }
}

/// Helper to examine an expression tree and extract range constraints for the given column.
fn extract_bounds_for_column(
    predicate: &Expr,
    col_name: &str,
    col_type: &DataType,
) -> Option<(DbKey, DbKey)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            let left_bounds = extract_bounds_for_column(left, col_name, col_type);
            let right_bounds = extract_bounds_for_column(right, col_name, col_type);
            match (left_bounds, right_bounds) {
                (Some((l_start, l_end)), Some((r_start, r_end))) => {
                    let start = std::cmp::max(l_start, r_start);
                    let end = std::cmp::min(l_end, r_end);
                    Some((start, end))
                }
                (Some(bounds), None) => Some(bounds),
                (None, Some(bounds)) => Some(bounds),
                (None, None) => None,
            }
        }
        Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } => {
            if let Some(lit) = get_column_comparison(left, right, col_name) {
                let key = val_to_key(lit)?;
                Some((key.clone(), key))
            } else {
                None
            }
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(lit) = get_column_comparison(left, right, col_name) {
                let key = val_to_key(lit)?;
                match op {
                    Operator::GtEq | Operator::Gt => {
                        let start = if matches!(op, Operator::Gt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v + 1),
                                DbKey::String(s) => DbKey::String(s + "\0"),
                                _ => key,
                            }
                        } else {
                            key
                        };
                        let end = match col_type {
                            DataType::Int => DbKey::Int(i64::MAX),
                            _ => DbKey::String("\u{10ffff}".to_string()),
                        };
                        Some((start, end))
                    }
                    Operator::LtEq | Operator::Lt => {
                        let start = match col_type {
                            DataType::Int => DbKey::Int(i64::MIN),
                            _ => DbKey::String("".to_string()),
                        };
                        let end = if matches!(op, Operator::Lt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v - 1),
                                DbKey::String(s) => DbKey::String(s),
                                _ => key,
                            }
                        } else {
                            key
                        };
                        Some((start, end))
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper to examine an expression tree and extract range constraints for the primary key.
fn extract_key_range(predicate: &Expr, schema: &Schema) -> Option<(DbKey, DbKey)> {
    let pk_idx = schema.primary_key_index()?;
    let pk_col = &schema.columns[pk_idx];
    extract_bounds_for_column(predicate, &pk_col.name, &pk_col.data_type)
}
