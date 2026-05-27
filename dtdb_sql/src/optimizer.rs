use crate::expr::{Expr, Operator};
use crate::logical::{JoinType, LogicalPlan, SetOpType};
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

    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut query_columns = HashSet::new();
        plan.collect_columns(&mut query_columns);
        let plan = self.push_down_predicate(plan, Vec::new(), &query_columns);
        let plan = self.optimize_join_order(plan);
        self.eliminate_sorts(plan)
    }

    fn push_down_predicate(
        &self,
        plan: LogicalPlan,
        mut conjuncts: Vec<Expr>,
        query_columns: &HashSet<String>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { source, predicate } => {
                split_conjuncts_rec(predicate, &mut conjuncts);
                self.push_down_predicate(*source, conjuncts, query_columns)
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                ..
            } => {
                let left_schema = left.schema();
                let right_schema = right.schema();
                let mut left_conjuncts = Vec::new();
                let mut right_conjuncts = Vec::new();
                let mut remaining_conjuncts = Vec::new();

                for conj in conjuncts {
                    let cols = referenced_columns(&conj);
                    if cols.is_empty() {
                        remaining_conjuncts.push(conj);
                    } else if cols_subset_of_schema(&cols, &left_schema) {
                        left_conjuncts.push(conj);
                    } else if cols_subset_of_schema(&cols, &right_schema)
                        && (join_type == JoinType::Inner || join_type == JoinType::Cross)
                    {
                        right_conjuncts.push(conj);
                    } else {
                        remaining_conjuncts.push(conj);
                    }
                }

                let opt_left = self.push_down_predicate(*left, left_conjuncts, query_columns);
                let opt_right = self.push_down_predicate(*right, right_conjuncts, query_columns);

                let join_node = LogicalPlan::new_join(opt_left, opt_right, condition, join_type);

                if let Some(pred) = combine_conjuncts(remaining_conjuncts) {
                    LogicalPlan::Filter {
                        source: Box::new(join_node),
                        predicate: pred,
                    }
                } else {
                    join_node
                }
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                ..
            } => {
                let opt_source = self.push_down_predicate(*source, Vec::new(), query_columns);
                let proj_node = LogicalPlan::new_projection(opt_source, expressions, field_names);
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    LogicalPlan::Filter {
                        source: Box::new(proj_node),
                        predicate: pred,
                    }
                } else {
                    proj_node
                }
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
                ..
            } => {
                let opt_source = self.push_down_predicate(*source, Vec::new(), query_columns);
                let aggr_node =
                    LogicalPlan::new_aggregate(opt_source, group_by, aggrs, field_names);
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    LogicalPlan::Filter {
                        source: Box::new(aggr_node),
                        predicate: pred,
                    }
                } else {
                    aggr_node
                }
            }
            LogicalPlan::Sort { source, keys } => {
                let opt_source = self.push_down_predicate(*source, conjuncts, query_columns);
                LogicalPlan::Sort {
                    source: Box::new(opt_source),
                    keys,
                }
            }
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => {
                let opt_source = self.push_down_predicate(*source, Vec::new(), query_columns);
                let limit_node = LogicalPlan::Limit {
                    source: Box::new(opt_source),
                    limit,
                    offset,
                };
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    LogicalPlan::Filter {
                        source: Box::new(limit_node),
                        predicate: pred,
                    }
                } else {
                    limit_node
                }
            }
            LogicalPlan::SetOp {
                left,
                right,
                op,
                all,
            } => {
                let opt_left = self.push_down_predicate(*left, Vec::new(), query_columns);
                let opt_right = self.push_down_predicate(*right, Vec::new(), query_columns);
                let setop_node = LogicalPlan::SetOp {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    op,
                    all,
                };
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    LogicalPlan::Filter {
                        source: Box::new(setop_node),
                        predicate: pred,
                    }
                } else {
                    setop_node
                }
            }
            LogicalPlan::Scan {
                table_name,
                schema,
                range,
            } => {
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    let best_source =
                        self.select_best_scan_path(&table_name, &schema, &pred, query_columns);
                    LogicalPlan::Filter {
                        source: Box::new(best_source),
                        predicate: pred,
                    }
                } else {
                    LogicalPlan::Scan {
                        table_name,
                        schema,
                        range,
                    }
                }
            }
            LogicalPlan::IndexScan {
                table_name,
                index_name,
                schema,
                range,
            } => {
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    let best_source =
                        self.select_best_scan_path(&table_name, &schema, &pred, query_columns);
                    LogicalPlan::Filter {
                        source: Box::new(best_source),
                        predicate: pred,
                    }
                } else {
                    LogicalPlan::IndexScan {
                        table_name,
                        index_name,
                        schema,
                        range,
                    }
                }
            }
            LogicalPlan::FullTextScan {
                table_name,
                index_name,
                schema,
                query_str,
            } => {
                if let Some(pred) = combine_conjuncts(conjuncts) {
                    let best_source =
                        self.select_best_scan_path(&table_name, &schema, &pred, query_columns);
                    LogicalPlan::Filter {
                        source: Box::new(best_source),
                        predicate: pred,
                    }
                } else {
                    LogicalPlan::FullTextScan {
                        table_name,
                        index_name,
                        schema,
                        query_str,
                    }
                }
            }
        }
    }

    fn optimize_join_order(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                ..
            } => {
                let opt_left = self.optimize_join_order(*left);
                let opt_right = self.optimize_join_order(*right);

                if join_type == JoinType::Inner {
                    let left_rows = self.estimate_plan_rows(&opt_left);
                    let right_rows = self.estimate_plan_rows(&opt_right);

                    if left_rows < right_rows {
                        let original_schema = LogicalPlan::new_join(
                            opt_left.clone(),
                            opt_right.clone(),
                            condition.clone(),
                            join_type,
                        )
                        .schema();

                        let swapped_cond = swap_join_condition(condition);
                        let swapped_join =
                            LogicalPlan::new_join(opt_right, opt_left, swapped_cond, join_type);

                        let expressions = original_schema
                            .columns
                            .iter()
                            .map(|col| Expr::Column(col.name.clone(), None))
                            .collect();
                        let field_names = original_schema
                            .columns
                            .iter()
                            .map(|col| col.name.clone())
                            .collect();

                        LogicalPlan::new_projection(swapped_join, expressions, field_names)
                    } else {
                        LogicalPlan::new_join(opt_left, opt_right, condition, join_type)
                    }
                } else {
                    LogicalPlan::new_join(opt_left, opt_right, condition, join_type)
                }
            }
            LogicalPlan::Filter { source, predicate } => LogicalPlan::Filter {
                source: Box::new(self.optimize_join_order(*source)),
                predicate,
            },
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                ..
            } => LogicalPlan::new_projection(
                self.optimize_join_order(*source),
                expressions,
                field_names,
            ),
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
                ..
            } => LogicalPlan::new_aggregate(
                self.optimize_join_order(*source),
                group_by,
                aggrs,
                field_names,
            ),
            LogicalPlan::Sort { source, keys } => LogicalPlan::Sort {
                source: Box::new(self.optimize_join_order(*source)),
                keys,
            },
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => LogicalPlan::Limit {
                source: Box::new(self.optimize_join_order(*source)),
                limit,
                offset,
            },
            LogicalPlan::SetOp {
                left,
                right,
                op,
                all,
            } => LogicalPlan::SetOp {
                left: Box::new(self.optimize_join_order(*left)),
                right: Box::new(self.optimize_join_order(*right)),
                op,
                all,
            },
            other => other,
        }
    }

    fn estimate_plan_rows(&self, plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::Scan {
                table_name, range, ..
            }
            | LogicalPlan::IndexScan {
                table_name, range, ..
            } => {
                let base = self
                    .database
                    .get_table_statistics(table_name)
                    .map(|s| s.row_count)
                    .unwrap_or(1000);
                if range.is_some() {
                    (base as f64 * 0.1) as usize
                } else {
                    base as usize
                }
            }
            LogicalPlan::FullTextScan { table_name, .. } => {
                let base = self
                    .database
                    .get_table_statistics(table_name)
                    .map(|s| s.row_count)
                    .unwrap_or(1000);
                (base as f64 * 0.05) as usize
            }
            LogicalPlan::Filter { source, .. } => {
                (self.estimate_plan_rows(source) as f64 * 0.2) as usize
            }
            LogicalPlan::Projection { source, .. } => self.estimate_plan_rows(source),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                let left_rows = self.estimate_plan_rows(left);
                let right_rows = self.estimate_plan_rows(right);
                match join_type {
                    JoinType::Inner => std::cmp::min(left_rows, right_rows),
                    JoinType::Left => left_rows,
                    JoinType::Cross => left_rows * right_rows,
                }
            }
            LogicalPlan::Aggregate {
                source, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    (self.estimate_plan_rows(source) as f64 * 0.1) as usize
                }
            }
            LogicalPlan::Sort { source, .. } | LogicalPlan::Limit { source, .. } => {
                self.estimate_plan_rows(source)
            }
            LogicalPlan::SetOp {
                left, right, op, ..
            } => {
                let left_rows = self.estimate_plan_rows(left);
                let right_rows = self.estimate_plan_rows(right);
                match op {
                    SetOpType::Union => left_rows + right_rows,
                    SetOpType::Intersect => std::cmp::min(left_rows, right_rows),
                    SetOpType::Except => left_rows,
                }
            }
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
                && let Some(col) = schema.columns.iter().find(|c| {
                    c.name == *col_name
                        || dtdb_relational::schema::ends_with_dot_suffix(&c.name, col_name)
                })
            {
                // Verify index type match
                let is_fulltext = index.index_type == dtdb_relational::schema::IndexType::FullText;
                let has_match = Self::has_match_predicate_for_col(predicate, &col.name);
                if is_fulltext != has_match {
                    continue;
                }

                if is_fulltext {
                    if let Some(query_str) =
                        Self::extract_fulltext_query_str_for_col(predicate, &col.name)
                    {
                        let fts_plan = LogicalPlan::FullTextScan {
                            table_name: table_name.to_string(),
                            index_name: index.name.clone(),
                            schema: schema.clone(),
                            query_str,
                        };
                        // FullTextScan has a very low cost as it resolves boolean terms using index set operations
                        let cost = 5.0;
                        if cost < min_cost {
                            min_cost = cost;
                            best_plan = fts_plan;
                        }
                    }
                } else {
                    if let Some((start, end)) =
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
            }
        }

        best_plan
    }

    fn has_match_predicate_for_col(predicate: &Expr, col_name: &str) -> bool {
        match predicate {
            Expr::Match { column, .. } => {
                column == col_name
                    || dtdb_relational::schema::ends_with_dot_suffix(column, col_name)
                    || dtdb_relational::schema::ends_with_dot_suffix(col_name, column)
            }
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => {
                Self::has_match_predicate_for_col(left, col_name)
                    || Self::has_match_predicate_for_col(right, col_name)
            }
            Expr::Not(inner) => Self::has_match_predicate_for_col(inner, col_name),
            _ => false,
        }
    }

    fn extract_fulltext_query_str_for_col(predicate: &Expr, col_name: &str) -> Option<String> {
        match predicate {
            Expr::Match {
                column, query_str, ..
            } => {
                if column == col_name
                    || dtdb_relational::schema::ends_with_dot_suffix(column, col_name)
                    || dtdb_relational::schema::ends_with_dot_suffix(col_name, column)
                {
                    Some(query_str.clone())
                } else {
                    None
                }
            }
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => Self::extract_fulltext_query_str_for_col(left, col_name)
                .or_else(|| Self::extract_fulltext_query_str_for_col(right, col_name)),
            Expr::Not(inner) => Self::extract_fulltext_query_str_for_col(inner, col_name),
            _ => None,
        }
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
                    || dtdb_relational::schema::ends_with_dot_suffix(q_col, &col.name)
                    || dtdb_relational::schema::ends_with_dot_suffix(&col.name, q_col)
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

    fn eliminate_sorts(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Sort { source, keys } => {
                let opt_source = self.eliminate_sorts(*source);

                if keys.len() == 1 && keys[0].1 {
                    let sort_expr = &keys[0].0;
                    if let Expr::Column(sort_col, _) = sort_expr {
                        // 1. Check if opt_source is already sorted by sort_col (ASC)
                        if let Some(sorted_col) = Self::get_plan_sort_key(&opt_source)
                            && (sorted_col == *sort_col
                                || dtdb_relational::schema::ends_with_dot_suffix(
                                    sort_col,
                                    &sorted_col,
                                )
                                || dtdb_relational::schema::ends_with_dot_suffix(
                                    &sorted_col,
                                    sort_col,
                                ))
                        {
                            return opt_source;
                        }

                        // 2. Otherwise, check if we can promote the underlying scan to an IndexScan on sort_col.
                        if let Some(promoted) =
                            self.try_promote_to_index_scan(opt_source.clone(), sort_col)
                        {
                            return promoted;
                        }
                    }
                }

                LogicalPlan::Sort {
                    source: Box::new(opt_source),
                    keys,
                }
            }
            LogicalPlan::Filter { source, predicate } => LogicalPlan::Filter {
                source: Box::new(self.eliminate_sorts(*source)),
                predicate,
            },
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                schema,
            } => LogicalPlan::Projection {
                source: Box::new(self.eliminate_sorts(*source)),
                expressions,
                field_names,
                schema,
            },
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(self.eliminate_sorts(*left)),
                right: Box::new(self.eliminate_sorts(*right)),
                condition,
                join_type,
                schema,
            },
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
                schema,
            } => {
                let opt_source = self.eliminate_sorts(*source);
                if group_by.len() == 1
                    && let Expr::Column(group_col, _) = &group_by[0]
                    && let Some(promoted) =
                        self.try_promote_to_index_scan(opt_source.clone(), group_col)
                {
                    return LogicalPlan::Aggregate {
                        source: Box::new(promoted),
                        group_by,
                        aggrs,
                        field_names,
                        schema,
                    };
                }
                LogicalPlan::Aggregate {
                    source: Box::new(opt_source),
                    group_by,
                    aggrs,
                    field_names,
                    schema,
                }
            }
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => LogicalPlan::Limit {
                source: Box::new(self.eliminate_sorts(*source)),
                limit,
                offset,
            },
            LogicalPlan::SetOp {
                left,
                right,
                op,
                all,
            } => LogicalPlan::SetOp {
                left: Box::new(self.eliminate_sorts(*left)),
                right: Box::new(self.eliminate_sorts(*right)),
                op,
                all,
            },
            other => other,
        }
    }

    pub(crate) fn get_plan_sort_key(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::Scan {
                schema, range: _, ..
            } => schema
                .primary_key_index()
                .map(|pk_idx| schema.columns[pk_idx].name.clone()),
            LogicalPlan::IndexScan {
                schema, index_name, ..
            } => {
                if let Some(index) = schema.indexes.iter().find(|idx| &idx.name == index_name)
                    && let Some(first_col) = index.columns.first()
                {
                    Some(first_col.clone())
                } else {
                    None
                }
            }
            LogicalPlan::Filter { source, .. } => Self::get_plan_sort_key(source),
            LogicalPlan::Limit { source, .. } => Self::get_plan_sort_key(source),
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                ..
            } => {
                let child_sort_key = Self::get_plan_sort_key(source)?;
                for (idx, expr) in expressions.iter().enumerate() {
                    if let Expr::Column(name, _) = expr
                        && (name == &child_sort_key
                            || dtdb_relational::schema::ends_with_dot_suffix(name, &child_sort_key)
                            || dtdb_relational::schema::ends_with_dot_suffix(&child_sort_key, name))
                    {
                        return Some(field_names[idx].clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn try_promote_to_index_scan(&self, plan: LogicalPlan, sort_col: &str) -> Option<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                range,
            } => {
                if range.is_none() {
                    for index in &schema.indexes {
                        if let Some(first_col) = index.columns.first()
                            && (first_col == sort_col
                                || dtdb_relational::schema::ends_with_dot_suffix(
                                    first_col, sort_col,
                                )
                                || dtdb_relational::schema::ends_with_dot_suffix(
                                    sort_col, first_col,
                                ))
                        {
                            return Some(LogicalPlan::IndexScan {
                                table_name: table_name.clone(),
                                index_name: index.name.clone(),
                                schema: schema.clone(),
                                range: None,
                            });
                        }
                    }
                }
                None
            }
            LogicalPlan::Filter { source, predicate } => {
                let promoted_source = self.try_promote_to_index_scan(*source, sort_col)?;
                Some(LogicalPlan::Filter {
                    source: Box::new(promoted_source),
                    predicate,
                })
            }
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => {
                let promoted_source = self.try_promote_to_index_scan(*source, sort_col)?;
                Some(LogicalPlan::Limit {
                    source: Box::new(promoted_source),
                    limit,
                    offset,
                })
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                schema,
            } => {
                let mut source_col_name = None;
                for (idx, expr) in expressions.iter().enumerate() {
                    if field_names[idx] == sort_col
                        && let Expr::Column(orig_name, _) = expr
                    {
                        source_col_name = Some(orig_name.clone());
                        break;
                    }
                }
                if let Some(orig_col) = source_col_name {
                    let promoted_source = self.try_promote_to_index_scan(*source, &orig_col)?;
                    Some(LogicalPlan::Projection {
                        source: Box::new(promoted_source),
                        expressions,
                        field_names,
                        schema,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
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
        (Expr::Column(name, _), Expr::Literal(lit))
            if name == col_name
                || dtdb_relational::schema::ends_with_dot_suffix(name, col_name)
                || dtdb_relational::schema::ends_with_dot_suffix(col_name, name) =>
        {
            Some(lit)
        }
        (Expr::Literal(lit), Expr::Column(name, _))
            if name == col_name
                || dtdb_relational::schema::ends_with_dot_suffix(name, col_name)
                || dtdb_relational::schema::ends_with_dot_suffix(col_name, name) =>
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
                                DbKey::Int(v) => DbKey::Int(v.saturating_add(1)),
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
                                DbKey::Int(v) => DbKey::Int(v.saturating_sub(1)),
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
        Expr::Match { .. } => None,
        _ => None,
    }
}

/// Helper to examine an expression tree and extract range constraints for the primary key.
fn extract_key_range(predicate: &Expr, schema: &Schema) -> Option<(DbKey, DbKey)> {
    let pk_idx = schema.primary_key_index()?;
    let pk_col = &schema.columns[pk_idx];
    extract_bounds_for_column(predicate, &pk_col.name, &pk_col.data_type)
}

fn split_conjuncts_rec(expr: Expr, conjuncts: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            split_conjuncts_rec(*left, conjuncts);
            split_conjuncts_rec(*right, conjuncts);
        }
        other => conjuncts.push(other),
    }
}

fn combine_conjuncts(mut conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut expr = conjuncts.remove(0);
    for conj in conjuncts {
        expr = Expr::BinaryOp {
            left: Box::new(expr),
            op: Operator::And,
            right: Box::new(conj),
        };
    }
    Some(expr)
}

fn referenced_columns(expr: &Expr) -> HashSet<String> {
    let mut cols = HashSet::new();
    expr.collect_columns(&mut cols);
    cols
}

fn cols_subset_of_schema(cols: &HashSet<String>, schema: &Schema) -> bool {
    cols.iter().all(|col| schema_contains_col(schema, col))
}

fn schema_contains_col(schema: &Schema, col_name: &str) -> bool {
    schema.columns.iter().any(|col| {
        col.name == col_name
            || dtdb_relational::schema::ends_with_dot_suffix(col_name, &col.name)
            || dtdb_relational::schema::ends_with_dot_suffix(&col.name, col_name)
    })
}

fn swap_join_condition(condition: Expr) -> Expr {
    match condition {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: right,
            op,
            right: left,
        },
        other => other,
    }
}
