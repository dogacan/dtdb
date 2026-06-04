use crate::expr::{Expr, Operator};
use crate::logical::{FtsQuery, JoinType, LogicalPlan, PlanKey, SetOpType};
use chrono;
use dtdb_relational::{DataType, Database, Schema};
use dtdb_storage::DbKey;
use dtdb_storage::DbValue;
use rust_decimal;
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
            LogicalPlan::Distinct { source } => {
                let opt_source = self.push_down_predicate(*source, conjuncts, query_columns);
                LogicalPlan::Distinct {
                    source: Box::new(opt_source),
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
                query,
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
                        query,
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
            LogicalPlan::Distinct { source } => LogicalPlan::Distinct {
                source: Box::new(self.optimize_join_order(*source)),
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
            LogicalPlan::Distinct { source } => {
                (self.estimate_plan_rows(source) as f64 * 0.8) as usize
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
                && let Some(col) = schema.columns.iter().find(|c| c.matches_name(col_name))
            {
                let is_fulltext = index.index_type == dtdb_relational::IndexType::FullText;
                let has_match = Self::has_match_predicate_for_col(predicate, &col.name);

                if is_fulltext {
                    if has_match {
                        // A MATCH predicate resolves as a boolean FULLTEXT query.
                        if let Some(query_str) =
                            Self::extract_fulltext_query_str_for_col(predicate, &col.name)
                        {
                            let fts_plan = LogicalPlan::FullTextScan {
                                table_name: table_name.to_string(),
                                index_name: index.name.clone(),
                                schema: schema.clone(),
                                query: FtsQuery::Match(query_str),
                            };
                            // FullTextScan has a very low cost as it resolves boolean terms using index set operations
                            let cost = 5.0;
                            if cost < min_cost {
                                min_cost = cost;
                                best_plan = fts_plan;
                            }
                        }
                    } else if let Some((plan, cost)) = self.try_trigram_like_plan(
                        table_name,
                        schema,
                        index,
                        &col.name,
                        predicate,
                        stats_opt.as_ref(),
                        query_columns,
                    ) {
                        // A substring LIKE on a fulltext index whose tokenizer
                        // can compile the pattern into intersectable tokens.
                        if cost < min_cost {
                            min_cost = cost;
                            best_plan = plan;
                        }
                    }
                } else if !has_match
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
        }

        best_plan
    }

    fn has_match_predicate_for_col(predicate: &Expr, col_name: &str) -> bool {
        match predicate {
            Expr::Match { column, .. } => dtdb_relational::column_names_match(column, col_name),
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
                if dtdb_relational::column_names_match(column, col_name) {
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

    /// Find a `col LIKE 'literal'` predicate for `col_name` and return its
    /// pattern. Recurses through AND but deliberately not through NOT: a
    /// negated LIKE cannot be served by a token-intersection (superset) plan.
    fn extract_like_pattern_for_col(predicate: &Expr, col_name: &str) -> Option<String> {
        match predicate {
            Expr::BinaryOp {
                left,
                op: Operator::Like,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name, _), Expr::Literal(DbValue::String(pattern)))
                    if dtdb_relational::column_names_match(name, col_name) =>
                {
                    Some(pattern.to_string())
                }
                _ => None,
            },
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => Self::extract_like_pattern_for_col(left, col_name)
                .or_else(|| Self::extract_like_pattern_for_col(right, col_name)),
            _ => None,
        }
    }

    /// Build a token-intersection plan for a substring LIKE on a fulltext index,
    /// when the index's tokenizer can compile the pattern (e.g. trigrams) and
    /// up-to-date statistics exist to cost it. Returns `(plan, cost)`, or `None`
    /// to fall back to a full scan + filter.
    ///
    /// Cost is keyed off the most-common-value statistics: the candidate set is
    /// bounded by the *rarest* required token's posting list (an intersection
    /// cannot exceed its smallest input), so a selective token makes the plan
    /// cheap while an all-common pattern (e.g. `%the%`) estimates near a full
    /// scan and loses. Without statistics we decline rather than risk that
    /// footgun. The residual LIKE filter the caller wraps around this plan
    /// performs the mandatory recheck.
    #[allow(clippy::too_many_arguments)]
    fn try_trigram_like_plan(
        &self,
        table_name: &str,
        schema: &Schema,
        index: &dtdb_relational::IndexDefinition,
        col_name: &str,
        predicate: &Expr,
        stats_opt: Option<&dtdb_relational::TableStatistics>,
        query_columns: &HashSet<String>,
    ) -> Option<(LogicalPlan, f64)> {
        // No statistics -> cannot tell a selective token from a common one, so
        // decline and let the full scan + filter handle it.
        let stats = stats_opt?;
        let idx_stats = stats.index_stats.get(&index.name)?;

        let pattern = Self::extract_like_pattern_for_col(predicate, col_name)?;
        let tokenizer_name = index.tokenizer.as_deref().unwrap_or("simple");
        let tokenizer = dtdb_relational::get_tokenizer(tokenizer_name)?;
        let required = tokenizer.plan_like(&pattern)?.required;
        if required.is_empty() {
            return None;
        }

        let posting_len = |token: &str| idx_stats.posting_len_upper_bound(&[DbKey::string(token)]);
        // Intersection size is bounded by the rarest token; we still walk every
        // required token's posting list.
        let candidate_rows = required
            .iter()
            .map(|t| posting_len(t))
            .min()
            .unwrap_or(stats.row_count);
        let postings_touched: u64 = required.iter().map(|t| posting_len(t)).sum();

        // Mirror estimate_index_scan_cost so the plan competes on equal footing:
        // posting-list scan + random-I/O row fetches + recheck CPU.
        let row_count = stats.row_count;
        let index_io = (postings_touched as f64 * 16.0).max(10.0);
        let needed_groups = self.get_needed_locality_groups(schema, query_columns);
        let mut table_io = 0.0;
        for group in &needed_groups {
            if let Some(g_stats) = stats.locality_group_stats.get(group) {
                let random_factor = 10.0
                    * (g_stats.total_sstable_size as f64 / (row_count.max(1) as f64)).max(10.0);
                table_io += candidate_rows as f64 * random_factor;
            } else {
                table_io += candidate_rows as f64 * 100.0;
            }
        }
        let cost = index_io + table_io + candidate_rows as f64 * 0.1;

        let plan = LogicalPlan::FullTextScan {
            table_name: table_name.to_string(),
            index_name: index.name.clone(),
            schema: schema.clone(),
            query: FtsQuery::AllTokens(required),
        };
        Some((plan, cost))
    }

    fn get_needed_locality_groups(
        &self,
        schema: &Schema,
        query_columns: &HashSet<String>,
    ) -> HashSet<String> {
        let mut needed_groups = HashSet::new();
        for col in &schema.columns {
            let matches_query = query_columns.iter().any(|q_col| col.matches_name(q_col));
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
                // Exact-match point lookup: at most one row. Fall back to a
                // large assumed cardinality when statistics are absent so this
                // stays highly selective (and thus far cheaper than a full
                // scan) instead of collapsing to a selectivity of 1.0.
                s = 1.0 / (effective_row_count(row_count) as f64);
            } else {
                let is_start_unbounded = is_plan_key_unbounded_min(start);
                let is_end_unbounded = is_plan_key_unbounded_max(end);
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
        start: &PlanKey,
        end: &PlanKey,
        query_columns: &HashSet<String>,
    ) -> f64 {
        let row_count = stats_opt.map(|s| s.row_count).unwrap_or(0);

        let mut s_idx = 0.05; // default closed range
        if start == end {
            if let Some(stats) = stats_opt
                && let Some(idx_stats) = stats.index_stats.get(index_name)
            {
                s_idx = idx_stats.avg_rows_per_value / (effective_row_count(row_count) as f64);
            } else {
                s_idx = 1.0 / (effective_row_count(row_count) as f64);
            }
        } else {
            let is_start_unbounded = is_plan_key_unbounded_min(start);
            let is_end_unbounded = is_plan_key_unbounded_max(end);
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
                            && dtdb_relational::column_names_match(&sorted_col, sort_col)
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
            LogicalPlan::Distinct { source } => LogicalPlan::Distinct {
                source: Box::new(self.eliminate_sorts(*source)),
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
            LogicalPlan::Filter { source, .. }
            | LogicalPlan::Limit { source, .. }
            | LogicalPlan::Distinct { source } => Self::get_plan_sort_key(source),
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                ..
            } => {
                let child_sort_key = Self::get_plan_sort_key(source)?;
                for (idx, expr) in expressions.iter().enumerate() {
                    if let Expr::Column(name, _) = expr
                        && dtdb_relational::column_names_match(name, &child_sort_key)
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
                            && dtdb_relational::column_names_match(first_col, sort_col)
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
            LogicalPlan::Distinct { source } => {
                let promoted_source = self.try_promote_to_index_scan(*source, sort_col)?;
                Some(LogicalPlan::Distinct {
                    source: Box::new(promoted_source),
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
        DbKey::Date(d) => *d == chrono::NaiveDate::MIN,
        DbKey::Time(t) => *t == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        DbKey::Timestamp(ts) => *ts == chrono::NaiveDateTime::MIN,
        DbKey::Decimal(dec) => *dec == rust_decimal::Decimal::MIN,
        DbKey::Composite(parts) => parts.first().is_none_or(is_key_unbounded_min),
    }
}

fn is_key_unbounded_max(key: &DbKey) -> bool {
    match key {
        DbKey::Int(v) => *v == i64::MAX,
        DbKey::String(s) => &**s == "\u{10ffff}",
        DbKey::Bool(b) => *b,
        DbKey::Date(d) => *d == chrono::NaiveDate::MAX,
        DbKey::Time(t) => {
            *t == chrono::NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap()
        }
        DbKey::Timestamp(ts) => *ts == chrono::NaiveDateTime::MAX,
        DbKey::Decimal(dec) => *dec == rust_decimal::Decimal::MAX,
        DbKey::Composite(parts) => parts.first().is_none_or(is_key_unbounded_max),
    }
}

fn is_plan_key_unbounded_min(key: &PlanKey) -> bool {
    match key {
        PlanKey::Value(k) => is_key_unbounded_min(k),
        PlanKey::Parameter(_) => false,
    }
}

fn is_plan_key_unbounded_max(key: &PlanKey) -> bool {
    match key {
        PlanKey::Value(k) => is_key_unbounded_max(k),
        PlanKey::Parameter(_) => false,
    }
}

fn get_column_comparison(left: &Expr, right: &Expr, col_name: &str) -> Option<PlanKey> {
    match (left, right) {
        (Expr::Column(name, _), Expr::Literal(lit))
        | (Expr::Literal(lit), Expr::Column(name, _))
            if dtdb_relational::column_names_match(name, col_name) =>
        {
            val_to_key(lit).map(PlanKey::Value)
        }
        (Expr::Column(name, _), Expr::Parameter(p))
        | (Expr::Parameter(p), Expr::Column(name, _))
            if dtdb_relational::column_names_match(name, col_name) =>
        {
            Some(PlanKey::Parameter(p.clone()))
        }
        _ => None,
    }
}

fn val_to_key(val: &DbValue) -> Option<DbKey> {
    match val {
        DbValue::Int(v) => Some(DbKey::Int(*v)),
        DbValue::String(s) => Some(DbKey::String(s.clone())),
        DbValue::Date(d) => Some(DbKey::Date(*d)),
        DbValue::Time(t) => Some(DbKey::Time(*t)),
        DbValue::Timestamp(ts) => Some(DbKey::Timestamp(*ts)),
        DbValue::Decimal(dec) => Some(DbKey::Decimal(*dec)),
        _ => None,
    }
}

/// Assumed table cardinality used for selectivity math when no statistics are
/// available. It only needs to be large enough that an exact-match predicate is
/// estimated as far cheaper than a full scan, so a primary-key (or secondary
/// index) point lookup is chosen even on a table that has never been analyzed.
///
/// Without this, an exact-match selectivity is `1.0 / row_count`, which
/// collapses to `1.0` when `row_count` is `0` — making a point lookup cost the
/// same as a full scan and defeating predicate pushdown on fresh tables.
const DEFAULT_ROW_COUNT_ESTIMATE: u64 = 1_000_000;

/// Returns `row_count` when known, otherwise a large default so selectivity
/// estimates remain meaningful in the absence of statistics.
fn effective_row_count(row_count: u64) -> u64 {
    if row_count > 0 {
        row_count
    } else {
        DEFAULT_ROW_COUNT_ESTIMATE
    }
}

/// Helper to examine an expression tree and extract range constraints for the given column.
fn extract_bounds_for_column(
    predicate: &Expr,
    col_name: &str,
    col_type: &DataType,
) -> Option<(PlanKey, PlanKey)> {
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
                    let start = match (&l_start, &r_start) {
                        (PlanKey::Value(lk), PlanKey::Value(rk)) => {
                            PlanKey::Value(std::cmp::max(lk.clone(), rk.clone()))
                        }
                        (PlanKey::Parameter(_), _) => l_start,
                        (_, PlanKey::Parameter(_)) => r_start,
                    };
                    let end = match (&l_end, &r_end) {
                        (PlanKey::Value(lk), PlanKey::Value(rk)) => {
                            PlanKey::Value(std::cmp::min(lk.clone(), rk.clone()))
                        }
                        (PlanKey::Parameter(_), _) => l_end,
                        (_, PlanKey::Parameter(_)) => r_end,
                    };
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
        } => get_column_comparison(left, right, col_name).map(|key| (key.clone(), key)),
        Expr::BinaryOp {
            left,
            op: Operator::Like,
            right,
        } => extract_like_prefix_bounds(left, right, col_name, col_type),
        Expr::BinaryOp { left, op, right } => {
            if let Some(key) = get_column_comparison(left, right, col_name) {
                match op {
                    Operator::GtEq | Operator::Gt => {
                        let start = match key {
                            PlanKey::Value(k) => {
                                if matches!(op, Operator::Gt) {
                                    match k {
                                        DbKey::Int(i64::MAX) => return None,
                                        DbKey::Int(v) => {
                                            PlanKey::Value(DbKey::Int(v.checked_add(1)?))
                                        }
                                        DbKey::String(s) => {
                                            PlanKey::Value(DbKey::string(format!("{s}\0")))
                                        }
                                        _ => PlanKey::Value(k),
                                    }
                                } else {
                                    PlanKey::Value(k)
                                }
                            }
                            PlanKey::Parameter(_) => key,
                        };
                        let end = match col_type {
                            DataType::Int => PlanKey::Value(DbKey::Int(i64::MAX)),
                            DataType::Date => PlanKey::Value(DbKey::Date(chrono::NaiveDate::MAX)),
                            DataType::Time => PlanKey::Value(DbKey::Time(
                                chrono::NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999)
                                    .unwrap(),
                            )),
                            DataType::Timestamp => {
                                PlanKey::Value(DbKey::Timestamp(chrono::NaiveDateTime::MAX))
                            }
                            DataType::Decimal => {
                                PlanKey::Value(DbKey::Decimal(rust_decimal::Decimal::MAX))
                            }
                            _ => PlanKey::Value(DbKey::string("\u{10ffff}")),
                        };
                        Some((start, end))
                    }
                    Operator::LtEq | Operator::Lt => {
                        let start = match col_type {
                            DataType::Int => PlanKey::Value(DbKey::Int(i64::MIN)),
                            DataType::Date => PlanKey::Value(DbKey::Date(chrono::NaiveDate::MIN)),
                            DataType::Time => PlanKey::Value(DbKey::Time(
                                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                            )),
                            DataType::Timestamp => {
                                PlanKey::Value(DbKey::Timestamp(chrono::NaiveDateTime::MIN))
                            }
                            DataType::Decimal => {
                                PlanKey::Value(DbKey::Decimal(rust_decimal::Decimal::MIN))
                            }
                            _ => PlanKey::Value(DbKey::string("")),
                        };
                        let end = match key {
                            PlanKey::Value(k) => {
                                if matches!(op, Operator::Lt) {
                                    match k {
                                        DbKey::Int(i64::MIN) => return None,
                                        DbKey::Int(v) => {
                                            PlanKey::Value(DbKey::Int(v.checked_sub(1)?))
                                        }
                                        DbKey::String(s) => PlanKey::Value(DbKey::String(s)),
                                        _ => PlanKey::Value(k),
                                    }
                                } else {
                                    PlanKey::Value(k)
                                }
                            }
                            PlanKey::Parameter(_) => key,
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

/// Reduces `col LIKE 'literal…'` to the key range that brackets every value
/// sharing the pattern's leading literal prefix. dtdb's LIKE has no `ESCAPE`,
/// so `%` and `_` are always wildcards.
///
/// The range is a deliberate *superset*: the upper bound is inclusive (so it
/// admits one boundary value just past the prefix space), and a pattern like
/// `'ab%c'` constrains only its prefix. This is sound because
/// `select_best_scan_path` always re-applies the original `LIKE` as a `Filter`
/// above the chosen scan, which removes the extra rows.
///
/// Returns `None` unless the column is the string column we are bounding, the
/// pattern is a literal (a bound parameter's value is unknown at plan time),
/// and the pattern has a non-empty literal prefix (a leading wildcard such as
/// `'%abc'` is not reducible to a prefix range).
fn extract_like_prefix_bounds(
    left: &Expr,
    right: &Expr,
    col_name: &str,
    col_type: &DataType,
) -> Option<(PlanKey, PlanKey)> {
    if !matches!(col_type, DataType::String) {
        return None;
    }
    let (Expr::Column(name, _), Expr::Literal(DbValue::String(pattern))) = (left, right) else {
        return None;
    };
    if !dtdb_relational::column_names_match(name, col_name) {
        return None;
    }
    let prefix = like_literal_prefix(pattern)?;
    let start = PlanKey::Value(DbKey::string(prefix.as_str()));
    let end = match prefix_successor(&prefix) {
        Some(succ) => PlanKey::Value(DbKey::string(succ)),
        // The prefix is composed entirely of U+10FFFF and has no representable
        // successor; fall back to the unbounded string maximum.
        None => PlanKey::Value(DbKey::string("\u{10ffff}")),
    };
    Some((start, end))
}

/// The maximal run of literal characters at the start of a LIKE pattern, up to
/// (but not including) the first `%` or `_` wildcard. `None` when the pattern
/// begins with a wildcard.
fn like_literal_prefix(pattern: &str) -> Option<String> {
    let prefix: String = pattern
        .chars()
        .take_while(|&c| c != '%' && c != '_')
        .collect();
    (!prefix.is_empty()).then_some(prefix)
}

/// The lexicographically smallest string strictly greater than every string
/// that has `prefix` as a prefix: increment the last scalar that can be
/// incremented, dropping trailing U+10FFFF scalars. `None` when `prefix` is
/// composed entirely of U+10FFFF.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_scalar(last) {
            let mut s: String = chars.into_iter().collect();
            s.push(next);
            return Some(s);
        }
    }
    None
}

/// The next Unicode scalar value after `c`, stepping over the UTF-16 surrogate
/// gap. `None` for U+10FFFF, which has no successor.
fn next_scalar(c: char) -> Option<char> {
    match c as u32 {
        0xD7FF => Some('\u{E000}'),
        0x10FFFF => None,
        n => char::from_u32(n + 1),
    }
}

/// Helper to examine an expression tree and extract range constraints for the primary key.
fn extract_key_range(predicate: &Expr, schema: &Schema) -> Option<(PlanKey, PlanKey)> {
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
    schema
        .columns
        .iter()
        .any(|col| dtdb_relational::column_names_match(&col.name, col_name))
}

fn swap_join_condition(condition: Expr) -> Expr {
    match condition {
        Expr::BinaryOp { left, op, right } => {
            let new_op = match op {
                Operator::Eq => Operator::Eq,
                Operator::NotEq => Operator::NotEq,
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                other => other,
            };
            Expr::BinaryOp {
                left: right,
                op: new_op,
                right: left,
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtdb_relational::{Column, DataType, IndexDefinition, IndexType, Schema};

    #[test]
    fn test_get_plan_sort_key_projection() {
        let schema = Schema::new(vec![Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }]);

        let scan = LogicalPlan::Scan {
            table_name: "t".to_string(),
            schema: schema.clone(),
            range: None,
        };

        let proj = LogicalPlan::Projection {
            source: Box::new(scan),
            expressions: vec![crate::expr::Expr::Column("id".to_string(), None)],
            field_names: vec!["alias_id".to_string()],
            schema: schema.clone(),
        };

        let sort_key = Optimizer::get_plan_sort_key(&proj);
        assert_eq!(sort_key, Some("alias_id".to_string()));
    }

    #[test]
    fn test_is_key_unbounded_min_max() {
        assert!(is_key_unbounded_min(&DbKey::Int(i64::MIN)));
        assert!(!is_key_unbounded_min(&DbKey::Int(0)));
        assert!(is_key_unbounded_max(&DbKey::Int(i64::MAX)));
        assert!(!is_key_unbounded_max(&DbKey::Int(0)));

        assert!(is_key_unbounded_min(&DbKey::string("")));
        assert!(!is_key_unbounded_min(&DbKey::string("a")));
        assert!(is_key_unbounded_max(&DbKey::string("\u{10ffff}")));

        assert!(is_key_unbounded_min(&DbKey::Bool(false)));
        assert!(!is_key_unbounded_min(&DbKey::Bool(true)));
        assert!(is_key_unbounded_max(&DbKey::Bool(true)));
        assert!(!is_key_unbounded_max(&DbKey::Bool(false)));

        assert!(is_key_unbounded_min(&DbKey::composite(vec![])));
        assert!(is_key_unbounded_min(&DbKey::composite(vec![DbKey::Bool(
            false
        )])));
        assert!(!is_key_unbounded_min(&DbKey::composite(vec![DbKey::Bool(
            true
        )])));
        assert!(is_key_unbounded_max(&DbKey::composite(vec![])));
        assert!(is_key_unbounded_max(&DbKey::composite(vec![DbKey::Bool(
            true
        )])));
        assert!(!is_key_unbounded_max(&DbKey::composite(vec![DbKey::Bool(
            false
        )])));
    }

    #[test]
    fn test_val_to_key_unsupported() {
        assert_eq!(val_to_key(&DbValue::Float(1.5)), None);
        assert_eq!(val_to_key(&DbValue::Bool(true)), None);
        assert_eq!(val_to_key(&DbValue::Null), None);
    }

    #[test]
    fn test_swap_join_conditions() {
        let l = Box::new(Expr::Column("a".to_string(), None));
        let r = Box::new(Expr::Column("b".to_string(), None));

        let cond_eq = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::Eq,
            right: r.clone(),
        };
        let swapped_eq = swap_join_condition(cond_eq);
        assert!(matches!(
            swapped_eq,
            Expr::BinaryOp {
                op: Operator::Eq,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_eq {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let cond_neq = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::NotEq,
            right: r.clone(),
        };
        let swapped_neq = swap_join_condition(cond_neq);
        assert!(matches!(
            swapped_neq,
            Expr::BinaryOp {
                op: Operator::NotEq,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_neq {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let cond_lt = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::Lt,
            right: r.clone(),
        };
        let swapped_lt = swap_join_condition(cond_lt);
        assert!(matches!(
            swapped_lt,
            Expr::BinaryOp {
                op: Operator::Gt,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_lt {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let cond_lteq = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::LtEq,
            right: r.clone(),
        };
        let swapped_lteq = swap_join_condition(cond_lteq);
        assert!(matches!(
            swapped_lteq,
            Expr::BinaryOp {
                op: Operator::GtEq,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_lteq {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let cond_gteq = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::GtEq,
            right: r.clone(),
        };
        let swapped_gteq = swap_join_condition(cond_gteq);
        assert!(matches!(
            swapped_gteq,
            Expr::BinaryOp {
                op: Operator::LtEq,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_gteq {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let cond_other = Expr::BinaryOp {
            left: l.clone(),
            op: Operator::Add,
            right: r.clone(),
        };
        let swapped_other = swap_join_condition(cond_other);
        assert!(matches!(
            swapped_other,
            Expr::BinaryOp {
                op: Operator::Add,
                ..
            }
        ));
        if let Expr::BinaryOp { left, right, .. } = swapped_other {
            assert_eq!(left, r);
            assert_eq!(right, l);
        }

        let non_binary = Expr::Literal(DbValue::Int(1));
        assert_eq!(swap_join_condition(non_binary.clone()), non_binary);
    }

    #[test]
    fn test_extract_bounds_for_column_edge_cases() {
        let col_type_int = DataType::Int;

        // WHERE id < i64::MIN should return None (empty range)
        let predicate_lt_min = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string(), None)),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(DbValue::Int(i64::MIN))),
        };
        assert_eq!(
            extract_bounds_for_column(&predicate_lt_min, "id", &col_type_int),
            None
        );

        // WHERE val > true for bool type (unsupported val_to_key)
        let predicate_bool_gt = Expr::BinaryOp {
            left: Box::new(Expr::Column("val".to_string(), None)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(DbValue::Bool(true))),
        };
        assert_eq!(
            extract_bounds_for_column(&predicate_bool_gt, "val", &DataType::Bool),
            None
        );

        // WHERE val > 5 for boolean col_type (testing fallback arm _ => key in matches!(op, Operator::Gt))
        let predicate_bool_gt_2 = Expr::BinaryOp {
            left: Box::new(Expr::Column("val".to_string(), None)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(DbValue::Int(5))),
        };
        let bounds = extract_bounds_for_column(&predicate_bool_gt_2, "val", &DataType::Bool);
        assert!(bounds.is_some());
        let (start, end) = bounds.unwrap();
        assert_eq!(start, PlanKey::Value(DbKey::Int(6)));
        assert_eq!(end, PlanKey::Value(DbKey::string("\u{10ffff}")));

        // WHERE val < 5 for boolean col_type (testing fallback arm _ => key in matches!(op, Operator::Lt))
        let predicate_bool_lt = Expr::BinaryOp {
            left: Box::new(Expr::Column("val".to_string(), None)),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(DbValue::Int(5))),
        };
        let bounds = extract_bounds_for_column(&predicate_bool_lt, "val", &DataType::Bool);
        assert!(bounds.is_some());
        let (start, end) = bounds.unwrap();
        assert_eq!(start, PlanKey::Value(DbKey::string("")));
        assert_eq!(end, PlanKey::Value(DbKey::Int(4)));

        // And operators
        let predicate_and = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string(), None)),
                op: Operator::GtEq,
                right: Box::new(Expr::Literal(DbValue::Int(5))),
            }),
            op: Operator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string(), None)),
                op: Operator::LtEq,
                right: Box::new(Expr::Literal(DbValue::Int(10))),
            }),
        };
        let bounds = extract_bounds_for_column(&predicate_and, "id", &col_type_int);
        assert_eq!(
            bounds,
            Some((
                PlanKey::Value(DbKey::Int(5)),
                PlanKey::Value(DbKey::Int(10))
            ))
        );
    }

    #[test]
    fn test_like_literal_prefix_extraction() {
        assert_eq!(like_literal_prefix("ab%"), Some("ab".to_string()));
        assert_eq!(like_literal_prefix("abc"), Some("abc".to_string())); // no wildcard at all
        assert_eq!(like_literal_prefix("ab_d"), Some("ab".to_string())); // `_` also terminates
        assert_eq!(like_literal_prefix("abc%xyz"), Some("abc".to_string()));
        // A leading wildcard leaves no literal prefix.
        assert_eq!(like_literal_prefix("%ab"), None);
        assert_eq!(like_literal_prefix("_ab"), None);
        assert_eq!(like_literal_prefix(""), None);
    }

    #[test]
    fn test_next_scalar_and_prefix_successor() {
        assert_eq!(next_scalar('a'), Some('b'));
        // Steps over the UTF-16 surrogate gap (U+D800..=U+DFFF).
        assert_eq!(next_scalar('\u{D7FF}'), Some('\u{E000}'));
        // The maximum scalar has no successor.
        assert_eq!(next_scalar('\u{10FFFF}'), None);

        assert_eq!(prefix_successor("ab"), Some("ac".to_string()));
        // Trailing max scalars carry into the preceding character.
        assert_eq!(prefix_successor("a\u{10FFFF}"), Some("b".to_string()));
        // An all-max prefix has no representable successor.
        assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
    }

    #[test]
    fn test_extract_like_prefix_bounds() {
        let like = |pat: &str| Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string(), None)),
            op: Operator::Like,
            right: Box::new(Expr::Literal(DbValue::string(pat))),
        };

        // `name LIKE 'ab%'` brackets the prefix space as the inclusive range
        // ["ab", "ac"]; the residual LIKE filter drops the lone "ac" boundary.
        assert_eq!(
            extract_bounds_for_column(&like("ab%"), "name", &DataType::String),
            Some((
                PlanKey::Value(DbKey::string("ab")),
                PlanKey::Value(DbKey::string("ac"))
            ))
        );
        // A later `_`/`%` only narrows beyond the prefix, so the prefix range is
        // identical to the pure-prefix case.
        assert_eq!(
            extract_bounds_for_column(&like("ab_d"), "name", &DataType::String),
            Some((
                PlanKey::Value(DbKey::string("ab")),
                PlanKey::Value(DbKey::string("ac"))
            ))
        );

        // Not reducible to a prefix range: leading wildcard, parameterized
        // pattern, column on the wrong side, or a non-string column.
        assert_eq!(
            extract_bounds_for_column(&like("%ab"), "name", &DataType::String),
            None
        );
        let like_param = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string(), None)),
            op: Operator::Like,
            right: Box::new(Expr::Parameter("p".to_string())),
        };
        assert_eq!(
            extract_bounds_for_column(&like_param, "name", &DataType::String),
            None
        );
        let like_reversed = Expr::BinaryOp {
            left: Box::new(Expr::Literal(DbValue::string("ab%"))),
            op: Operator::Like,
            right: Box::new(Expr::Column("name".to_string(), None)),
        };
        assert_eq!(
            extract_bounds_for_column(&like_reversed, "name", &DataType::String),
            None
        );
        assert_eq!(
            extract_bounds_for_column(&like("ab%"), "name", &DataType::Int),
            None
        );
    }

    #[test]
    fn test_get_plan_sort_key_edge_cases() {
        let mut schema = Schema::new(vec![Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }]);
        schema.indexes.push(IndexDefinition {
            name: "idx_id".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            tokenizer: None,
        });

        // IndexScan sort key check
        let iscan_valid = LogicalPlan::IndexScan {
            table_name: "t".to_string(),
            index_name: "idx_id".to_string(),
            schema: schema.clone(),
            range: None,
        };
        assert_eq!(
            Optimizer::get_plan_sort_key(&iscan_valid),
            Some("id".to_string())
        );

        let iscan_invalid = LogicalPlan::IndexScan {
            table_name: "t".to_string(),
            index_name: "idx_non_existent".to_string(),
            schema: schema.clone(),
            range: None,
        };
        assert_eq!(Optimizer::get_plan_sort_key(&iscan_invalid), None);

        // Filter / Limit / Distinct propagation
        let filter = LogicalPlan::Filter {
            source: Box::new(iscan_valid.clone()),
            predicate: Expr::Literal(DbValue::Bool(true)),
        };
        assert_eq!(
            Optimizer::get_plan_sort_key(&filter),
            Some("id".to_string())
        );

        // Projection match and mismatch
        let proj_match = LogicalPlan::Projection {
            source: Box::new(iscan_valid.clone()),
            expressions: vec![Expr::Column("id".to_string(), None)],
            field_names: vec!["alias_id".to_string()],
            schema: schema.clone(),
        };
        assert_eq!(
            Optimizer::get_plan_sort_key(&proj_match),
            Some("alias_id".to_string())
        );

        let proj_mismatch = LogicalPlan::Projection {
            source: Box::new(iscan_valid.clone()),
            expressions: vec![Expr::Literal(DbValue::Int(1))],
            field_names: vec!["alias_id".to_string()],
            schema: schema.clone(),
        };
        assert_eq!(Optimizer::get_plan_sort_key(&proj_mismatch), None);
    }

    #[test]
    fn test_try_promote_to_index_scan_edge_cases() {
        let mut schema = Schema::new(vec![
            Column {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_nullable: false,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
            Column {
                id: 0,
                name: "val".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
        ]);
        schema.indexes.push(IndexDefinition {
            name: "idx_val".to_string(),
            columns: vec!["val".to_string()],
            index_type: IndexType::BTree,
            tokenizer: None,
        });

        let tmp = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(tmp.path()).unwrap());
        let opt = Optimizer::new(db);

        // Scan range is not None (cannot promote)
        let scan_with_range = LogicalPlan::Scan {
            table_name: "t".to_string(),
            schema: schema.clone(),
            range: Some((
                PlanKey::Value(DbKey::Int(1)),
                PlanKey::Value(DbKey::Int(10)),
            )),
        };
        assert!(
            opt.try_promote_to_index_scan(scan_with_range, "val")
                .is_none()
        );

        // Scan with no range (can promote)
        let scan = LogicalPlan::Scan {
            table_name: "t".to_string(),
            schema: schema.clone(),
            range: None,
        };
        let promoted = opt.try_promote_to_index_scan(scan.clone(), "val");
        assert!(promoted.is_some());
        assert!(matches!(promoted.unwrap(), LogicalPlan::IndexScan { .. }));

        // Filter propagation
        let filter = LogicalPlan::Filter {
            source: Box::new(scan.clone()),
            predicate: Expr::Literal(DbValue::Bool(true)),
        };
        let filter_promoted = opt.try_promote_to_index_scan(filter, "val");
        assert!(filter_promoted.is_some());
        if let Some(LogicalPlan::Filter { source, .. }) = filter_promoted {
            assert!(matches!(*source, LogicalPlan::IndexScan { .. }));
        } else {
            panic!("Expected Filter containing IndexScan");
        }

        // Limit propagation
        let limit = LogicalPlan::Limit {
            source: Box::new(scan.clone()),
            limit: Some(Expr::Literal(DbValue::Int(10))),
            offset: None,
        };
        let limit_promoted = opt.try_promote_to_index_scan(limit, "val");
        assert!(limit_promoted.is_some());
        if let Some(LogicalPlan::Limit { source, .. }) = limit_promoted {
            assert!(matches!(*source, LogicalPlan::IndexScan { .. }));
        } else {
            panic!("Expected Limit containing IndexScan");
        }

        // Distinct propagation
        let distinct = LogicalPlan::Distinct {
            source: Box::new(scan.clone()),
        };
        let distinct_promoted = opt.try_promote_to_index_scan(distinct, "val");
        assert!(distinct_promoted.is_some());
        if let Some(LogicalPlan::Distinct { source, .. }) = distinct_promoted {
            assert!(matches!(*source, LogicalPlan::IndexScan { .. }));
        } else {
            panic!("Expected Distinct containing IndexScan");
        }

        // Projection propagation
        let proj = LogicalPlan::Projection {
            source: Box::new(scan.clone()),
            expressions: vec![Expr::Column("val".to_string(), None)],
            field_names: vec!["alias_val".to_string()],
            schema: schema.clone(),
        };
        let proj_promoted = opt.try_promote_to_index_scan(proj, "alias_val");
        assert!(proj_promoted.is_some());
        if let Some(LogicalPlan::Projection { source, .. }) = proj_promoted {
            assert!(matches!(*source, LogicalPlan::IndexScan { .. }));
        } else {
            panic!("Expected Projection containing IndexScan");
        }
    }
}
