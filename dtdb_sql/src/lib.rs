pub mod engine;
pub mod expr;
pub mod logical;
pub mod optimizer;
pub mod physical;
pub mod planner;
pub mod query;

pub use engine::{ExecutionResult, SqlEngine};
pub use expr::{Expr, Operator};
pub use logical::{AggregateExpr, LogicalPlan, format_logical_plan};
pub use optimizer::Optimizer;
pub use physical::PhysicalOperator;
pub use planner::{LogicalPlanner, SqlStatement, plan_expr};
pub use query::SqlQuery;

/// Macro to enforce that SQL queries are compile-time literal strings.
///
/// Example:
/// ```rust
/// use dtdb_sql::sql_query;
/// let q = sql_query!("SELECT * FROM users WHERE id = @id").bind("id", 10);
/// ```
#[macro_export]
macro_rules! sql_query {
    ($text:literal) => {
        $crate::SqlQuery::new($text)
    };
    ($other:expr) => {
        compile_error!("sql_query! only accepts compile-time literal strings!")
    };
}
