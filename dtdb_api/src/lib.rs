pub mod proto {
    tonic::include_proto!("dtdb");
}

pub mod client;
pub mod query;
pub mod server;

pub use query::SqlQuery;

/// Macro to enforce that SQL queries are compile-time literal strings.
///
/// Example:
/// ```rust
/// use dtdb_api::sql_query;
/// let q = sql_query!("SELECT * FROM users WHERE id = @id").bind("id", 10);
/// ```
#[macro_export]
macro_rules! sql_query {
    ($text:literal) => {
        $crate::SqlQuery::new($text.to_string())
    };
    ($other:expr) => {
        compile_error!("sql_query! only accepts compile-time literal strings!")
    };
}
