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

/// Validates that binding to a non-loopback address requires TLS and Authentication.
pub fn validate_bind_security(
    bind_addr_str: &str,
    has_auth: bool,
    has_tls: bool,
) -> Result<(), String> {
    let addr: std::net::SocketAddr = bind_addr_str
        .parse()
        .map_err(|e| format!("Failed to parse bind address: {}", e))?;
    let ip = addr.ip();
    if !ip.is_loopback() && (!has_auth || !has_tls) {
        return Err(format!(
            "Security Error: Binding to a non-loopback address ({}) requires both TLS and Authentication to be configured.",
            bind_addr_str
        ));
    }
    Ok(())
}
