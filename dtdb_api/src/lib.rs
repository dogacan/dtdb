#![allow(clippy::result_large_err)]

pub mod proto {
    // tonic-prost-build 0.14 carries the `.proto` file's leading comments into
    // the generated Rust as doc comments; their indented continuation lines trip
    // clippy's doc_lazy_continuation lint in code we don't control.
    #![allow(clippy::doc_lazy_continuation)]
    tonic::include_proto!("dtdb");
}

pub mod client;
pub mod in_process;
pub mod query;
pub mod remote;
pub mod server;

pub use query::SqlQuery;

// Compile-check the in-process API guide: every Rust code block in
// docs/in_process_api.md is run as a doctest (all `no_run`, so they compile
// and type-check against the real API but never touch disk or the network).
// The `tests/doc_examples_no_run.rs` guard enforces the `no_run` annotation.
// `#[cfg(doctest)]` keeps this item out of the built library and rendered docs.
#[cfg(doctest)]
#[doc = include_str!("../../docs/in_process_api.md")]
struct InProcessApiGuideDoctests;

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
