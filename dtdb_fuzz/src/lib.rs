//! Shared helpers for the dtdb fuzz / property targets in `fuzz_targets/`.
//!
//! Each target lives in its own integration test binary and is marked
//! `#[ignore]` so that `cargo test` doesn't run it by default; invoke with
//! `cargo test -p dtdb_fuzz -- --ignored` or via `scripts/run_fuzz.sh`.

/// Iteration budget for bolero `check!()` targets. Defaults to 1024 and can be
/// overridden via `BOLERO_FUZZ_ITERATIONS` (see `scripts/run_fuzz.sh`).
///
/// Bolero's stable-Rust engine otherwise time-boxes at 1 second per target,
/// which translates to only a handful of iterations for slower harnesses.
pub fn bolero_iterations() -> usize {
    std::env::var("BOLERO_FUZZ_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024)
}
