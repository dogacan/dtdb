//! Guard test: every Rust code block in the markdown docs that are wired up as
//! doctests (via `#[doc = include_str!(...)]` in `src/lib.rs`) must be marked
//! `no_run` — so `cargo test --doc` compiles and type-checks the examples
//! against the real API but never executes them (no disk, no network).
//!
//! Without this, someone could add a plain ```` ```rust ```` block (or an
//! untagged ```` ``` ```` block, which rustdoc also compiles as Rust) whose
//! doctest would run for real on `cargo test`.
//!
//! Acceptable annotations are `no_run`, `ignore`, and `compile_fail` — none of
//! those execute the block. Anything else that rustdoc would run (a bare
//! `rust`, an untagged fence, or `should_panic`) is a failure.

use std::path::{Path, PathBuf};

/// Markdown files included as doctests in `src/lib.rs`. Keep this in sync with
/// the `#[doc = include_str!(...)]` attributes there.
const DOCTESTED_MARKDOWN: &[&str] = &["../docs/in_process_api.md"];

/// Annotations that mean "rustdoc will not execute this block".
const NON_RUNNING: &[&str] = &["no_run", "ignore", "compile_fail"];

#[test]
fn every_rust_doc_block_is_no_run() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations: Vec<String> = Vec::new();

    for rel in DOCTESTED_MARKDOWN {
        let path = manifest_dir.join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        collect_violations(&path, &text, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "Found Rust doc code blocks that rustdoc would execute. Mark each with \
         `no_run` (preferred), or `ignore`/`compile_fail` if appropriate:\n{}",
        violations.join("\n")
    );
}

fn collect_violations(path: &Path, text: &str, violations: &mut Vec<String>) {
    let mut in_block = false;
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim_start();
        if !line.starts_with("```") {
            continue;
        }

        if in_block {
            // A fence while inside a block closes it; closing fences carry no
            // info string, so there is nothing to validate.
            in_block = false;
            continue;
        }

        // Opening fence: the "info string" is everything after the backticks.
        in_block = true;
        let info = line.trim_start_matches('`').trim();

        if !is_rust_doctest(info) {
            continue; // ```sql, ```text, ```bash, etc. are not compiled by rustdoc.
        }

        let tokens: Vec<&str> = info
            .split([',', ' '])
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .collect();

        if !tokens.iter().any(|t| NON_RUNNING.contains(t)) {
            let shown = if info.is_empty() {
                "``` (untagged — rustdoc treats this as a runnable Rust block)".to_string()
            } else {
                format!("```{info}")
            };
            violations.push(format!("  {}:{} {}", path.display(), idx + 1, shown));
        }
    }
}

/// rustdoc compiles a fenced block as a Rust doctest when the info string is
/// empty or its first token is `rust`. Everything else (other languages) is
/// left alone.
fn is_rust_doctest(info: &str) -> bool {
    if info.is_empty() {
        return true;
    }
    let first = info.split([',', ' ']).next().unwrap_or("").trim();
    first == "rust"
}
