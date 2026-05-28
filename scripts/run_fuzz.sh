#!/bin/bash
# Runs the dtdb_fuzz property/stress targets with bounded iteration budgets.
#
# Iteration counts come from env vars so the same script serves both PR-time
# (fast) and nightly (long) CI invocations:
#
#   BOLERO_FUZZ_ITERATIONS  default 1024   — bolero corruption targets
#   SHUTTLE_MAX_ITERATIONS  default 1024   — reserved (no shuttle targets yet)
#   PROPTEST_CASES          default 256    — proptest-driven targets

set -euo pipefail

: "${BOLERO_FUZZ_ITERATIONS:=1024}"
: "${PROPTEST_CASES:=256}"

# bolero's stable-Rust "rng" engine takes its iteration count from
# BOLERO_RNG_ITERATIONS (otherwise it stops after 1s of wall clock).
export BOLERO_RNG_ITERATIONS="$BOLERO_FUZZ_ITERATIONS"
export PROPTEST_CASES

echo "=== dtdb_fuzz runner ==="
echo "BOLERO_FUZZ_ITERATIONS=$BOLERO_FUZZ_ITERATIONS"
echo "PROPTEST_CASES=$PROPTEST_CASES"
echo

cargo test -p dtdb_fuzz \
    --test wal_recovery \
    --test sstable_reader \
    --test concurrent_txns \
    --test sql_op_sequence \
    -- --ignored --nocapture
