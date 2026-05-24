#!/bin/bash
# A script to run DuctTapeDB tests under AddressSanitizer (ASan)

set -e

# 1. Detect the host target architecture
TARGET=$(rustc -vV | grep host | cut -d ' ' -f 2)

echo "=== AddressSanitizer (ASan) Test Runner ==="
echo "Host target: $TARGET"

# 2. Check if nightly compiler is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo "Rust nightly toolchain not found. Installing..."
    rustup toolchain install nightly
else
    echo "Rust nightly toolchain is already installed."
fi

# 3. Run cargo test with ASan enabled
echo "Compiling and running tests with AddressSanitizer..."
RUSTFLAGS="-Z sanitizer=address" \
RUSTDOCFLAGS="-Z sanitizer=address" \
ASAN_OPTIONS=detect_leaks=1 \
cargo +nightly test -Zbuild-std --target "$TARGET" "$@"
