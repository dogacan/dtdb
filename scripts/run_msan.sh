#!/bin/bash
# A script to run DuctTapeDB tests under MemorySanitizer (MSan)

set -e

# MemorySanitizer is only supported on Linux targets.
# Check if we are running on Linux.
OS=$(uname -s)
if [ "$OS" != "Linux" ]; then
    echo "ERROR: MemorySanitizer (MSan) is only supported on Linux targets (e.g., x86_64-unknown-linux-gnu)."
    echo "You are currently running on $OS. You can run MSan inside a Linux container or VM."
    exit 1
fi

# 1. Detect the host target architecture
TARGET=$(rustc -vV | grep host | cut -d ' ' -f 2)

echo "=== MemorySanitizer (MSan) Test Runner ==="
echo "Host target: $TARGET"

# 2. Check if nightly compiler is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo "Rust nightly toolchain not found. Installing..."
    rustup toolchain install nightly
else
    echo "Rust nightly toolchain is already installed."
fi

# 3. Run cargo test with MSan enabled
echo "Compiling and running tests with MemorySanitizer..."
RUSTFLAGS="-Z sanitizer=memory" \
RUSTDOCFLAGS="-Z sanitizer=memory" \
cargo +nightly test -Zbuild-std --target "$TARGET" "$@"
