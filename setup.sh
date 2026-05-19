#!/bin/bash
set -e

echo "===================================================="
echo "          KS SQL : TITAN-PRIME BUILD SYSTEM          "
echo "===================================================="

# Check for Rust toolchain
if ! command -v cargo &> /dev/null
then
    echo "[!] Rust toolchain not found. Installing via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
else
    echo "[+] Rust toolchain detected."
fi

# Build release binary
echo "[+] Compiling High-Performance Standalone Binary..."
cargo build --release

echo "===================================================="
echo "SUCCESS: Binary generated at target/release/ks-sql"
echo "===================================================="
echo "Usage: ./target/release/ks-sql --port w:8080 m:5432 --user admin --password admin"
echo "Dashboard (Cyber-Command): http://localhost:8080"
echo "===================================================="
