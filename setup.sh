#!/bin/bash
set -e

echo "Installing Rust toolchain..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

echo "Building KS SQL in release mode..."
cargo build --release

echo "Build complete. Binary located at target/release/ks-sql"
ls -lh target/release/ks-sql
