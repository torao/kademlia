#!/bin/bash
cargo fmt

export RUSTFLAGS="-C instrument-coverage"
export LLVM_PROFILE_FILE="prof/kademlia-%p-%m.profraw"
cargo +nightly test

grcov prof -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/
start ./target/debug/coverage/index.html
