@echo off
cargo fmt --all
rem cargo clippy --all-targets --all-features -- -D warnings

rem del prof\* /Q
rem cargo clean

setlocal
set "RUSTFLAGS=-C instrument-coverage"
set "LLVM_PROFILE_FILE=prof/kademlia-%%p-%%m.profraw"
set "RUST_LOG=debug"
cargo +nightly test %*

grcov prof -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/
start .\target\debug\coverage\index.html
