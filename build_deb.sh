#!/bin/bash
set -eo pipefail
cargo run --release generate-completions
cargo deb