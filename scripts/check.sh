#!/bin/zsh
set -e
cd "$(dirname "$0")/.."
export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$HOME/.cargo/bin:$PATH"
cargo build --release
cargo test --release -p blackhole-core
for s in e2e.sh mount_test.sh s3_test.sh server_test.sh; do
  echo "== scripts/$s"
  ./scripts/$s > /tmp/blackhole-check-$s.out 2>&1 || { echo "FAILED: see /tmp/blackhole-check-$s.out"; exit 1; }
  echo "ok"
done
echo "== scripts/mcp_smoke.py"
python3 -I scripts/mcp_smoke.py > /tmp/blackhole-check-mcp.out 2>&1 || { echo "FAILED: see /tmp/blackhole-check-mcp.out"; exit 1; }
echo "ok"
echo "all checks passed"
