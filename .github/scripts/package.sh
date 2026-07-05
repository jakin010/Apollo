#!/usr/bin/env bash
# Package the release binary for one target into dist/apollo-<tag>-<target>.tar.gz.
#
# Usage: package.sh <target-triple> <tag>
#
# Looks for the built binary at target/<target-triple>/release/apollo (macOS
# jobs, which pass --target) or target/release/apollo (Linux jobs, which build
# for the host), strips debug symbols, and bundles it with whatever of
# $EXTRA_FILES exists at the repo root (missing ones are skipped, not fatal).
set -euo pipefail

target="$1"
tag="$2"

# The macOS jobs pass `--target <triple>`, so the binary is under
# target/<triple>/release/. The Linux jobs build for the host with no `--target`
# (and the CUDA "target" is an artifact label, not a real triple), so theirs is
# under target/release/. Accept either.
bin="target/${target}/release/apollo"
[[ -f "$bin" ]] || bin="target/release/apollo"

if [[ ! -f "$bin" ]]; then
  echo "error: no apollo binary at target/${target}/release/ or target/release/" >&2
  exit 1
fi

# Strip if a stripper is available for this host (both macOS and Linux ship
# `strip`; harmless to skip if not found).
if command -v strip >/dev/null 2>&1; then
  strip "$bin" || echo "warning: strip failed, continuing with unstripped binary"
fi

stage="$(mktemp -d)/apollo-${tag}-${target}"
mkdir -p "$stage"
cp "$bin" "$stage/"

for f in ${EXTRA_FILES:-}; do
  if [[ -f "$f" ]]; then
    cp "$f" "$stage/"
  fi
done

mkdir -p dist
tar -czf "dist/apollo-${tag}-${target}.tar.gz" -C "$(dirname "$stage")" "$(basename "$stage")"

echo "wrote dist/apollo-${tag}-${target}.tar.gz"
ls -la dist/