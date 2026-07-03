#!/bin/bash
# generate_bpf.sh — Generate BPF Go bindings for the current architecture.
#
# Run this on EACH target architecture (arm64 and x86_64) to produce the
# architecture-specific .o + .go files. Commit both sets to the repo.
#
# Prerequisites:
#   - clang, llvm, libbpf-dev
#   - bpftool (for vmlinux.h generation)
#   - ~/go/bin/bpf2go (go install github.com/cilium/ebpf/cmd/bpf2go@latest)
#
# Usage:
#   ./pkg/capture/bpf/generate_bpf.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CAPTURE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$CAPTURE_DIR"

ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  TARGET="amd64"; BPF_ARCH="__TARGET_ARCH_x86" ;;
  aarch64) TARGET="arm64"; BPF_ARCH="__TARGET_ARCH_arm64" ;;
  *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

echo "=== Generating BPF objects for $ARCH ($TARGET) ==="

# Step 1: Generate vmlinux.h from current kernel BTF
echo "→ Generating vmlinux.h from /sys/kernel/btf/vmlinux..."
sudo bpftool btf dump file /sys/kernel/btf/vmlinux format c > bpf/vmlinux.h

# Step 2: Generate XDP capture program
echo "→ Compiling XDP BPF program (target=$TARGET)..."
~/go/bin/bpf2go -go-package capture -output-dir . \
  -tags "linux,ebpf" \
  -target "$TARGET" \
  pgshadowXdp bpf/pgshadow_xdp.c -- \
  -I bpf -O2 -g

# Step 3: Generate SSL uprobe program
echo "→ Compiling SSL uprobe BPF program (target=$TARGET)..."
~/go/bin/bpf2go -go-package capture -output-dir . \
  -tags "linux,ebpf" \
  -target "$TARGET" \
  pgshadowSsl bpf/pgshadow_ssl.c -- \
  -I bpf -D"$BPF_ARCH" -O2 -g

echo ""
echo "=== Done! Generated files: ==="
ls -la pgshadowxdp_${TARGET}_bpfel.* pgshadowssl_${TARGET}_bpfel.*
echo ""
echo "Commit these files to the repo for $TARGET support."
