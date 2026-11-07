#!/usr/bin/env bash
set -euo pipefail

DURATION="${DURATION:-2s}"
WORKERS="${WORKERS:-8}"

for wl in W1 W2 W3 W4 W5 W6; do
  for mode in direct deferred batched; do
    echo "=== Running workload=${wl} mode=${mode} duration=${DURATION} ==="
    go run ./cmd/finjournalctl bench --workload "${wl}" --mode "${mode}" --duration "${DURATION}" --workers "${WORKERS}"
  done
done
