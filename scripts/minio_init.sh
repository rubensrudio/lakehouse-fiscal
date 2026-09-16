#!/usr/bin/env bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
"${compose[@]}" exec -T minio-init sh -c '
  mc mb --ignore-existing local/lakehouse-fiscal
  for prefix in landing bronze silver gold _checkpoints; do
    echo ready | mc pipe "local/lakehouse-fiscal/${prefix}/.keep"
  done
'
