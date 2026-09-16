#!/usr/bin/env bash
set -euo pipefail
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
"${compose[@]}" exec -T --user 0 spark sh -c \
  'rm -rf -- /workspace/metastore_db /workspace/spark-warehouse; rm -f -- /workspace/derby.log' \
  2>/dev/null || true
"${compose[@]}" down -v --remove-orphans
rm -rf -- metastore_db spark-warehouse
rm -f -- derby.log
