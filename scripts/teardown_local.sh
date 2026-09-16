#!/usr/bin/env bash
set -euo pipefail
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
"${compose[@]}" down -v --remove-orphans
