#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  cp .env.example .env
fi
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
"${compose[@]}" build spark
"${compose[@]}" up -d
./scripts/wait_for_services.sh
./scripts/minio_init.sh
