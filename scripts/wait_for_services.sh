#!/usr/bin/env bash
set -euo pipefail

deadline=$((SECONDS + 180))
services=(spark minio minio-init postgres)
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
while (( SECONDS < deadline )); do
  unhealthy=""
  for service in "${services[@]}"; do
    container_id="$("${compose[@]}" ps -q "${service}")"
    if [[ -z "${container_id}" ]]; then
      unhealthy="${service}"
      break
    fi
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
    if [[ "${status}" != "healthy" ]]; then
      unhealthy="${service}"
      break
    fi
  done
  [[ -z "${unhealthy}" ]] && exit 0
  sleep 2
done
echo "Service '${unhealthy}' did not become healthy within 180s." >&2
exit 1
