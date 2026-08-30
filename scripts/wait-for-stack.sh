#!/usr/bin/env bash
# Blocks until every service that has a healthcheck reports healthy.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

services=(postgres kafka minio iceberg-rest connect trino schema-registry)

echo "waiting for services to become healthy ..."
for _ in $(seq 1 90); do
  pending=()
  for s in "${services[@]}"; do
    cid="$(docker compose ps -q "$s" 2>/dev/null || true)"
    # No container at all means the service is off behind a profile -- skip it.
    if [ -z "$cid" ]; then continue; fi
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo missing)"
    case "$status" in healthy|running) ;; *) pending+=("$s (${status})") ;; esac
  done
  if [ ${#pending[@]} -eq 0 ]; then echo "  all services healthy"; exit 0; fi
  printf '  still waiting: %s\n' "${pending[*]}"
  sleep 5
done
echo "timed out waiting for: ${pending[*]}" >&2
exit 1
