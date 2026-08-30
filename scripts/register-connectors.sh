#!/usr/bin/env bash
# Registers the Debezium source and the Iceberg sink, then waits for both to
# report RUNNING. Safe to run repeatedly -- see register() for why that matters.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; . "${ROOT}/.env"; set +a

CONNECT_URL="${CONNECT_URL:-http://localhost:${CONNECT_PORT}}"

wait_for_connect() {
  echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
  for _ in $(seq 1 60); do
    if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
      echo "  Connect is up"
      return 0
    fi
    sleep 5
  done
  echo "  Connect never came up" >&2
  exit 1
}

# Submits a connector config, but only when it differs from what is running.
#
# This matters more than it looks. A PUT to /config restarts the connector's
# tasks, and a stopped Iceberg sink task can leave its commit coordinator thread
# behind holding an already-closed REST catalog client. That zombie keeps
# answering on the control topic, and from then on every commit fails with
# "Connection pool shut down" -- while the connector still reports RUNNING and
# the consumer group still shows zero lag. Re-submitting an unchanged config is
# the easiest way to trigger it, so we don't.
register() {
  local file="$1" name current
  name="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['name'])" "$file")"
  current="$(curl -fsS "${CONNECT_URL}/connectors/${name}/config" 2>/dev/null || true)"

  if [ -n "$current" ] && CURRENT_CONFIG="$current" python3 -c '
import json, os, sys
desired = json.load(open(sys.argv[1]))["config"]
current = json.loads(os.environ["CURRENT_CONFIG"])
current.pop("name", None)
sys.exit(0 if {k: str(v) for k, v in desired.items()} == current else 1)
' "$file"; then
    echo "  ${name}: config unchanged, left running"
    return 0
  fi

  echo "registering connector '${name}' ..."
  curl -fsS -X PUT "${CONNECT_URL}/connectors/${name}/config" \
    -H 'Content-Type: application/json' \
    -d "$(python3 -c "import json,sys;print(json.dumps(json.load(open(sys.argv[1]))['config']))" "$file")" \
    >/dev/null
  echo "  submitted"
}

check() {
  local name="$1"
  for _ in $(seq 1 30); do
    local state
    state="$(curl -fsS "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null \
      | python3 -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo UNKNOWN)"
    case "$state" in
      RUNNING) echo "  ${name}: RUNNING"; return 0 ;;
      FAILED)  echo "  ${name}: FAILED" >&2
               curl -fsS "${CONNECT_URL}/connectors/${name}/status" | python3 -m json.tool >&2
               return 1 ;;
      *)       sleep 3 ;;
    esac
  done
  echo "  ${name}: never reached RUNNING" >&2
  return 1
}

wait_for_connect
register "${ROOT}/connectors/01-postgres-source.json"
register "${ROOT}/connectors/02-iceberg-sink.json"

echo "waiting for connectors to settle ..."
sleep 5
check postgres-source
check iceberg-sink

echo
echo "connectors are running. Next:"
echo "  make load     # generate change traffic"
echo "  make query    # see it in Iceberg"
