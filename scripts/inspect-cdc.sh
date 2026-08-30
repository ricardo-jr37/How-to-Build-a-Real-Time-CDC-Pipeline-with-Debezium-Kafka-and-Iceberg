#!/usr/bin/env bash
# Prints the moving parts of the pipeline: replication slot, topics,
# connector status and consumer lag. First stop when something looks stuck.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
set -a; . ./.env; set +a

echo "== postgres replication slot =="
docker compose exec -T postgres psql -U postgres -d shop -x -c \
  "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
   FROM pg_replication_slots"

echo "== kafka topics =="
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -v '^_' || true

echo
echo "== connector status =="
for c in postgres-source iceberg-sink; do
  curl -fsS "http://localhost:${CONNECT_PORT}/connectors/${c}/status" | python3 -m json.tool || echo "${c}: not registered"
done

echo
echo "== sink commit health =="
# The Iceberg sink reports RUNNING and zero lag even when every commit is
# failing, so connector state alone tells you nothing. Look at what the
# coordinator actually did on its last cycles.
recent="$(docker compose logs connect --tail 400 2>&1 | grep -E 'committed to|Commit failed|pool shut down' | tail -8)"
if grep -q 'pool shut down' <<<"$recent"; then
  echo "  UNHEALTHY: the commit coordinator is holding a closed catalog client."
  echo "  Nothing will land in Iceberg until the Connect worker is restarted:"
  echo "      make restart-connect"
  echo "  (restarting the connector is not enough -- the zombie thread lives in the worker)"
elif grep -q 'committed to [1-9]' <<<"$recent"; then
  echo "  healthy -- recent commits landed:"
  grep -E 'committed to [1-9]' <<<"$recent" | tail -2 | sed 's/^/    /'
else
  echo "  idle -- no data committed recently (normal with no write traffic)"
fi

echo
echo "== sink consumer lag =="
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group connect-iceberg-sink 2>/dev/null || echo "(sink group not formed yet)"
