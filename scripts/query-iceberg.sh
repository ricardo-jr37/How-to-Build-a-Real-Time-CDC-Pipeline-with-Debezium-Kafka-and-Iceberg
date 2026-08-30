#!/usr/bin/env bash
# Runs a SQL statement against the Iceberg tables through Trino.
#   ./scripts/query-iceberg.sh "SELECT count(*) FROM iceberg.raw.orders"
# With no argument it prints a summary of both layers of the lakehouse.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

run() { docker compose exec -T trino trino --server localhost:8080 --execute "$1"; }

if [ $# -ge 1 ]; then
  run "$1"
  exit 0
fi

echo "== raw change log: events per operation (r read · c insert · u update · d delete) =="
run "SELECT 'customers' AS source_table, __op AS op, count(*) AS events
     FROM iceberg.raw.customers GROUP BY __op
     UNION ALL
     SELECT 'orders', __op, count(*) FROM iceberg.raw.orders GROUP BY __op
     UNION ALL
     SELECT 'clients', __op, count(*) FROM iceberg.raw.clients GROUP BY __op
     ORDER BY 1, 2"

echo
echo "== current state (views over the change log) =="
run "SELECT 'customers' AS tbl, count(*) AS rows FROM iceberg.lakehouse.customers
     UNION ALL
     SELECT 'orders', count(*) FROM iceberg.lakehouse.orders
     UNION ALL
     SELECT 'clients', count(*) FROM iceberg.lakehouse.clients
     ORDER BY 1"

echo
echo "== most recently changed orders =="
run "SELECT id, customer_id, status, total_amount, currency, _last_change_at
     FROM iceberg.lakehouse.orders ORDER BY _last_change_at DESC, id DESC LIMIT 10"

echo
echo "== clients, current state =="
run "SELECT id, full_name, country, tier, phone_number, is_active
     FROM iceberg.lakehouse.clients ORDER BY id"

echo
echo "== snapshots committed to raw.orders =="
run "SELECT snapshot_id, committed_at, operation,
            summary['added-records'] AS added_records
     FROM iceberg.raw.\"orders\$snapshots\"
     ORDER BY committed_at DESC LIMIT 5"

echo
echo "== end-to-end lag: newest change now in Iceberg =="
run "SELECT from_unixtime(max(__ts_ms) / 1e3, 'UTC') AS newest_event,
            current_timestamp AS now,
            date_diff('second', from_unixtime(max(__ts_ms) / 1e3, 'UTC'), current_timestamp) AS lag_seconds
     FROM iceberg.raw.orders"
