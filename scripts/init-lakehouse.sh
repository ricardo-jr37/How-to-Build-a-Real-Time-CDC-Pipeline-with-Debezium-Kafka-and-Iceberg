#!/usr/bin/env bash
# Creates the Iceberg namespaces, the raw change-log tables and the
# current-state views. Runs before the connectors: the sink writes into tables
# that already exist, with types we chose rather than types it guessed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "creating namespaces and raw tables ..."
./scripts/run-sql.sh trino/tables.sql

echo "creating current-state views ..."
./scripts/run-sql.sh trino/views.sql

echo "lakehouse ready: iceberg.raw.{customers,orders,clients} + iceberg.lakehouse views"
