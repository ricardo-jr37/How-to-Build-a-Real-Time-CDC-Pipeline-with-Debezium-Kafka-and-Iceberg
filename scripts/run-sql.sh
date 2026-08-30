#!/usr/bin/env bash
# Runs every statement in a .sql file against Trino, one at a time.
#   ./scripts/run-sql.sh trino/tables.sql
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

file="${1:?usage: run-sql.sh <file.sql>}"

# Strip comments and flatten each statement onto one line, because
# `trino --execute` takes a single statement at a time.
mapfile -t statements < <(python3 -c "
import re, sys
sql = re.sub(r'--[^\n]*', '', open(sys.argv[1]).read())
for stmt in sql.split(';'):
    stmt = ' '.join(stmt.split())
    if stmt:
        print(stmt)
" "$file")

for stmt in "${statements[@]}"; do
  docker compose exec -T trino trino --server localhost:8080 --execute "$stmt" >/dev/null
  echo "  ${stmt:0:64}..."
done
