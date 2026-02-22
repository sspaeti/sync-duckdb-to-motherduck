#!/usr/bin/env bash
# sync_duckdb.sh — Sync a local DuckDB database to MotherDuck via CLI
# Usage: ./sync_duckdb.sh local.db my_remote_db

set -euo pipefail

LOCAL_DB="${1:?Usage: $0 <local.db> <remote_db_name>}"
REMOTE_DB="${2:?Usage: $0 <local.db> <remote_db_name>}"

if [[ -z "${MOTHERDUCK_TOKEN:-}" ]]; then
  echo "Error: MOTHERDUCK_TOKEN is not set."
  echo "  export MOTHERDUCK_TOKEN=your_token_here"
  exit 1
fi

echo "Syncing '${LOCAL_DB}' → md:${REMOTE_DB}"

duckdb "${LOCAL_DB}" <<SQL
ATTACH 'md:';
CREATE OR REPLACE DATABASE ${REMOTE_DB} FROM CURRENT_DATABASE();
SQL

echo "Done."
