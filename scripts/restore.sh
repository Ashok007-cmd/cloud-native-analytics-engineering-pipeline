#!/usr/bin/env bash
set -euo pipefail

# Restore a DuckDB backup produced by scripts/backup.sh.
# Usage: scripts/restore.sh <path-to-backup.tar.gz>

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path-to-backup.tar.gz>" >&2
    exit 1
fi

BACKUP_FILE="$1"
DUCKDB_PATH="${DUCKDB_PATH:-dbt_project/dev.duckdb}"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "ERROR: backup file not found: $BACKUP_FILE" >&2
    exit 1
fi

if [ -f "$DUCKDB_PATH" ]; then
    SAFETY_COPY="${DUCKDB_PATH}.pre-restore.$(date -u +"%Y%m%d_%H%M%S")"
    echo "Existing database found — saving safety copy to $SAFETY_COPY"
    cp "$DUCKDB_PATH" "$SAFETY_COPY"
fi

echo "Restoring from $BACKUP_FILE..."
# The archive stores the same relative path backup.sh was invoked with
# (e.g. dbt_project/dev.duckdb) — extract relative to the current directory,
# which must be the project root, matching how `make backup`/`make restore` run.
tar -xzf "$BACKUP_FILE"

echo "Restore complete: $DUCKDB_PATH"
