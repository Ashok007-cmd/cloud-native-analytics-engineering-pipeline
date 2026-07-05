#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
MAX_BACKUPS="${MAX_BACKUPS:-30}"
TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")

if [ -z "$BACKUP_DIR" ] || [ "$BACKUP_DIR" = "/" ]; then
    echo "ERROR: BACKUP_DIR is unsafe: '$BACKUP_DIR'" >&2
    exit 1
fi

mkdir -p "$BACKUP_DIR"

echo "=== Backup started: $TIMESTAMP ==="

# DuckDB — archive with tar+gzip; include WAL when present
# Using tar avoids multi-stream gzip (two concatenated gzip streams confuse most tools)
DUCKDB_PATH="${DUCKDB_PATH:-dbt_project/dev.duckdb}"
DUCKDB_WAL="${DUCKDB_PATH}.wal"
if [ -f "$DUCKDB_PATH" ]; then
    DUCKDB_BASENAME=$(basename "$DUCKDB_PATH" .duckdb)
    DUCKDB_BACKUP="$BACKUP_DIR/${DUCKDB_BASENAME}.duckdb.${TIMESTAMP}.tar.gz"

    # Force a CHECKPOINT before archiving so the on-disk main file is fully
    # consistent and the WAL is flushed/truncated. Reading the file at the
    # filesystem level (tar) without this step risks capturing a main file
    # and WAL from different points in time if DuckDB auto-checkpoints
    # mid-backup, producing a corrupt or silently-wrong restore.
    echo "Checkpointing DuckDB database before backup: $DUCKDB_PATH"
    if ! DUCKDB_PATH="$DUCKDB_PATH" python3 -c "
import duckdb, os, sys
db_path = os.environ['DUCKDB_PATH']
try:
    con = duckdb.connect(db_path)
    con.execute('CHECKPOINT')
    con.close()
except Exception as exc:
    print(f'ERROR: checkpoint failed: {exc}', file=sys.stderr)
    sys.exit(1)
"; then
        echo "ERROR: Failed to checkpoint DuckDB database before backup" >&2
        exit 1
    fi

    FILES_TO_ARCHIVE=("$DUCKDB_PATH")
    [ -f "$DUCKDB_WAL" ] && FILES_TO_ARCHIVE+=("$DUCKDB_WAL")
    tar -czf "$DUCKDB_BACKUP" "${FILES_TO_ARCHIVE[@]}"
    if [ -s "$DUCKDB_BACKUP" ]; then
        CHECKSUM=$(sha256sum "$DUCKDB_BACKUP" | awk '{print $1}')
        SIZE=$(wc -c < "$DUCKDB_BACKUP" | tr -d ' ')
        WAL_NOTE=""
        [ -f "$DUCKDB_WAL" ] && WAL_NOTE=" (+ WAL)"
        echo "DuckDB backed up${WAL_NOTE}: $DUCKDB_BACKUP ($SIZE bytes, sha256=$CHECKSUM)"
        # Original files are left in place — a backup preserves the working copy.
        # Use scripts/restore.sh to restore from an archive when needed.
    else
        rm -f "$DUCKDB_BACKUP"
        echo "ERROR: tar failed or produced empty backup" >&2
        exit 1
    fi
else
    echo "WARNING: DuckDB database not found at $DUCKDB_PATH"
fi

# Remove backups older than RETENTION_DAYS (quoted to handle variable expansion safely)
echo "Cleaning backups older than ${RETENTION_DAYS} days..."
find "$BACKUP_DIR" -name "*.tar.gz" -type f -mtime "+${RETENTION_DAYS}" -delete

# Hard cap: keep only the MAX_BACKUPS most-recent backups regardless of age
BACKUP_COUNT=$(find "$BACKUP_DIR" -name "*.tar.gz" -type f | wc -l)
if [ "$BACKUP_COUNT" -gt "$MAX_BACKUPS" ]; then
    TO_DELETE=$(( BACKUP_COUNT - MAX_BACKUPS ))
    echo "Backup count ($BACKUP_COUNT) exceeds MAX_BACKUPS ($MAX_BACKUPS) — removing $TO_DELETE oldest..."
    find "$BACKUP_DIR" -name "*.tar.gz" -type f -printf '%T+ %p\n' \
        | sort \
        | head -n "$TO_DELETE" \
        | awk '{print $2}' \
        | xargs rm -f
fi

echo "=== Backup complete ==="
