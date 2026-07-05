"""Ingest the Online Retail II CSV into DuckDB as the raw layer."""
import contextlib
import csv
import logging
import os
import sys

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(PROJECT_ROOT, "dbt_project", "dev.duckdb"))
CSV_PATH = os.environ.get("RAW_CSV_PATH", os.path.join(PROJECT_ROOT, "data", "online_retail_II.csv"))

_MINIMUM_ROW_COUNT = int(os.environ.get("INGEST_MIN_ROWS", "1000"))


def validate_csv(path: str) -> int:
    """Validate CSV readability and return the number of data rows."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found at {path}")
    logger.info("Reading CSV from %s", path)
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                raise ValueError(f"CSV at {path} is empty (no header row)")
            expected_cols = len(header)
            logger.info("CSV header OK: %d columns detected — %s", expected_cols, header)
            row_count = 0
            for row in reader:
                if len(row) != expected_cols:
                    logger.warning("Row %d has %d columns (expected %d): %s", row_count + 2, len(row), expected_cols, row)
                row_count += 1
            logger.info("CSV validated: %d data rows (including rows with missing cells)", row_count)
            return row_count
    except (OSError, csv.Error) as exc:
        raise RuntimeError(f"Failed to read or parse CSV at {path}: {exc}") from exc


def main() -> None:
    # Step 1: Validate source CSV
    logger.info("Starting ingest: CSV=%s, DB=%s", CSV_PATH, DB_PATH)
    try:
        csv_row_count = validate_csv(CSV_PATH)
        logger.info("CSV validation passed: %d rows found", csv_row_count)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        logger.error("CSV validation failed: %s", exc)
        sys.exit(1)

    # Step 2: Ensure database directory
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Step 3: Connect to DuckDB
    logger.info("Connecting to DuckDB at %s", DB_PATH)
    try:
        con = duckdb.connect(DB_PATH)
    except duckdb.Error as exc:
        logger.error("Failed to connect to DuckDB at %s: %s", DB_PATH, exc)
        sys.exit(1)

    try:
        # Step 4: Ingest into staging table
        logger.info("Ingesting CSV into staging table raw.online_retail_staging...")
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # Stage into a temporary table first so the existing table is only replaced
        # after a successful load — avoids a window where raw.online_retail is gone.
        con.execute("DROP TABLE IF EXISTS raw.online_retail_staging")
        con.execute("""
            CREATE TABLE raw.online_retail_staging AS
            SELECT
                "Invoice"      AS invoice_no,
                "StockCode"    AS stock_code,
                "Description"  AS description,
                "Quantity"     AS quantity,
                "InvoiceDate"  AS invoice_date,
                "Price"        AS unit_price,
                "Customer ID"  AS customer_id,
                "Country"      AS country,
                CURRENT_TIMESTAMP AS _ingested_at
            FROM read_csv(?, header=true, all_varchar=true)
        """, [CSV_PATH])

        # Step 5: Validate row count
        row_count = con.execute("SELECT COUNT(*) FROM raw.online_retail_staging").fetchone()[0]
        logger.info("Staging table loaded: %d rows", row_count)
        if row_count < _MINIMUM_ROW_COUNT:
            con.execute("ROLLBACK")
            logger.error(
                "Ingestion aborted: expected at least %d rows, got %d",
                _MINIMUM_ROW_COUNT,
                row_count,
            )
            sys.exit(1)

        # Step 6: Atomic swap — rename staging → production table
        logger.info("Swapping staging into raw.online_retail...")
        con.execute("DROP TABLE IF EXISTS raw.online_retail")
        con.execute("ALTER TABLE raw.online_retail_staging RENAME TO online_retail")
        con.execute("COMMIT")

        # Step 7: Verify final row count
        final_count = con.execute("SELECT COUNT(*) FROM raw.online_retail").fetchone()[0]
        logger.info("Ingested %d rows into raw.online_retail", final_count)
        if final_count < _MINIMUM_ROW_COUNT:
            logger.error(
                "Post-swap row count %d below minimum %d — this should be "
                "unreachable since the staging table was already validated; "
                "investigate the rename step",
                final_count,
                _MINIMUM_ROW_COUNT,
            )
            sys.exit(1)
        sample = con.execute("SELECT * FROM raw.online_retail LIMIT 3").fetchall()
        logger.info("Sample:\n%s", sample)
    except duckdb.Error as exc:
        with contextlib.suppress(duckdb.Error):
            con.execute("ROLLBACK")
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)
    finally:
        con.close()
        logger.info("DuckDB connection closed")


if __name__ == "__main__":
    main()
