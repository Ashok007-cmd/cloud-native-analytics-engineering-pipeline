"""Tests for pipeline scripts — unit tests for ingest_raw, dbt_cosmos_dag, and backup.sh."""
from __future__ import annotations

import contextlib
import csv
import json
import os
import subprocess
import sys
from datetime import UTC
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# ── Scripts directory & ci_setup ─────────────────────────────────────────────


def test_scripts_directory_exists() -> None:
    assert SCRIPTS_DIR.exists()
    expected = {"ingest_raw.py", "ci_setup.py", "backup.sh", "bootstrap_env.sh"}
    actual = {f.name for f in SCRIPTS_DIR.iterdir()}
    assert expected.issubset(actual), f"Missing scripts: {expected - actual}"


def test_ci_setup_imports_cleanly() -> None:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ci_setup_import_test", SCRIPTS_DIR / "ci_setup.py"
    )
    assert spec is not None
    assert spec.loader is not None


def test_ci_setup_generates_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util
    csv_path = tmp_path / "sample.csv"
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setenv("RAW_CSV_PATH", str(csv_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    spec = importlib.util.spec_from_file_location("ci_setup", SCRIPTS_DIR / "ci_setup.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    assert csv_path.exists()
    first_line = csv_path.read_text().splitlines()[0]
    expected_cols = {"Invoice", "StockCode", "Description", "Quantity",
                     "InvoiceDate", "Price", "Customer ID", "Country"}
    assert set(first_line.split(",")) == expected_cols


def test_ci_setup_row_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util
    csv_path = tmp_path / "count_test.csv"
    monkeypatch.setenv("RAW_CSV_PATH", str(csv_path))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "x.duckdb"))
    spec = importlib.util.spec_from_file_location("ci_setup_count", SCRIPTS_DIR / "ci_setup.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    lines = csv_path.read_text().splitlines()
    assert len(lines) - 1 == 3000


# ── ingest_raw.py ─────────────────────────────────────────────────────────────


def _make_valid_csv(tmp_path: Path, rows: int = 10) -> Path:
    """Helper: write a valid online_retail-style CSV to tmp_path."""
    p = tmp_path / "test.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Invoice", "StockCode", "Description", "Quantity",
                         "InvoiceDate", "Price", "Customer ID", "Country"])
        for i in range(rows):
            writer.writerow([f"INV{i:06d}", f"SKU{i:05d}", f"Item {i}",
                             str(i + 1), "2025-01-15 12:00:00",
                             f"{i + 1.99:.2f}", str(10000 + i), "UK"])
    return p


def test_validate_csv_returns_row_count(tmp_path: Path) -> None:
    from ingest_raw import validate_csv
    csv_path = _make_valid_csv(tmp_path, rows=5)
    assert validate_csv(str(csv_path)) == 5


def test_validate_csv_missing_file() -> None:
    from ingest_raw import validate_csv
    with pytest.raises(FileNotFoundError):
        validate_csv("/nonexistent/path.csv")


def test_validate_csv_empty_file(tmp_path: Path) -> None:
    from ingest_raw import validate_csv
    empty = tmp_path / "empty.csv"
    empty.touch()
    with pytest.raises(ValueError, match="empty"):
        validate_csv(str(empty))


def test_ingest_pipeline_full(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: create a CSV, run the full ingest pipeline, verify the DuckDB table."""
    import duckdb

    csv_file = _make_valid_csv(tmp_path, rows=20)
    db_file = tmp_path / "test.duckdb"
    monkeypatch.setenv("RAW_CSV_PATH", str(csv_file))
    monkeypatch.setenv("DUCKDB_PATH", str(db_file))
    monkeypatch.setenv("INGEST_MIN_ROWS", "5")
    # Reload module to pick up env overrides
    import importlib

    import ingest_raw as ir
    importlib.reload(ir)
    ir.main()
    assert db_file.exists()
    con = duckdb.connect(str(db_file))
    try:
        count = con.execute("SELECT COUNT(*) FROM raw.online_retail").fetchone()[0]
        assert count == 20
    finally:
        con.close()


# ── ingest_raw.py: edge cases ─────────────────────────────────────────────


def test_validate_csv_column_mismatch(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    from ingest_raw import validate_csv
    csv_path = _make_valid_csv(tmp_path, rows=3)
    with open(csv_path, "a") as f:
        f.write("only_one_col\n")
    validate_csv(str(csv_path))
    assert "columns" in caplog.text


def test_validate_csv_oserror(tmp_path: Path) -> None:
    """Make CSV unreadable to trigger OSError → RuntimeError."""
    from ingest_raw import validate_csv
    csv_path = _make_valid_csv(tmp_path, rows=1)
    csv_path.chmod(0o000)
    with pytest.raises(RuntimeError, match="Failed to read"):
        validate_csv(str(csv_path))
    csv_path.chmod(0o644)


def _run_main_with_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    """Helper: reload ingest_raw.main() with env vars set via monkeypatch (auto-cleanup)."""
    import importlib
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import ingest_raw as ir
    importlib.reload(ir)
    ir.main()


def test_ingest_main_csv_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "x.duckdb"
    with pytest.raises(SystemExit):
        _run_main_with_env(monkeypatch, {
            "RAW_CSV_PATH": "/nonexistent/input.csv",
            "DUCKDB_PATH": str(db_file),
        })


def test_ingest_main_row_count_too_low(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = _make_valid_csv(tmp_path, rows=3)
    db_file = tmp_path / "low.duckdb"
    with pytest.raises(SystemExit):
        _run_main_with_env(monkeypatch, {
            "RAW_CSV_PATH": str(csv_file),
            "DUCKDB_PATH": str(db_file),
            "INGEST_MIN_ROWS": "100",
        })


def test_ingest_main_duckdb_connect_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = _make_valid_csv(tmp_path, rows=5)
    db_dir = tmp_path / "not_a_file"
    db_dir.mkdir()
    with pytest.raises(SystemExit):
        _run_main_with_env(monkeypatch, {
            "RAW_CSV_PATH": str(csv_file),
            "DUCKDB_PATH": str(db_dir),
            "INGEST_MIN_ROWS": "1",
        })


def test_ingest_main_duckdb_ingest_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import duckdb
    csv_file = _make_valid_csv(tmp_path, rows=5)
    db_file = tmp_path / "ingest_fail.duckdb"

    def failing_execute(*args, **kwargs):
        raise duckdb.Error("INSERT failed")
    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", failing_execute)

    with pytest.raises(SystemExit):
        _run_main_with_env(monkeypatch, {
            "RAW_CSV_PATH": str(csv_file),
            "DUCKDB_PATH": str(db_file),
            "INGEST_MIN_ROWS": "1",
        })


# ── dbt_cosmos_dag.py ────────────────────────────────────────────────────────


@contextlib.contextmanager
def _mock_cosmos_imports():
    """Mock cosmos and airflow modules so the DAG module can be imported without Airflow DB."""
    import types as _types
    cosmos_mock = _types.ModuleType("cosmos")
    cosmos_mock.DbtDag = MagicMock()
    cosmos_mock.ExecutionConfig = MagicMock()
    cosmos_mock.ProfileConfig = MagicMock()
    cosmos_mock.ProjectConfig = MagicMock()
    cosmos_mock.__spec__ = None

    cosmos_config = _types.ModuleType("cosmos.config")
    cosmos_config.__spec__ = None

    for mod_name in ("cosmos", "cosmos.config", "cosmos.constants",
                     "cosmos.converter", "cosmos.operators", "cosmos.operators.local"):
        sys.modules.setdefault(mod_name, _types.ModuleType(mod_name))
        sys.modules[mod_name].__spec__ = None
        sys.modules[mod_name].DbtDag = MagicMock()
        if hasattr(sys.modules[mod_name], "__file__"):
            del sys.modules[mod_name].__file__
    sys.modules["cosmos"] = cosmos_mock
    # Ensure the submodule stubs have the right attributes
    for sub in ("config", "constants", "converter", "operators", "operators.local"):
        full = f"cosmos.{sub}"
        m = _types.ModuleType(full)
        m.__spec__ = None
        m.DbtDag = MagicMock()
        sys.modules.setdefault(full, m)
        # Parent chain
        parts = full.split(".")
        parent = sys.modules["cosmos"]
        for p in parts[1:]:
            if not hasattr(parent, p):
                setattr(parent, p, _types.ModuleType(p))
            parent = getattr(parent, p)
    yield


def _import_dag_module():
    """Import dbt_cosmos_dag.py as a module for unit-testing its pure functions."""
    import importlib.util
    dag_path = Path(__file__).parent.parent / "airflow" / "dags" / "dbt_cosmos_dag.py"
    mod_name = "dbt_cosmos_dag_test"
    with _mock_cosmos_imports():
        spec = importlib.util.spec_from_file_location(mod_name, str(dag_path))
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
    return mod


def test_dag_extract_context_full() -> None:
    from datetime import datetime
    dag = _import_dag_module()
    ctx = {
        "ts": datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
        "dag_run": type("DR", (), {"dag_id": "test_dag"})(),
        "task": type("T", (), {"task_id": "test_task"})(),
        "logical_date": "2026-06-01",
        "exception": "Something broke",
    }
    info = dag._extract_context_info(ctx)
    assert info["dag_id"] == "test_dag"
    assert info["task_id"] == "test_task"
    assert "Something broke" in info["error"]


def test_dag_extract_context_minimal() -> None:
    dag = _import_dag_module()
    info = dag._extract_context_info({})
    assert info["timestamp"] is not None
    assert info["dag_id"] == "unknown_dag"
    assert info["task_id"] == "unknown_task"


def test_dag_write_failure_log(tmp_path: Path) -> None:
    dag = _import_dag_module()
    log_path = str(tmp_path / "failures.log")
    entry = {"error": "test", "dag_id": "d", "task_id": "t"}
    dag._write_failure_log(entry, log_path)
    with open(log_path) as f:
        written = json.loads(f.readline())
    assert written["error"] == "test"
    assert written["dag_id"] == "d"


def test_dag_failure_callback_empty_context(caplog: pytest.LogCaptureFixture) -> None:
    dag = _import_dag_module()
    dag.dag_failure_callback({})
    assert "Empty context" in caplog.text


def test_dag_slack_alert_sends() -> None:
    dag = _import_dag_module()
    dag._requests = MagicMock()
    entry = {"error": "boom", "dag_id": "d", "task_id": "t"}
    dag.SLACK_WEBHOOK_URL = "https://hooks.slack.com/fake"
    dag._send_slack_alert(entry)
    dag._requests.post.assert_called_once()


# ── dbt_cosmos_dag.py: edge cases ─────────────────────────────────────────


def test_dag_extract_context_ts_no_isoformat() -> None:
    dag = _import_dag_module()
    ctx = {"ts": 12345}
    info = dag._extract_context_info(ctx)
    assert info["timestamp"] == "12345"


def test_dag_write_failure_log_oserror(caplog: pytest.LogCaptureFixture) -> None:
    dag = _import_dag_module()
    dag._write_failure_log({"error": "x"}, "/nonexistent/dir/failures.log")
    assert "Failed to write" in caplog.text


def test_dag_slack_no_webhook() -> None:
    dag = _import_dag_module()
    dag.SLACK_WEBHOOK_URL = ""
    dag._requests = MagicMock()
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})
    dag._requests.post.assert_not_called()


def test_dag_slack_no_requests() -> None:
    dag = _import_dag_module()
    dag.SLACK_WEBHOOK_URL = "https://hooks.slack.com/fake"
    dag._requests = None
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})
    # No error — just returns early


def _make_dag_with_slack_post() -> tuple[object, MagicMock]:
    """Helper: import dag module, keep real requests module, mock post."""
    import requests
    dag = _import_dag_module()
    dag.SLACK_WEBHOOK_URL = "https://hooks.slack.com/fake"
    dag._requests = requests
    mock_post = MagicMock()
    dag._requests.post = mock_post  # type: ignore[method-assign]
    return dag, mock_post


def test_dag_slack_timeout() -> None:
    import requests
    dag, post = _make_dag_with_slack_post()
    post.side_effect = requests.exceptions.Timeout("timed out")
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})  # should not raise


def test_dag_slack_connection_error() -> None:
    import requests
    dag, post = _make_dag_with_slack_post()
    post.side_effect = requests.exceptions.ConnectionError("conn failed")
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})


def test_dag_slack_http_error() -> None:
    import requests
    dag, post = _make_dag_with_slack_post()
    post.side_effect = requests.exceptions.HTTPError("400 Bad Request")
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})


def test_dag_slack_generic_error() -> None:
    dag, post = _make_dag_with_slack_post()
    post.side_effect = RuntimeError("unexpected")
    dag._send_slack_alert({"dag_id": "d", "task_id": "t", "error": "e"})


def test_dag_failure_callback_full_path(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    dag = _import_dag_module()
    dag.SLACK_WEBHOOK_URL = "https://hooks.slack.com/fake"
    dag._requests = MagicMock()

    from datetime import datetime
    ctx = {
        "ts": datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
        "dag_run": type("DR", (), {"dag_id": "test_dag"})(),
        "task": type("T", (), {"task_id": "test_task"})(),
        "exception": "Something broke",
    }
    dag.dag_failure_callback(ctx)
    assert "ALERT - DAG failure" in caplog.text
    assert "test_dag" in caplog.text
    dag._requests.post.assert_called_once()


# ── backup.sh (subprocess) ────────────────────────────────────────────────────


def test_backup_script_exists() -> None:
    backup = SCRIPTS_DIR / "backup.sh"
    assert backup.exists(), "backup.sh must exist"
    assert os.access(str(backup), os.X_OK), "backup.sh must be executable"


def test_backup_script_creates_tar_gz(tmp_path: Path) -> None:
    """Run backup.sh with a real DuckDB file; verify a .tar.gz is created."""
    import duckdb
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t AS SELECT 1 AS a")
    con.close()
    assert db_path.exists()
    backup_dir = tmp_path / "bkup"
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        env={
            **os.environ,
            "DUCKDB_PATH": str(db_path),
            "BACKUP_DIR": str(backup_dir),
            "RETENTION_DAYS": "30",
            "MAX_BACKUPS": "10",
        },
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"backup.sh failed:\nstdout:{result.stdout}\nstderr:{result.stderr}"
    archives = list(backup_dir.glob("*.tar.gz"))
    assert len(archives) >= 1, f"No archives found in {backup_dir}: {result.stdout}"
    # A backup must preserve the working copy — only an explicit restore/delete removes it
    assert db_path.exists(), "Original DuckDB file must remain after backup"
    # Archive should be non-empty
    assert archives[0].stat().st_size > 0


def test_backup_script_safe_backup_dir_rejected() -> None:
    """backup.sh must refuse BACKUP_DIR=/."""
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        env={**os.environ, "BACKUP_DIR": "/"},
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1
    assert "unsafe" in result.stderr.lower()


def test_backup_script_empty_backup_dir_allowed(tmp_path: Path) -> None:
    """backup.sh should work when BACKUP_DIR is a non-existent path (creates it)."""
    import duckdb
    db_path = tmp_path / "src" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t AS SELECT 1 AS a")
    con.close()
    backup_dir = tmp_path / "new_backups"
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        env={
            **os.environ,
            "DUCKDB_PATH": str(db_path),
            "BACKUP_DIR": str(backup_dir),
            "RETENTION_DAYS": "30",
            "MAX_BACKUPS": "10",
        },
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"backup.sh failed:\n{result.stderr}"
    assert backup_dir.exists()
    archives = list(backup_dir.glob("*.tar.gz"))
    assert len(archives) >= 1


def test_restore_script_round_trip(tmp_path: Path) -> None:
    """backup.sh then restore.sh should recover a deleted/corrupted DuckDB file."""
    import duckdb

    db_rel = "dev.duckdb"
    con = duckdb.connect(str(tmp_path / db_rel))
    con.execute("CREATE TABLE t AS SELECT 42 AS a")
    con.close()

    backup_result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        cwd=tmp_path,
        env={**os.environ, "DUCKDB_PATH": db_rel, "BACKUP_DIR": "backups"},
        capture_output=True, text=True, timeout=30,
    )
    assert backup_result.returncode == 0, backup_result.stderr
    archive = next((tmp_path / "backups").glob("*.tar.gz"))

    # Simulate data loss, then restore
    (tmp_path / db_rel).unlink()
    restore_result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "restore.sh"), str(archive)],
        cwd=tmp_path,
        env={**os.environ, "DUCKDB_PATH": db_rel},
        capture_output=True, text=True, timeout=30,
    )
    assert restore_result.returncode == 0, restore_result.stderr
    con = duckdb.connect(str(tmp_path / db_rel))
    assert con.execute("SELECT a FROM t").fetchone() == (42,)
    con.close()


def test_restore_script_missing_file_rejected(tmp_path: Path) -> None:
    """restore.sh must fail cleanly when the given archive doesn't exist."""
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "restore.sh"), str(tmp_path / "nope.tar.gz")],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1
    assert "not found" in result.stderr.lower()


# ── backup.sh: edge cases ─────────────────────────────────────────────────


def test_backup_script_no_db_file(tmp_path: Path) -> None:
    """backup.sh should handle a missing DuckDB file gracefully."""
    fake_db = tmp_path / "nonexistent.duckdb"
    backup_dir = tmp_path / "empty_backup"
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        env={
            **os.environ,
            "DUCKDB_PATH": str(fake_db),
            "BACKUP_DIR": str(backup_dir),
        },
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0
    assert "not found" in result.stdout.lower()


def test_backup_script_empty_backup_dir(tmp_path: Path) -> None:
    """backup.sh on an empty backup dir should succeed (no old archives to clean)."""
    import duckdb
    db_path = tmp_path / "src" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t AS SELECT 1 AS a")
    con.close()
    backup_dir = tmp_path / "backup"
    result = subprocess.run(
        ["bash", str(SCRIPTS_DIR / "backup.sh")],
        env={
            **os.environ,
            "DUCKDB_PATH": str(db_path),
            "BACKUP_DIR": str(backup_dir),
        },
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert backup_dir.exists()
