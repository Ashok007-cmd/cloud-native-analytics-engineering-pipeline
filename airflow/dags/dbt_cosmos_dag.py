import contextlib
import json
import logging
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import requests as _requests
except ImportError:
    _requests = None

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def _extract_context_info(context: dict[str, Any]) -> dict[str, str]:
    """Safely extract and normalize fields from the Airflow context dict."""
    ts_val = context.get("ts")
    if ts_val is None:
        timestamp = datetime.now(tz=UTC).isoformat()
    elif hasattr(ts_val, "isoformat"):
        timestamp = ts_val.isoformat()
    else:
        timestamp = str(ts_val)

    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, 'dag_id', 'unknown_dag')

    task = context.get("task")
    task_id_val = getattr(task, 'task_id', 'unknown_task')

    return {
        "timestamp": timestamp,
        "dag_id": dag_id,
        "task_id": task_id_val,
        "logical_date": str(context.get("logical_date", "")),
        "error": str(context.get("exception", "Unknown error")),
    }


def _write_failure_log(log_entry: dict[str, str], log_path: str) -> None:
    """Append a structured JSON log entry to the failure log file."""
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        # Ensure log file is readable by the airflow group
        with contextlib.suppress(OSError):
            os.chmod(log_path, 0o640)
    except OSError as exc:
        logging.exception("Failed to write DAG failure log to %s: %s", log_path, exc)


def _send_slack_alert(log_entry: dict[str, str]) -> None:
    """Send a structured failure alert to the configured Slack webhook."""
    if not SLACK_WEBHOOK_URL or not _requests:
        return
    try:
        slack_text = (
            f":red_circle: *DAG failure*: `{log_entry['dag_id']}` "
            f"| Task: `{log_entry['task_id']}` "
            f"| Error: {log_entry['error']}"
        )
        response = _requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": slack_text},
            timeout=5,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except _requests.exceptions.Timeout:
        logging.warning("Slack webhook request timed out after 5s")
    except _requests.exceptions.ConnectionError:
        logging.warning("Slack webhook connection failed")
    except _requests.exceptions.HTTPError as e:
        logging.warning("Slack webhook HTTP error: %s", e)
    except Exception as slack_exc:
        logging.warning("Failed to send Slack alert: %s", slack_exc)


def dag_failure_callback(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback for DAG failures with logging and Slack alerting."""
    if not context:
        logging.warning("dag_failure_callback: Empty context provided")
        return

    log_entry = _extract_context_info(context)
    log_path = "/opt/airflow/logs/dag_failures.log"
    _write_failure_log(log_entry, log_path)
    logging.error(
        "ALERT - DAG failure: %s | Task: %s | Error: %s",
        log_entry["dag_id"],
        log_entry["task_id"],
        log_entry["error"],
    )
    _send_slack_alert(log_entry)


# Pool is created by the Airflow init container via docker-compose.yaml
# to prevent task starvation when multiple dbt tasks contend for DuckDB's single-writer lock.

# Dynamically resolve paths to support both local development and Docker containers
DAGS_DIR = Path(__file__).parent.resolve()
LOCAL_DBT_PROJECT = DAGS_DIR.parent.parent / "dbt_project"
DOCKER_DBT_PROJECT = Path("/opt/airflow/dbt_project")

dbt_project_path = str(DOCKER_DBT_PROJECT if DOCKER_DBT_PROJECT.exists() else LOCAL_DBT_PROJECT.resolve())

profiles_yml_path = os.path.join(dbt_project_path, "profiles.yml")

# Validate dbt executable
dbt_executable = shutil.which("dbt") or "/usr/local/bin/dbt"
if not os.path.exists(dbt_executable):
    logging.warning("dbt executable not found at %s — tasks will fail at runtime", dbt_executable)

profile_config = ProfileConfig(
    profile_name=os.environ.get("DBT_PROFILE", "duckdb"),
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profiles_yml_filepath=profiles_yml_path,
)

# Dynamically determine if we need the DuckDB concurrency limit pool
target_name = os.environ.get("DBT_TARGET", "dev")
operator_args = {"install_deps": True}
if target_name == "dev":
    operator_args["pool"] = "duckdb_pool"

dbt_dag = DbtDag(
    dag_id="online_retail_elt",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": dag_failure_callback,
    },
    project_config=ProjectConfig(
        dbt_project_path,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=dbt_executable,
    ),
    operator_args=operator_args,
    tags=["elt", "online_retail"],
)
