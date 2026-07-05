# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| main    | Yes       |

## Reporting a vulnerability

Do NOT open a public GitHub issue for security vulnerabilities.

Email **vashokkumar2012001@gmail.com** with:
- Description of the vulnerability
- Steps to reproduce
- Potential impact

You will receive a response within 72 hours. If confirmed, a fix will be released within 14 days.

## Credential handling

- All secrets (Fernet key, DB passwords, Snowflake credentials) are stored in environment variables only.
- `.env` files are `.gitignore`d; only `.env.example` (with placeholder values) is committed.
- `airflow/airflow.cfg` is `.gitignore`d — Airflow auto-generates it locally on first run, and it always contains a live `fernet_key`. An earlier version of this file was briefly committed with a real key; it has since been untracked and the real value purged from git history entirely (see `AUDIT_REPORT.md`). Rotate your local Fernet key if you ever cloned this repo before that fix.
- CI uses GitHub Actions secrets; credentials are never committed to the repository.
- Secret scanning runs on every PR via `detect-secrets` (baseline: `.secrets.baseline`).

## Docker security

- Airflow webserver port is bound to `127.0.0.1:8080` only (not `0.0.0.0`).
- Postgres service is on an isolated Docker network, not exposed to the host.

## Dependency management

- Python dependencies are pinned in `requirements-dev.txt` and `airflow/requirements.txt`.
- GitHub Actions uses SHA-pinned action versions to prevent supply-chain attacks.
- dbt packages are version-bounded in `dbt_project/packages.yml`.
