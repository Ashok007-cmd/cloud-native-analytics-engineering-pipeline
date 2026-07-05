#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/airflow/.env"
EXAMPLE_FILE="$PROJECT_ROOT/airflow/.env.example"

if [ -f "$ENV_FILE" ]; then
    echo "Warning: $ENV_FILE already exists. Skipping generation."
    exit 0
fi

echo "Generating $ENV_FILE from .env.example..."
cp "$EXAMPLE_FILE" "$ENV_FILE"

echo "Generating cryptographically secure Fernet key..."
if command -v python3 &>/dev/null; then
    PYTHON=python3
elif command -v python &>/dev/null; then
    PYTHON=python
else
    echo "ERROR: Python not found. Cannot generate Fernet key." >&2
    exit 1
fi

FERNET_KEY=$($PYTHON -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") || {
    echo "ERROR: Failed to generate Fernet key. Is 'cryptography' package installed?" >&2
    exit 1
}

if [ -z "$FERNET_KEY" ]; then
    echo "ERROR: Generated Fernet key is empty." >&2
    exit 1
fi

echo "Generating random Postgres password..."
PG_PASSWORD=$($PYTHON -c "import secrets; print(secrets.token_urlsafe(24))") || {
    echo "ERROR: Failed to generate Postgres password." >&2
    exit 1
}

echo "Generating random admin password..."
ADMIN_PASSWORD=$($PYTHON -c "import secrets; print(secrets.token_urlsafe(24))") || {
    echo "ERROR: Failed to generate admin password." >&2
    exit 1
}

if [ -z "$PG_PASSWORD" ] || [ -z "$ADMIN_PASSWORD" ]; then
    echo "ERROR: Generated password is empty." >&2
    exit 1
fi

# Targeted per-key replacement — both AIRFLOW_PG_PASSWORD and AIRFLOW_ADMIN_PASSWORD
# use the same <change_me> placeholder text in .env.example, so a blanket find/replace
# would assign both credentials the identical value. Match on the full key=value line
# instead so each secret is independent.
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s|<generate_with_command_above>|$FERNET_KEY|g" "$ENV_FILE"
    sed -i '' "s|^AIRFLOW_PG_PASSWORD=<change_me>|AIRFLOW_PG_PASSWORD=$PG_PASSWORD|" "$ENV_FILE"
    sed -i '' "s|^AIRFLOW_ADMIN_PASSWORD=<change_me>|AIRFLOW_ADMIN_PASSWORD=$ADMIN_PASSWORD|" "$ENV_FILE"
else
    sed -i "s|<generate_with_command_above>|$FERNET_KEY|g" "$ENV_FILE"
    sed -i "s|^AIRFLOW_PG_PASSWORD=<change_me>|AIRFLOW_PG_PASSWORD=$PG_PASSWORD|" "$ENV_FILE"
    sed -i "s|^AIRFLOW_ADMIN_PASSWORD=<change_me>|AIRFLOW_ADMIN_PASSWORD=$ADMIN_PASSWORD|" "$ENV_FILE"
fi

chmod 600 "$ENV_FILE"

echo "Successfully created $ENV_FILE with secure keys (permissions set to 600)."
