# Digest pinned for reproducibility (verified 2026-06-28)
# Regenerate: docker pull apache/airflow:2.10.4-python3.12 && docker inspect \
#   --format='{{index .RepoDigests 0}}' apache/airflow:2.10.4-python3.12
FROM apache/airflow@sha256:65db1f804c24dfb2c4302150be9945c195141d6f615ab14138c05e5e6cce076a

# No system packages (e.g. git) are required: packages.yml only references
# dbt Hub packages, which dbt deps fetches over HTTPS, not via git clone.
# Avoid installing git in the production image to reduce attack surface
# (see CVE-2024-32002, CVE-2024-32465). Re-add a purge-after-use apt-get
# block here if a future dependency requires git at build or run time.

# Copy requirements file and install python packages
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
