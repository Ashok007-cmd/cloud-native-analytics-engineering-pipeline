# AI/ML Integration Guide — Cloud-Native Analytics Engineering Pipeline

> **Context:** Online Retail II (1M+ transactions), DuckDB dev / Snowflake prod, dbt 1.11.11, Airflow 2.10.4 + Cosmos, GitHub Actions CI  
> **Date:** 2026-07-03  
> **Status:** Research & Recommendations

---

## Table of Contents

1. [Anomaly Detection in dbt](#1-anomaly-detection-in-dbt)
2. [Customer Churn Prediction Pipeline](#2-customer-churn-prediction-pipeline)
3. [Forecast Integration](#3-forecast-integration)
4. [LLM-Enhanced Data Catalog](#4-llm-enhanced-data-catalog)
5. [Cross-Cutting Recommendations](#5-cross-cutting-recommendations)
6. [Implementation Roadmap](#6-implementation-roadmap)

---

## 1. Anomaly Detection in dbt

### 1.1 What You Already Have

- `dbt-expectations` v0.10.x is **already installed** as a package dependency
- `dbt_utils` is also installed — provides row-count comparison utilities
- Singular tests exist: `assert_fct_orders_row_count_matches_staging.sql`, `assert_no_high_value_customers_with_negative_revenue.sql`
- Custom macros: `test_positive_value`, `test_not_in_future`
- `fct_orders` has `is_return`, `quantity`, `revenue` columns — all anomaly-amenable

### 1.2 Approach A: SQL-Based Anomaly Tests via dbt-expectations (Recommended MVP)

The `dbt-expectations` package provides `expect_column_values_to_be_within_n_moving_stdevs` — a z-score test with a rolling window that adapts to trends. **This is the single highest-ROI AI-adjacent investment** because you already have the package installed.

#### What It Detects

| Anomaly | Signal | Model |
|---------|--------|-------|
| Revenue dip | Daily `revenue` sum drops >3σ from rolling mean | `fct_orders` daily |
| Quantity spike | Bot/scraper order flood | `fct_orders` daily by `country` |
| Customer churn signal | New order rate drops in `int_customer_metrics` | `dim_customers_snapshot` |
| Product return surge | `return_count` climbs >3σ | `int_product_metrics` |
| Data pipeline failure | Row count drops to near-zero | `fct_orders` row count |

#### Implementation

**Step 1: Create a daily aggregate model for anomaly monitoring**

`dbt_project/models/marts/rpt_daily_metrics.sql`:

```sql
{{
    config(
        materialized='table',
        cluster_by='date_day'
    )
}}

WITH daily_revenue AS (
    SELECT
        d.full_date                                         AS date_day,
        COUNT(DISTINCT f.order_item_id)                     AS order_item_count,
        COUNT(DISTINCT f.invoice_no)                        AS order_count,
        SUM(CASE WHEN NOT f.is_return THEN f.revenue ELSE 0 END) AS gross_revenue,
        SUM(f.revenue)                                      AS net_revenue,
        SUM(CASE WHEN f.is_return THEN 1 ELSE 0 END)        AS return_item_count,
        COUNT(DISTINCT f.customer_key)                      AS active_customers,
        COUNT(DISTINCT f.product_key)                       AS active_products
    FROM {{ ref('fct_orders') }} AS f
    INNER JOIN {{ ref('dim_dates') }} AS d
        ON f.date_key = d.date_key
    GROUP BY d.full_date
)

SELECT
    date_day,
    order_item_count,
    order_count,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(net_revenue, 2)   AS net_revenue,
    return_item_count,
    active_customers,
    active_products,
    -- Lag 1-day for easy anomaly checking
    LAG(order_item_count, 1) OVER (ORDER BY date_day) AS prev_day_orders,
    LAG(gross_revenue, 1)   OVER (ORDER BY date_day) AS prev_day_revenue
FROM daily_revenue
```

**Step 2: Add anomaly detection tests to `schema.yml`**

Add under the `rpt_daily_metrics` model entry in `schema.yml`:

```yaml
  - name: rpt_daily_metrics
    description: >
      Daily aggregated metrics for anomaly detection monitoring.
      One row per calendar day with order counts, revenue, and customer activity.
    columns:
      - name: date_day
        tests:
          - unique
          - not_null
      - name: gross_revenue
        tests:
          - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
              date_column_name: date_day
              period: day
              lookback_periods: 1          # compare to yesterday
              trend_periods: 28            # 4-week rolling window
              test_periods: 14             # check last 14 days
              sigma_threshold: 3           # flag >3σ deviations
              take_logs: false             # revenue is already log-normal-ish
              group_by: []                 # no grouping (would need daily revenue per country)
              severity: warn               # don't break the build — notify
      - name: order_item_count
        tests:
          - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
              date_column_name: date_day
              period: day
              lookback_periods: 1
              trend_periods: 28
              test_periods: 14
              sigma_threshold: 3
              take_logs: true              # order counts benefit from log transform
              severity: warn
      - name: return_item_count
        tests:
          - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
              date_column_name: date_day
              period: day
              lookback_periods: 1
              trend_periods: 28
              test_periods: 14
              sigma_threshold: 3
              severity: warn
```

**Step 3: Add a row-count drift test for fct_orders**

Add to the `fct_orders` entry in `schema.yml`:

```yaml
      - name: order_item_id
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
              date_column_name: invoice_date
              period: day
              lookback_periods: 1
              trend_periods: 28
              test_periods: 7
              sigma_threshold: 3
              take_logs: true
              severity: error              # row count drop = pipeline failure
```

But wait — `order_item_id` isn't aggregated daily and `invoice_date` is a timestamp. For row-count drift, you need a daily aggregate. Better approach: **add a singular test** that computes daily row counts and checks against a rolling z-score:

`dbt_project/tests/assert_daily_row_count_within_3sigma.sql`:

```sql
-- Singular test: daily fct_orders row count must stay within 3σ
-- of the trailing 28-day rolling average.
WITH daily_counts AS (
    SELECT
        CAST(invoice_date AS DATE) AS day,
        COUNT(*) AS n
    FROM {{ ref('fct_orders') }}
    GROUP BY CAST(invoice_date AS DATE)
),
stats AS (
    SELECT
        day,
        n,
        AVG(n) OVER (ORDER BY day ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS rolling_avg,
        STDDEV(n) OVER (ORDER BY day ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS rolling_std
    FROM daily_counts
)
SELECT day, n, rolling_avg, rolling_std,
       (n - rolling_avg) / NULLIF(rolling_std, 0) AS z_score
FROM stats
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
  AND rolling_std IS NOT NULL
  AND ABS((n - rolling_avg) / NULLIF(rolling_std, 0)) > 3
```

**Step 4: Add country-level anomaly detection**

For detecting geographic shifts (e.g., all revenue suddenly from one country):

`dbt_project/models/marts/rpt_daily_metrics_by_country.sql`:

```sql
{{
    config(
        materialized='table',
        cluster_by='date_day'
    )
}}

SELECT
    d.full_date         AS date_day,
    f.country,
    COUNT(*)            AS order_item_count,
    SUM(f.revenue)      AS net_revenue,
    SUM(CASE WHEN NOT f.is_return THEN f.revenue ELSE 0 END) AS gross_revenue
FROM {{ ref('fct_orders') }} AS f
INNER JOIN {{ ref('dim_dates') }} AS d
    ON f.date_key = d.date_key
GROUP BY d.full_date, f.country
```

Then add `group_by: [country]` parameter to the revenue anomaly test.

### 1.3 Approach B: Elementary Data (Production-Grade)

For production anomaly detection with a UI, alerting, and Slack integration:

```bash
pip install elementary-data
```

```bash
edr init  # creates profiles.yml for Elementary
edr monitor  # runs anomaly detection + sends alerts
```

**Pros over dbt-expectations:**
- Automatic training period vs. detection period separation
- Seasonality-aware (day-of-week baselines)
- Built-in alerting (Slack, email, PagerDuty)
- Web UI for investigation
- Column-level drift detection (data type, null %, distribution)

**Cons:**
- Another service to run (can run as a dbt operation or Airflow task)
- Additional Python dependency
- The free tier has a 10k-row sample limit for some features

**Recommendation:** Start with Approach A (dbt-expectations, zero new deps). If alert fatigue becomes an issue, add Elementary for its seasonality handling.

### 1.4 CI/CD Integration

Add to `.github/workflows/ci.yml` after the `dbt build` step:

```yaml
      - name: Run anomaly detection on CI
        if: github.ref == 'refs/heads/main'  # only on main branch
        env:
          DUCKDB_PATH: dbt_project/ci.duckdb
        run: |
          cd dbt_project && dbt test --select tag:anomaly --profiles-dir .
```

And tag your anomaly tests in `schema.yml`:

```yaml
tests:
  - dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:
      tags: [anomaly]
      ...
```

### 1.5 Pitfalls & Mitigations

| Pitfall | Why | Mitigation |
|---------|-----|------------|
| 3σ on 500 tests = ~1 false positive/day | Pure statistics | Use `severity: warn` not `error`; tier alerts |
| Saturday ≠ Tuesday baseline | Day-of-week seasonality | Use `group_by` with day-of-week, or switch to Elementary |
| Cold start — no history | First 28 days have no rolling stats | Run `dbt test --exclude tag:anomaly` for the first month |
| Revenue has weekly seasonality | Retail data | Set `trend_periods: 35` (5 weeks) to smooth |

---

## 2. Customer Churn Prediction Pipeline

### 2.1 What You Already Have

- `int_customer_metrics` has: `customer_id`, `total_orders`, `total_revenue`, `gross_revenue`, `recency_days`, `return_count`, `customer_segment`
- `dim_customers_snapshot` has SCD2 history — enables time-series feature engineering
- `fct_orders` has line-level data going back to 2009
- RFM segmentation is already computed (`high_value`, `medium_value`, `low_value`)

### 2.2 Feature Engineering (dbt Intermediate Models)

**Step 1: Add behavioral features for churn prediction**

`dbt_project/models/intermediate/int_churn_features.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

WITH customer_months AS (
    -- Generate one row per customer per month they were active
    SELECT
        f.customer_key,
        DATE_TRUNC('month', f.invoice_date) AS month,
        COUNT(DISTINCT f.invoice_no) AS orders_in_month,
        SUM(f.quantity) AS quantity_in_month,
        SUM(CASE WHEN NOT f.is_return THEN f.revenue ELSE 0 END) AS gross_revenue_in_month,
        SUM(CASE WHEN f.is_return THEN 1 ELSE 0 END) AS returns_in_month,
        COUNT(DISTINCT f.product_key) AS products_in_month,
        COUNT(DISTINCT f.country) AS countries_in_month
    FROM {{ ref('fct_orders') }} AS f
    GROUP BY f.customer_key, DATE_TRUNC('month', f.invoice_date)
),

customer_features AS (
    SELECT
        cm.customer_key,
        cm.month,
        cm.orders_in_month,
        cm.quantity_in_month,
        cm.gross_revenue_in_month,
        cm.returns_in_month,
        cm.products_in_month,
        cm.countries_in_month,
        -- Trailing 3-month features
        SUM(cm.orders_in_month) OVER (
            PARTITION BY cm.customer_key
            ORDER BY cm.month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS orders_prev_3m,
        AVG(cm.gross_revenue_in_month) OVER (
            PARTITION BY cm.customer_key
            ORDER BY cm.month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_revenue_prev_3m,
        -- Month-over-month change
        cm.gross_revenue_in_month - LAG(cm.gross_revenue_in_month, 1) OVER (
            PARTITION BY cm.customer_key ORDER BY cm.month
        ) AS revenue_change_mom,
        -- Gap since last order (in months)
        CASE WHEN LAG(cm.month, 1) OVER (
            PARTITION BY cm.customer_key ORDER BY cm.month
        ) IS NOT NULL THEN
            DATEDIFF('month', LAG(cm.month, 1) OVER (
                PARTITION BY cm.customer_key ORDER BY cm.month
            ), cm.month)
        ELSE 0 END AS months_since_prev_order
    FROM customer_months AS cm
),

churn_labels AS (
    -- Label: customer churned if they had no activity in the next 3 months
    SELECT
        cf.*,
        CASE
            WHEN LEAD(cf.month, 1) OVER (
                PARTITION BY cf.customer_key ORDER BY cf.month
            ) IS NULL THEN NULL  -- can't determine — most recent period
            WHEN DATEDIFF('month', cf.month, LEAD(cf.month, 1) OVER (
                PARTITION BY cf.customer_key ORDER BY cf.month
            )) > 3 THEN 1  -- churned (gap > 3 months)
            ELSE 0          -- active
        END AS is_churned
    FROM customer_features AS cf
)

SELECT
    cl.customer_key,
    cl.month,
    cl.orders_in_month,
    cl.quantity_in_month,
    cl.gross_revenue_in_month,
    cl.returns_in_month,
    cl.products_in_month,
    cl.countries_in_month,
    cl.orders_prev_3m,
    cl.avg_revenue_prev_3m,
    cl.revenue_change_mom,
    cl.months_since_prev_order,
    COALESCE(cl.is_churned, -1) AS is_churned,  -- -1 = unknown, 0 = active, 1 = churned
    CASE WHEN cl.gross_revenue_in_month > 0
        THEN cl.returns_in_month * 1.0 / NULLIF(cl.gross_revenue_in_month, 0)
        ELSE 0
    END AS return_rate
FROM churn_labels AS cl
```

**Step 2: Create a view for inference input**

`dbt_project/models/marts/rpt_churn_scores.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

-- Latest features per customer for batch churn scoring
WITH latest_features AS (
    SELECT
        cf.customer_key,
        cf.month,
        cf.orders_in_month,
        cf.quantity_in_month,
        cf.gross_revenue_in_month,
        cf.returns_in_month,
        cf.products_in_month,
        cf.countries_in_month,
        cf.orders_prev_3m,
        cf.avg_revenue_prev_3m,
        cf.revenue_change_mom,
        cf.months_since_prev_order,
        cf.return_rate,
        ROW_NUMBER() OVER (
            PARTITION BY cf.customer_key
            ORDER BY cf.month DESC
        ) AS rn
    FROM {{ ref('int_churn_features') }} AS cf
)

SELECT
    lf.customer_key,
    d.customer_id,
    d.customer_segment,
    lf.month                                   AS last_active_month,
    lf.orders_in_month,
    lf.gross_revenue_in_month,
    lf.orders_prev_3m,
    lf.avg_revenue_prev_3m,
    lf.revenue_change_mom,
    lf.months_since_prev_order,
    lf.return_rate,
    d.recency_days,
    d.total_orders,
    d.total_revenue,
    d.return_count,
    NULL::FLOAT                                AS churn_probability,  -- filled by inference
    NULL::VARCHAR                              AS churn_risk_tier     -- filled by inference
FROM latest_features AS lf
LEFT JOIN {{ ref('dim_customers') }} AS d
    ON lf.customer_key = d.customer_key
WHERE lf.rn = 1
  AND lf.month < DATE_TRUNC('month', CURRENT_DATE)  -- exclude current partial month
```

### 2.3 Model Training (Python — Airflow DAG)

**When to train:** Weekly, triggered by Airflow after the dbt DAG completes.

`scripts/ml/train_churn_model.py`:

```python
"""
Train a churn prediction model using features from dbt's int_churn_features.
Outputs: model pickle + metrics to DuckDB model registry table.
"""
import argparse
import json
import pickle
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             roc_auc_score, f1_score, classification_report)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def load_features(duckdb_path: str) -> pd.DataFrame:
    """Load churn features from DuckDB."""
    con = duckdb.connect(duckdb_path, read_only=True)
    df = con.execute("""
        SELECT
            customer_key,
            month,
            orders_in_month,
            quantity_in_month,
            gross_revenue_in_month,
            returns_in_month,
            products_in_month,
            countries_in_month,
            orders_prev_3m,
            avg_revenue_prev_3m,
            revenue_change_mom,
            months_since_prev_order,
            return_rate,
            is_churned
        FROM analytics.intermediate.int_churn_features
        WHERE is_churned IN (0, 1)  -- exclude unknown (-1)
    """).fetchdf()
    con.close()
    return df


def train(
    df: pd.DataFrame,
    model_path: str,
    scaler_path: str,
    metrics_path: str,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    """
    Train a GradientBoostingClassifier on churn features.
    Saves model, scaler, and metrics to disk.
    """
    # Feature columns (exclude identifiers and target)
    feature_cols = [
        "orders_in_month", "quantity_in_month", "gross_revenue_in_month",
        "returns_in_month", "products_in_month", "countries_in_month",
        "orders_prev_3m", "avg_revenue_prev_3m", "revenue_change_mom",
        "months_since_prev_order", "return_rate",
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_churned"].values

    # Split (temporal — use last 20% of time as test)
    df_sorted = df.sort_values("month")
    split_idx = int(len(df_sorted) * (1 - test_size))
    train_idx = df_sorted.index[:split_idx]
    test_idx = df_sorted.index[split_idx:]

    X_train, X_test = X.loc[train_idx], X.loc[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    # Scale
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train
    model = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        random_state=random_state,
    )
    model.fit(X_train_scaled, y_train)

    # Evaluate
    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    metrics = {
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "f1": round(f1_score(y_test, y_pred), 4),
        "roc_auc": round(roc_auc_score(y_test, y_proba), 4),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "churn_rate_train": float(y_train.mean()),
        "churn_rate_test": float(y_test.mean()),
        "feature_importance": dict(
            zip(feature_cols, model.feature_importances_.round(4).tolist())
        ),
    }

    # Save
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Model saved to {model_path}")
    print(f"Metrics:\n{classification_report(y_test, y_pred)}")
    return metrics


def register_model_in_duckdb(
    duckdb_path: str,
    model_name: str,
    model_path: str,
    scaler_path: str,
    metrics_path: str,
) -> None:
    """Store model metadata in DuckDB model registry table."""
    con = duckdb.connect(duckdb_path)

    # Create registry table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.marts.model_registry (
            model_name    VARCHAR,
            model_version VARCHAR,
            trained_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_type    VARCHAR,
            metrics       JSON,
            model_path    VARCHAR,
            scaler_path   VARCHAR,
            is_active     BOOLEAN DEFAULT TRUE
        )
    """)

    # Deactivate previous versions
    con.execute(
        "UPDATE analytics.marts.model_registry "
        "SET is_active = FALSE WHERE model_name = ?",
        [model_name],
    )

    with open(metrics_path) as f:
        metrics_json = f.read()

    # Insert new version
    con.execute(
        """
        INSERT INTO analytics.marts.model_registry
            (model_name, model_version, model_type, metrics, model_path, scaler_path)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            model_name,
            pd.Timestamp.now().strftime("%Y%m%d_%H%M%S"),
            "GradientBoostingClassifier",
            metrics_json,
            str(model_path),
            str(scaler_path),
        ],
    )
    con.commit()
    con.close()
    print(f"Model '{model_name}' registered in DuckDB.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="dbt_project/dev.duckdb")
    parser.add_argument("--model-name", default="churn_prediction_v1")
    parser.add_argument("--model-dir", default="models/ml")
    args = parser.parse_args()

    print("Loading features...")
    df = load_features(args.duckdb_path)
    print(f"Loaded {len(df)} labeled samples.")

    model_path = f"{args.model_dir}/{args.model_name}_model.pkl"
    scaler_path = f"{args.model_dir}/{args.model_name}_scaler.pkl"
    metrics_path = f"{args.model_dir}/{args.model_name}_metrics.json"

    print("Training model...")
    metrics = train(df, model_path, scaler_path, metrics_path)

    print("Registering model in DuckDB...")
    register_model_in_duckdb(
        args.duckdb_path, args.model_name, model_path, scaler_path, metrics_path
    )
```

### 2.4 Inference Approaches

#### Option A: Python Inference via dbt Python Model (Recommended for DuckDB)

dbt-duckdb supports Python models. Create a `.py` model that loads the pickled model and scores customers:

`dbt_project/models/marts/inference_churn_probability.py`:

```python
"""
dbt Python model: batch inference of churn probabilities.
Loads the latest trained model from the registry and scores all current customers.
"""
import pickle
import duckdb
import pandas as pd


def model(dbt, session):
    # Read inference features from the rpt_churn_scores table
    # dbt.ref() returns a DuckDB Relation (not a DataFrame)
    features_rel = dbt.ref("rpt_churn_scores")

    # Convert to pandas for sklearn inference
    # (dbt-duckdb gives us a DuckDBPyConnection as `session`)
    features_df = features_rel.df()  # or session.execute("SELECT ...").fetchdf()

    feature_cols = [
        "orders_in_month", "gross_revenue_in_month", "orders_prev_3m",
        "avg_revenue_prev_3m", "revenue_change_mom", "months_since_prev_order",
        "return_rate", "recency_days", "total_orders", "return_count",
    ]

    # Load the active model from registry
    model_info = session.execute("""
        SELECT model_path, scaler_path
        FROM analytics.marts.model_registry
        WHERE model_name = 'churn_prediction_v1' AND is_active = TRUE
        ORDER BY trained_at DESC
        LIMIT 1
    """).fetchone()

    if model_info is None:
        # No model found — return null scores
        result_df = features_df[["customer_key"]].copy()
        result_df["churn_probability"] = None
        result_df["churn_risk_tier"] = "unknown"
        return result_df

    model_path, scaler_path = model_info

    with open(model_path, "rb") as f:
        model = pickle.load(f)
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    X = features_df[feature_cols].fillna(0)
    X_scaled = scaler.transform(X)

    probabilities = model.predict_proba(X_scaled)[:, 1]

    result_df = features_df[["customer_key"]].copy()
    result_df["churn_probability"] = probabilities.round(4)
    result_df["churn_risk_tier"] = pd.cut(
        probabilities,
        bins=[-0.001, 0.2, 0.5, 0.8, 1.001],
        labels=["low", "medium", "high", "critical"],
    )

    return result_df
```

#### Option B: SQL Inference via scikit-sqlearn (Zero-Dependency Inference)

The `scikit-sqlearn` library (v0.1.0+) converts trained sklearn models into raw SQL. This means **no Python runtime needed for inference**.

```bash
pip install scikit-sqlearn
```

```python
from sqlearn.tree_model import RandomForestClassifierConverter
from sklearn.ensemble import RandomForestClassifier

# Train as usual
model = RandomForestClassifier(n_estimators=100, max_depth=4)
model.fit(X_train, y_train)

# Convert to DuckDB SQL
converter = RandomForestClassifierConverter(model)
sql = converter.to_sql(
    feature_names=["orders_prev_3m", "avg_revenue_prev_3m", "recency_days", ...],
    table_name="rpt_churn_scores",
)
print(sql)
# Output: CASE WHEN ... THEN ... ELSE ... END  — pure SQL, runs in any DuckDB query
```

**Pros:** No pickle, no Python UDF, no serialization, runs in the database engine itself  
**Cons:** Only supports `RandomForestClassifier`, `DecisionTreeClassifier`, `LinearRegression`, and pipelines with `StandardScaler` + `OneHotEncoder`

#### Option C: DuckDB infera Extension (ONNX Inference)

The `infera` DuckDB community extension loads ONNX models and runs inference directly in SQL:

```sql
INSTALL infera FROM community;
LOAD infera;

SELECT infera_load_model('churn_model', '/models/churn.onnx');
SELECT customer_key, infera_predict('churn_model', feature1, feature2, ...) AS prob
FROM rpt_churn_scores;
```

**Pros:** Blazing fast (Rust-based), works with any ONNX model (XGBoost, PyTorch, sklearn), thread-safe  
**Cons:** Requires converting models to ONNX format, extension must be installed on all DuckDB instances (dev, CI, prod), not yet available on Snowflake

**Recommendation:**
- **Dev (DuckDB):** Use Option A (Python model) — easiest to debug
- **Prod (Snowflake):** Use Snowpark Python UDFs or Option B (SQL-generation) if using RandomForest
- **If you have many models:** Use Option C (infera + ONNX) — most performant

### 2.5 Airflow Orchestration

Add to your Airflow DAG (alongside the existing Cosmos-based dbt DAG):

```python
from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime

@dag(
    schedule="0 6 * * 1",  # Every Monday at 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "model_name": Param(
            default="churn_prediction_v1",
            type="string",
            description="Model name registered in model_registry",
        ),
    },
)
def ml_training_pipeline():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    # Step 1: Ensure dbt features are built
    dbt_feature_build = BashOperator(
        task_id="build_churn_features",
        bash_command=(
            "cd dbt_project && dbt run --select int_churn_features+ "
            "--profiles-dir ."
        ),
    )

    # Step 2: Train model
    train_model = BashOperator(
        task_id="train_churn_model",
        bash_command=(
            "python scripts/ml/train_churn_model.py "
            "--duckdb-path dbt_project/dev.duckdb "
            "--model-name {{ params.model_name }}"
        ),
    )

    # Step 3: Score all customers via dbt Python model
    dbt_inference = BashOperator(
        task_id="run_churn_inference",
        bash_command=(
            "cd dbt_project && dbt run --select inference_churn_probability "
            "--profiles-dir ."
        ),
    )

    # Step 4: Log model metrics to a report table
    log_metrics = BashOperator(
        task_id="log_model_metrics_to_dashboard",
        bash_command=(
            "python scripts/ml/log_model_performance.py "
            "--duckdb-path dbt_project/dev.duckdb "
            "--model-name {{ params.model_name }}"
        ),
    )

    dbt_feature_build >> train_model >> dbt_inference >> log_metrics


ml_training_pipeline()
```

### 2.6 CI/CD for ML

Add to `.github/workflows/ci.yml`:

```yaml
      - name: Validate ML features
        run: |
          cd dbt_project && dbt test --select int_churn_features --profiles-dir .

      - name: Quick model sanity check (on CI sample)
        run: |
          python scripts/ml/quick_sanity_check.py \
            --duckdb-path dbt_project/ci.duckdb \
            --min-roc-auc 0.65
```

`scripts/ml/quick_sanity_check.py` — trains on CI sample data, asserts minimum AUC:

```python
import sys, duckdb, json
from train_churn_model import load_features, train

df = load_features("dbt_project/ci.duckdb")
if len(df) < 100:
    print(f"Too few samples ({len(df)}) — skipping sanity check.")
    sys.exit(0)

metrics = train(df, "/tmp/ci_model.pkl", "/tmp/ci_scaler.pkl", "/tmp/ci_metrics.json")
print(json.dumps(metrics, indent=2))
assert metrics["roc_auc"] >= 0.65, f"ROC-AUC too low: {metrics['roc_auc']}"
print("Sanity check passed.")
```

### 2.7 Required Python Dependencies

Add to `requirements-dev.in`:

```
scikit-learn>=1.8.0
pandas>=2.2.0
numpy>=1.26.0
joblib>=1.3.0
```

Add to `airflow/requirements.in`:

```
scikit-learn>=1.8.0
pandas>=2.2.0
scikit-sqlearn>=0.1.0  # optional — for SQL-based inference
```

---

## 3. Forecast Integration

### 3.1 Use Cases in This Project

| Forecast | Source | Horizon | Value |
|----------|--------|---------|-------|
| Daily revenue | `rpt_daily_metrics.gross_revenue` | 30 days | Cash flow planning |
| Product demand | `int_product_metrics.total_quantity_sold` by month | 3 months | Inventory planning |
| Customer count | `rpt_daily_metrics.active_customers` | 30 days | Capacity planning |
| Return rate | `rpt_daily_metrics.return_item_count` | 30 days | Operations |

### 3.2 Approach A: StatsForecast (Recommended — 500x faster than Prophet)

[Nixtla StatsForecast](https://github.com/Nixtla/statsforecast) v2.0.3+ is the fastest statistical forecasting library for Python. It runs AutoARIMA, AutoETS, Theta, and MSTL.

`scripts/ml/forecast_revenue.py`:

```python
"""
Generate revenue forecasts using StatsForecast.
Output: forecast table written to DuckDB for BI consumption.
"""
import argparse
from datetime import datetime, timedelta

import duckdb
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA, AutoETS, MSTL


def load_daily_revenue(duckdb_path: str) -> pd.DataFrame:
    con = duckdb.connect(duckdb_path, read_only=True)
    df = con.execute("""
        SELECT
            date_day AS ds,
            gross_revenue AS y,
            'revenue' AS unique_id
        FROM analytics.marts.rpt_daily_metrics
        WHERE date_day < CURRENT_DATE
        ORDER BY date_day
    """).fetchdf()
    con.close()
    return df


def forecast(df: pd.DataFrame, horizon: int = 30) -> pd.DataFrame:
    """Fit models and forecast. Returns forecasts with prediction intervals."""
    sf = StatsForecast(
        models=[
            AutoARIMA(season_length=7),       # weekly seasonality
            AutoETS(season_length=7),
            MSTL(season_length=7),
        ],
        freq="D",
        n_jobs=4,
    )
    sf.fit(df)
    forecasts = sf.predict(h=horizon, level=[80, 95])
    return forecasts


def write_forecast_to_duckdb(duckdb_path: str, forecasts: pd.DataFrame) -> None:
    """Write forecast results to a table in DuckDB."""
    con = duckdb.connect(duckdb_path)

    # Create forecast table
    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics.marts.forecasts (
            forecast_id    INTEGER PRIMARY KEY,
            model_name     VARCHAR,
            unique_id      VARCHAR,
            ds             DATE,
            yhat           FLOAT,
            yhat_lower_80  FLOAT,
            yhat_upper_80  FLOAT,
            yhat_lower_95  FLOAT,
            yhat_upper_95  FLOAT,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Stack all models (AutoARIMA, AutoETS, MSTL columns -> rows)
    model_cols = [c for c in forecasts.columns if c != "ds" and c != "unique_id"]

    rows = []
    for model_name in ["AutoARIMA", "AutoETS", "MSTL"]:
        prefix = f"{model_name}"
        prefix_len = len(prefix) + 1  # +1 for the slash separator
        model_cols_for = [c for c in model_cols if c.startswith(prefix)]

        for _, row in forecasts.iterrows():
            rows.append({
                "model_name": model_name,
                "unique_id": row.get("unique_id", "revenue"),
                "ds": row["ds"],
                "yhat": row.get(f"{prefix}/yhat", 0),
                "yhat_lower_80": row.get(f"{prefix}/yhat_lower_80", 0),
                "yhat_upper_80": row.get(f"{prefix}/yhat_upper_80", 0),
                "yhat_lower_95": row.get(f"{prefix}/yhat_lower_95", 0),
                "yhat_upper_95": row.get(f"{prefix}/yhat_upper_95", 0),
            })

    result_df = pd.DataFrame(rows)
    con.execute("DELETE FROM analytics.marts.forecasts WHERE unique_id = 'revenue'")
    con.execute("INSERT INTO analytics.marts.forecasts SELECT nextval('forecast_id'), * FROM result_df")
    con.commit()
    con.close()
    print(f"Wrote {len(result_df)} forecast rows to DuckDB.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="dbt_project/dev.duckdb")
    parser.add_argument("--horizon", type=int, default=30)
    args = parser.parse_args()

    print("Loading revenue data...")
    df = load_daily_revenue(args.duckdb_path)
    print(f"Loaded {len(df)} days of revenue data.")

    print(f"Forecasting {args.horizon} days ahead...")
    forecasts = forecast(df, horizon=args.horizon)

    print("Writing to DuckDB...")
    write_forecast_to_duckdb(args.duckdb_path, forecasts)

    # Print best model summary
    print("\nForecast complete. Sample:")
    print(forecasts.head())
```

### 3.3 Approach B: Prophet (When You Need Holiday Effects)

If your retail data has holiday effects (Christmas, Black Friday), Prophet handles them natively:

```python
from prophet import Prophet

df = load_daily_revenue("dbt_project/dev.duckdb")

model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    changepoint_prior_scale=0.05,
)
model.fit(df.rename(columns={"ds": "ds", "y": "y"}))

future = model.make_future_dataframe(periods=30)
forecast = model.predict(future)

# Write to DuckDB
con = duckdb.connect("dbt_project/dev.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS analytics.marts.prophet_forecast AS SELECT * FROM forecast")
```

**StatsForecast vs. Prophet comparison:**

| Factor | StatsForecast | Prophet |
|--------|--------------|---------|
| Speed | 500x faster | Baseline |
| Holiday effects | Manual via regressors | Native |
| Multiple seasonality | Yes (MSTL) | Yes (yearly+weekly+ daily) |
| Confidence intervals | Native | Native |
| Multiple time series | Native (unique_id) | Per-series loop |
| Changepoint detection | Via MSTL | Native |
| Python deps | Light (numpy, pandas) | Heavy (pystan/prophet) |
| Best for | Scale, speed, production | Interpretability, holidays |

**Recommendation:** Use StatsForecast for daily revenue and product demand (faster, lighter). Add Prophet later if holiday effects prove significant.

### 3.4 dbt Integration for Forecast Consumption

Create a dbt model that joins forecasts to actuals for BI:

`dbt_project/models/marts/rpt_forecast_vs_actual.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

WITH actuals AS (
    SELECT
        date_day,
        gross_revenue,
        order_item_count,
        active_customers
    FROM {{ ref('rpt_daily_metrics') }}
    WHERE date_day >= CURRENT_DATE - INTERVAL '90 days'
),

forecasts AS (
    SELECT
        ds              AS date_day,
        yhat            AS forecasted_revenue,
        yhat_lower_95   AS forecast_lower,
        yhat_upper_95   AS forecast_upper
    FROM {{ source('ml', 'forecasts') }}
    WHERE model_name = 'AutoARIMA'
      AND unique_id = 'revenue'
)

SELECT
    COALESCE(a.date_day, f.date_day) AS date_day,
    a.gross_revenue          AS actual_revenue,
    f.forecasted_revenue,
    f.forecast_lower,
    f.forecast_upper,
    CASE
        WHEN a.gross_revenue IS NOT NULL AND f.forecasted_revenue IS NOT NULL
            THEN ROUND((a.gross_revenue - f.forecasted_revenue) / NULLIF(f.forecasted_revenue, 0) * 100, 2)
        ELSE NULL
    END AS pct_deviation,
    CASE WHEN a.date_day >= CURRENT_DATE THEN 'forecast' ELSE 'actual' END AS data_type
FROM actuals a
FULL OUTER JOIN forecasts f USING (date_day)
ORDER BY date_day DESC
```

### 3.5 Product-Level Demand Forecasting

For inventory planning, forecast at the product category level:

`scripts/ml/forecast_product_demand.py`:

```python
# Extends the revenue forecast approach for multiple time series
import duckdb
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA

con = duckdb.connect("dbt_project/dev.duckdb")

# Monthly product quantity by product (top 50 by volume)
df = con.execute("""
    SELECT
        p.stock_code || ' - ' || p.description AS unique_id,
        DATE_TRUNC('month', f.invoice_date) AS ds,
        SUM(CASE WHEN NOT f.is_return THEN f.quantity ELSE 0 END) AS y
    FROM {{ source('analytics', 'fct_orders') }} AS f
    JOIN {{ source('analytics', 'dim_products_snapshot') }} AS p
        ON f.product_key = p.product_key AND p.dbt_valid_to IS NULL
    GROUP BY p.stock_code, p.description, DATE_TRUNC('month', f.invoice_date)
    HAVING SUM(CASE WHEN NOT f.is_return THEN f.quantity ELSE 0 END) > 100
""").fetchdf()

sf = StatsForecast(
    models=[AutoARIMA(season_length=12)],  # yearly seasonality
    freq="ME",
    n_jobs=4,
)
sf.fit(df)
forecasts = sf.predict(h=3, level=[80])  # 3-month horizon

# Write back to DuckDB
con.execute("CREATE TABLE IF NOT EXISTS analytics.marts.product_demand_forecast AS SELECT * FROM forecasts")
```

### 3.6 Required Dependencies

```
statsforecast>=2.0.0
prophet>=1.3.0  # optional — only if holiday effects matter
```

---

## 4. LLM-Enhanced Data Catalog

### 4.1 Approach A: SchemaScribe (Recommended for dbt Projects)

[SchemaScribe](https://github.com/dongwonmoon/SchemaScribe) is purpose-built for dbt projects. It scans `manifest.json` and uses an LLM to generate missing descriptions.

**Setup:**

```bash
pip install schema-scribe
```

**Configuration** (`schema-scribe-config.yaml`):

```yaml
llm:
  provider: openai
  model: gpt-4o-mini  # cheap, fast, good enough for descriptions
  temperature: 0.1    # low temperature for deterministic output

dbt:
  project_dir: dbt_project
  target: dev
```

**Usage:**

```bash
# Generate descriptions for all undocumented models/columns
schema-scribe dbt --project-dir dbt_project --update

# Check in CI (fails if documentation is missing)
schema-scribe dbt --project-dir dbt_project --check

# Interactive mode (human-in-the-loop)
schema-scribe dbt --project-dir dbt_project --interactive
```

**Your current coverage (`schema.yml`):** Every column has a `description` — already well-documented. SchemaScribe can:
1. Extend existing descriptions with business context
2. Generate test suggestions based on column names and data types
3. Flag potential PII columns automatically

**PII Detection Example Output:**

After running SchemaScribe, it would generate:

```yaml
      - name: customer_id
        description: >
          Customer identifier from source system. ⚠️ POTENTIAL PII:
          This is a unique identifier that can be linked to individual customers.
          Ensure this column is excluded from any public-facing datasets.
        tags: [pii, customer_identifier]
```

### 4.2 Approach B: Custom dbt + LLM Pipeline (More Control)

For deeper integration, build a script that reads `manifest.json` and calls an LLM directly:

`scripts/catalog/generate_descriptions.py`:

```python
"""
Read dbt manifest.json, identify undocumented columns,
and generate descriptions using an LLM.
"""
import json
import os
from pathlib import Path

import yaml


def load_manifest(project_dir: str) -> dict:
    manifest_path = Path(project_dir) / "target" / "manifest.json"
    with open(manifest_path) as f:
        return json.load(f)


def get_undocumented_nodes(manifest: dict) -> list[dict]:
    """Find models with missing or minimal column descriptions."""
    nodes = []
    for node_name, node in manifest["nodes"].items():
        if node["resource_type"] != "model":
            continue
        if not node.get("columns"):
            continue

        missing = []
        for col_name, col_info in node["columns"].items():
            desc = col_info.get("description", "")
            if not desc or len(desc) < 15:
                missing.append(col_name)

        if missing:
            nodes.append({
                "name": node_name,
                "relation_name": node["relation_name"],
                "database": node["database"],
                "schema_": node["schema"],
                "missing_columns": missing,
                "original_description": node.get("description", ""),
            })
    return nodes


def generate_descriptions_llm(nodes: list[dict]) -> list[dict]:
    """
    Call an LLM to generate descriptions.
    Uses OpenAI API — swap the client for Anthropic/Ollama.
    """
    from openai import OpenAI

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    results = []
    for node in nodes:
        prompt = f"""You are a data catalog assistant for a retail analytics pipeline.

Table: {node['relation_name']}
Schema: {node['schema_']}
Table description: {node['original_description']}

The following columns are missing descriptions:
{chr(10).join(f'- {c}' for c in node['missing_columns'])}

Generate a concise, business-friendly description for each column.
Use the existing table context. Keep descriptions under 100 chars.
Return as a JSON dict {{"column_name": "description"}}.
"""

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,
        )

        suggestions = json.loads(resp.choices[0].message.content)
        results.append({
            "node_name": node["name"],
            "suggestions": suggestions,
        })
        print(f"Generated {len(suggestions)} descriptions for {node['name']}")

    return results


def update_schema_yml(project_dir: str, results: list[dict]) -> None:
    """Merge AI-generated descriptions back into schema.yml files."""
    # This is a simplified version — real implementation would use
    # ruamel.yaml to preserve comments and formatting
    for result in results:
        schema_path = Path(project_dir) / "models" / "schema.yml"
        with open(schema_path) as f:
            schema = yaml.safe_load(f)

        for model_entry in schema.get("models", []):
            if model_entry["name"] in result["node_name"]:
                for col in model_entry.get("columns", []):
                    if col["name"] in result["suggestions"]:
                        col["description"] = result["suggestions"][col["name"]]

        with open(schema_path, "w") as f:
            yaml.dump(schema, f, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    project_dir = "dbt_project"
    manifest = load_manifest(project_dir)
    nodes = get_undocumented_nodes(manifest)
    if not nodes:
        print("All columns are documented. ✓")
    else:
        results = generate_descriptions_llm(nodes)
        update_schema_yml(project_dir, results)
        print(f"Updated {len(results)} schema entries.")
```

### 4.3 Generating Column Test Suggestions

LLMs can infer appropriate dbt tests from column names, types, and existing descriptions:

```python
def suggest_tests(node_info: dict) -> list[dict]:
    prompt = f"""For this analytics model table, suggest dbt generic tests:

Table: {node_info['relation_name']}
Columns: {json.dumps(node_info['columns'], indent=2)}

For each column, suggest 1-3 appropriate dbt tests from:
- not_null, unique, accepted_values, positive_value
- relationships (foreign key check)
- dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs
- dbt_expectations.expect_column_mean_to_be_between

Return as JSON: {{"column_name": ["test1", "test2"]}}
"""
    # ... call LLM, parse response
```

### 4.4 CI/CD: Auto-Update Docs on Every PR

Add to `.github/workflows/ci.yml`:

```yaml
      - name: Check documentation completeness
        run: |
          pip install schema-scribe
          schema-scribe dbt --project-dir dbt_project --check || \
            echo "::warning::Some columns are undocumented — run 'schema-scribe dbt --update' locally"

      - name: Generate dbt docs
        run: |
          cd dbt_project && dbt docs generate --profiles-dir .
          cp target/index.html docs/catalog.html  # serve as CI artifact
```

### 4.5 Required Dependencies

```
schema-scribe>=0.1.0
openai>=1.0.0  # if using custom LLM pipeline
```

---

## 5. Cross-Cutting Recommendations

### 5.1 File & Directory Structure

After implementing all four areas:

```
dbt_project/
├── models/
│   ├── marts/
│   │   ├── rpt_daily_metrics.sql             # NEW: daily aggregates for anomaly detection
│   │   ├── rpt_daily_metrics_by_country.sql   # NEW: per-country daily metrics
│   │   ├── rpt_churn_scores.sql               # NEW: churn inference features
│   │   ├── rpt_forecast_vs_actual.sql         # NEW: forecast vs actual comparison
│   │   ├── inference_churn_probability.py     # NEW: dbt Python model for inference
│   │   └── ... (existing fct_orders, dims)
│   └── intermediate/
│       └── int_churn_features.sql             # NEW: churn feature engineering
├── tests/
│   └── assert_daily_row_count_within_3sigma.sql  # NEW: row-count drift test
scripts/
├── ml/
│   ├── train_churn_model.py                  # NEW: sklearn training pipeline
│   ├── forecast_revenue.py                   # NEW: StatsForecast training
│   ├── forecast_product_demand.py            # NEW: multi-series product forecast
│   ├── log_model_performance.py              # NEW: model metrics tracking
│   └── quick_sanity_check.py                 # NEW: CI validation
└── catalog/
    └── generate_descriptions.py              # NEW: LLM catalog generator
```

### 5.2 Model Registry Table Schema

```sql
CREATE TABLE analytics.marts.model_registry (
    model_name    VARCHAR,
    model_version VARCHAR,
    trained_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_type    VARCHAR,              -- e.g., GradientBoostingClassifier
    feature_names JSON,                 -- list of feature columns used
    metrics       JSON,                 -- accuracy, precision, recall, f1, roc_auc
    model_path    VARCHAR,              -- filesystem path to .pkl
    scaler_path   VARCHAR,              -- filesystem path to scaler .pkl
    training_rows INTEGER,              -- number of training samples
    is_active     BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (model_name, model_version)
);
```

### 5.3 Environment Variable Strategy

Add to `.env.example`:

```bash
# ML
MODEL_DIR=/path/to/models
MLFLOW_TRACKING_URI=  # optional, for experiment tracking

# LLM Catalog
OPENAI_API_KEY=sk-...
SCHEMA_SCRIBE_LLM_PROVIDER=openai
SCHEMA_SCRIBE_LLM_MODEL=gpt-4o-mini
```

### 5.4 Performance Considerations

| Component | Data Volume Concern | Mitigation |
|-----------|-------------------|------------|
| dbt-expectations moving stdevs | Full table scan on every test run | Materialize daily aggregate first, then test that |
| Churn feature engineering | Window functions over 1M rows × 5k customers | Already a `table` materialization; add `cluster_by: month` |
| Python model inference | Loading 5k rows to pandas is fast | Use batch UDF if >100k rows |
| StatsForecast | Fitting multiple models on 3 years of daily data | <2 seconds for 3 models |
| LLM catalog generation | ~50-100 columns to describe at ~$0.01/run | Use gpt-4o-mini, cost ~$0.15 for full refresh |

### 5.5 Alerting Strategy

**Tier 1 — Page (PagerDuty/Slack urgent):**
- `dbt test` failure on `fct_orders` anomaly with `severity: error`
- Row count drops >3σ on consecutive days
- `model_registry` training fails

**Tier 2 — Digest (daily Slack summary):**
- `severity: warn` anomaly flags
- Churn probability increase for any `high_value` segment customer
- Forecast accuracy degradation (>20% MAPE)

**Tier 3 — Dashboard:**
- Anomaly history trends
- Model performance over time (AUC drift)
- Forecast vs. actual comparison chart

---

## 6. Implementation Roadmap

### Phase 1: Anomaly Detection (1-2 days)

1. Create `rpt_daily_metrics.sql` model
2. Add `dbt-expectations` moving-stdev tests to `schema.yml`
3. Add singular `assert_daily_row_count_within_3sigma.sql` test
4. Tag new tests with `anomaly`
5. Run on CI for one week with `severity: warn`
6. Adjust sigma thresholds if needed

### Phase 2: Churn Prediction (3-5 days)

1. Create `int_churn_features.sql` with window functions
2. Write `train_churn_model.py` training script
3. Test locally: `python scripts/ml/train_churn_model.py`
4. Create `inference_churn_probability.py` dbt Python model
5. Add Airflow DAG tasks for training pipeline
6. Add CI sanity check

### Phase 3: Forecasting (2-3 days)

1. Write `forecast_revenue.py` using StatsForecast
2. Create `rpt_forecast_vs_actual.sql` dbt model
3. Schedule weekly forecast via Airflow
4. (Optional) Add product-level demand forecasting

### Phase 4: LLM Catalog (1 day)

1. Run `schema-scribe dbt --interactive` to review AI suggestions
2. Add CI check `schema-scribe dbt --check`
3. (Optional) Build custom pipeline for PII tagging

---

## Summary Table

| Capability | Effort | Impact | New Dependencies | Risk |
|-----------|--------|--------|-----------------|------|
| Anomaly detection | Low (1-2d) | High — catches pipeline failures | None (dbt-expectations already installed) | Low |
| Churn prediction | Med (3-5d) | High — business value, ML foundation | scikit-learn, pandas | Med — model decay |
| Forecasting | Med (2-3d) | Medium — ops planning | statsforecast | Low — stat models are robust |
| LLM catalog | Low (1d) | Medium — documentation quality | schema-scribe / openai | Low |

**Start with anomaly detection** — zero new dependencies, immediate value, and it gives you the monitoring infrastructure for everything else.
