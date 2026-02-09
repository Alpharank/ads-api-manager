#!/usr/bin/env python3
"""
Export enriched campaign data and LTV data from AWS Athena.

Queries staging.google_ads_campaign_data (populated by auto-attribution pipeline)
to get campaign-level metrics with attribution data (apps, funded, value).

Usage:
    python scripts/export_athena_data.py                  # Export current month
    python scripts/export_athena_data.py 2025-11          # Export specific month
    python scripts/export_athena_data.py --list-clients   # List available client_ids

Output:
  data/{client_id}/enriched/{month}.csv   — campaign metrics + value/production/apps/funded
  data/{client_id}/ltv/{month}.csv        — LTV by source

Note: This exports LAST-TOUCH attribution only (the pipeline's default model).
For first_click/linear attribution, the auto-attribution pipeline would need to
store multiple attribution versions per application.
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
import yaml

try:
    import boto3
except ImportError:
    sys.exit("boto3 is required. Install with: pip install boto3")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# ── Configuration ──
# Based on auto-attribution repo: staging tables are in 'staging' database
# Workgroup and output bucket may need adjustment based on your IAM permissions
ATHENA_DATABASE = "staging"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_BUCKET = "s3://etl.alpharank.airflow/athena-results/"
AWS_REGION = "us-east-1"

# NOTE: This script exports LAST-TOUCH attribution only (the default model).
# The auto-attribution pipeline stores applications with a single attribution model.
# To get first_click/linear attribution, the pipeline would need to recompute
# customer journeys under each model and store separate attribution outputs.

CLIENTS_CONFIG = PROJECT_ROOT / "config" / "clients.yaml"


def load_athena_clients() -> dict:
    """Load clients config, returning {athena_id: client_id} mapping."""
    with open(CLIENTS_CONFIG, 'r') as f:
        clients = yaml.safe_load(f)
    return {v['athena_id']: k for k, v in clients.items()}

# Campaign-level query with value/production/apps/funded
# Mirrors the Athena query from staging.google_ads_campaign_data
CAMPAIGN_QUERY = """
WITH campaign_level AS (
  SELECT
    campaign_id,
    campaign,
    sum(clicks)        AS clicks,
    sum(cost)          AS cost,
    sum(apps)          AS apps,
    sum(approved)      AS approved,
    sum(funded)        AS funded,
    sum(production)    AS production,
    sum("value")       AS "value"
  FROM staging.google_ads_campaign_data
  WHERE day BETWEEN date ? AND date ?
    AND client_id = ?
  GROUP BY campaign_id, campaign
)
SELECT
  campaign_id,
  campaign AS campaign_name,
  COALESCE(clicks, 0) AS clicks,
  ROUND(COALESCE(cost, 0), 2) AS cost,
  COALESCE(apps, 0) AS apps,
  COALESCE(approved, 0) AS approved,
  COALESCE(funded, 0) AS funded,
  ROUND(COALESCE(production, 0), 2) AS production,
  ROUND(COALESCE("value", 0), 2) AS value,
  CASE WHEN cost > 0 THEN ROUND((("value" / cost) - 1), 2) ELSE NULL END AS roas,
  CASE WHEN funded > 0 THEN ROUND(cost / funded, 2) ELSE NULL END AS cpf,
  CASE WHEN funded > 0 THEN ROUND("value" / funded, 2) ELSE NULL END AS avg_funded_value
FROM campaign_level
ORDER BY cost DESC
"""

# LTV by source query
LTV_QUERY = """
SELECT
    source,
    COUNT(*) AS accounts,
    SUM(lifetime_value) AS total_ltv,
    AVG(lifetime_value) AS avg_ltv
FROM digital_lending_value_by_source
WHERE client_id = ?
  AND month = ?
GROUP BY source
ORDER BY total_ltv DESC
"""


ATHENA_QUERY_TIMEOUT_SECONDS = 300  # 5 minutes


def run_athena_query(client: "boto3.client", query: str, params: list = None) -> str:
    """Submit an Athena query and wait for completion. Returns query execution ID."""
    kwargs = dict(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_BUCKET},
    )
    if params:
        kwargs["ExecutionParameters"] = params

    response = client.start_query_execution(**kwargs)
    execution_id = response["QueryExecutionId"]

    start_time = time.monotonic()
    while True:
        elapsed = time.monotonic() - start_time
        if elapsed > ATHENA_QUERY_TIMEOUT_SECONDS:
            client.stop_query_execution(QueryExecutionId=execution_id)
            raise RuntimeError(
                f"Athena query timed out after {ATHENA_QUERY_TIMEOUT_SECONDS}s "
                f"(execution_id={execution_id})"
            )
        result = client.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            return execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)


def fetch_results(client: "boto3.client", execution_id: str) -> pd.DataFrame:
    """Fetch all result rows from a completed Athena query."""
    paginator = client.get_paginator("get_query_results")
    rows = []
    columns = None

    for page in paginator.paginate(QueryExecutionId=execution_id):
        result_set = page["ResultSet"]
        if columns is None:
            columns = [col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]
            # Skip header row in first page
            data_rows = result_set["Rows"][1:]
        else:
            data_rows = result_set["Rows"]

        for row in data_rows:
            rows.append([field.get("VarCharValue", "") for field in row["Data"]])

    return pd.DataFrame(rows, columns=columns)


def export_enriched(athena_id: str, dashboard_id: str, month: str, athena_client: "boto3.client") -> None:
    """Export enriched campaign data for a client/month.

    Args:
        athena_id: The client_id used in the Athena partition (e.g. 'california_coast_cu').
        dashboard_id: The client_id used for dashboard file paths (e.g. 'californiacoast_cu').
    """
    import calendar

    year, mo = month.split("-")
    start_date = f"{year}-{mo}-01"
    last_day = calendar.monthrange(int(year), int(mo))[1]
    end_date = f"{year}-{mo}-{last_day:02d}"

    execution_id = run_athena_query(
        athena_client, CAMPAIGN_QUERY, params=[start_date, end_date, athena_id]
    )
    df = fetch_results(athena_client, execution_id)

    if df.empty:
        print(f"  No enriched data for {athena_id}/{month}")
        return

    for col in ["clicks", "cost", "apps", "approved", "funded", "production", "value", "roas", "cpf", "avg_funded_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out_dir = DATA_DIR / dashboard_id / "enriched"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{month}.csv"
    df.to_csv(out_path, index=False)
    print(f"  enriched/{month}.csv: {len(df)} rows")


def export_ltv(client_id: str, month: str, athena_client: "boto3.client") -> None:
    """Export LTV data for a client/month."""
    execution_id = run_athena_query(
        athena_client, LTV_QUERY, params=[client_id, month]
    )
    df = fetch_results(athena_client, execution_id)

    if df.empty:
        print(f"  No LTV data for {client_id}/{month}")
        return

    for col in ["accounts", "total_ltv", "avg_ltv"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out_dir = DATA_DIR / client_id / "ltv"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{month}.csv"
    df.to_csv(out_path, index=False)
    print(f"  ltv/{month}.csv: {len(df)} rows")


def list_client_ids(athena_client: "boto3.client") -> None:
    """List all available client_ids in the google_ads_campaign_data table."""
    query = """
    SELECT DISTINCT client_id
    FROM staging.google_ads_campaign_data
    ORDER BY client_id
    """
    execution_id = run_athena_query(athena_client, query)
    df = fetch_results(athena_client, execution_id)
    print("Available client_ids in staging.google_ads_campaign_data:")
    for cid in df["client_id"].tolist():
        print(f"  - {cid}")
    print()


def main():
    # Check if we should just list client IDs
    if len(sys.argv) > 1 and sys.argv[1] == "--list-clients":
        athena_client = boto3.client("athena", region_name=AWS_REGION)
        list_client_ids(athena_client)
        return

    athena_client = boto3.client("athena", region_name=AWS_REGION)

    # Default to current month; override with CLI arg
    month = sys.argv[1] if len(sys.argv) > 1 else None
    if not month:
        from datetime import datetime
        month = datetime.utcnow().strftime("%Y-%m")

    athena_clients = load_athena_clients()  # {athena_id: dashboard_client_id}
    for athena_id, dashboard_id in athena_clients.items():
        print(f"Processing {athena_id} (-> {dashboard_id}) for {month}...")
        export_enriched(athena_id, dashboard_id, month, athena_client)
        export_ltv(athena_id, month, athena_client)

    print("\nDone!")


if __name__ == "__main__":
    main()
