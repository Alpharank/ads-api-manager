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

# Client IDs must match the partition values in staging.google_ads_campaign_data
# These are the client_id values from prod.application_data, typically snake_case
CLIENTS = [
    "california_coast_cu",
    "commonwealth_one_fcu",
    "first_community_cu",
    "kitsap_cu",
    "public_service_cu",
]

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
  WHERE day BETWEEN date'{start_date}' AND date'{end_date}'
    AND client_id = '{client_id}'
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
WHERE client_id = '{client_id}'
  AND month = '{month}'
GROUP BY source
ORDER BY total_ltv DESC
"""


def run_athena_query(client: "boto3.client", query: str) -> str:
    """Submit an Athena query and wait for completion. Returns query execution ID."""
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_BUCKET},
    )
    execution_id = response["QueryExecutionId"]

    while True:
        result = client.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            return execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(1)


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


def export_enriched(client_id: str, month: str, athena_client: "boto3.client") -> None:
    """Export enriched campaign data for a client/month."""
    # Calculate date range for the month
    year, mo = month.split("-")
    start_date = f"{year}-{mo}-01"
    # Get last day of month
    import calendar
    last_day = calendar.monthrange(int(year), int(mo))[1]
    end_date = f"{year}-{mo}-{last_day:02d}"

    query = CAMPAIGN_QUERY.format(
        client_id=client_id,
        start_date=start_date,
        end_date=end_date
    )
    execution_id = run_athena_query(athena_client, query)
    df = fetch_results(athena_client, execution_id)

    if df.empty:
        print(f"  No enriched data for {client_id}/{month}")
        return

    # Convert numeric columns
    for col in ["clicks", "cost", "apps", "approved", "funded", "production", "value", "roas", "cpf", "avg_funded_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out_dir = DATA_DIR / client_id / "enriched"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{month}.csv"
    df.to_csv(out_path, index=False)
    print(f"  enriched/{month}.csv: {len(df)} rows")


def export_ltv(client_id: str, month: str, athena_client: "boto3.client") -> None:
    """Export LTV data for a client/month."""
    query = LTV_QUERY.format(client_id=client_id, month=month)
    execution_id = run_athena_query(athena_client, query)
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

    for client_id in CLIENTS:
        print(f"Processing {client_id} for {month}...")
        export_enriched(client_id, month, athena_client)
        export_ltv(client_id, month, athena_client)

    print("\nDone!")


if __name__ == "__main__":
    main()
