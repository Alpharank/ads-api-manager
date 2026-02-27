#!/usr/bin/env python3
"""
Export keyword-level data to Athena as Parquet.

Joins keyword metrics (clicks, cost from Google Ads) with enriched daily
attribution (apps, approved, funded, value from GCLID attribution) and writes
Snappy-compressed Parquet to S3 for the staging.google_ads_keyword_data table.

Usage:
    python scripts/export_keyword_to_athena.py --client kitsap_cu --month 2026-02
    python scripts/export_keyword_to_athena.py --all --month 2026-02
    python scripts/export_keyword_to_athena.py --all           # current + previous month
    python scripts/export_keyword_to_athena.py --dry-run --client kitsap_cu --month 2026-02

Output:
    s3://etl.alpharank.airflow/staging/google_ads_keyword_data/client_id={athena_id}/{month}.snappy.parquet
"""

import argparse
import sys
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

try:
    import boto3
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as e:
    sys.exit(f"Missing dependency: {e}\nInstall with: pip install boto3 pandas pyarrow")

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CLIENTS_CONFIG = PROJECT_ROOT / "config" / "clients.yaml"
AWS_CONFIG = PROJECT_ROOT / "config" / "config.yaml"

S3_BUCKET = "etl.alpharank.airflow"
S3_PREFIX = "staging/google_ads_keyword_data"
ATHENA_DATABASE = "staging"
ATHENA_TABLE = "google_ads_keyword_data"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_BUCKET = "s3://etl.alpharank.airflow/athena-results/"
AWS_REGION = "us-west-2"


def load_clients() -> dict:
    with open(CLIENTS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def load_aws_config() -> dict:
    with open(AWS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def load_keyword_metrics(client_id: str, month: str) -> pd.DataFrame:
    """Load aggregated keyword metrics from local data/{client_id}/keywords/{month}.csv."""
    path = DATA_DIR / client_id / "keywords" / f"{month}.csv"
    if not path.exists():
        print(f"  No keyword metrics file: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    # Standardize column names
    rename = {
        "campaign_id": "campaign_id",
        "campaign_name": "campaign",
        "ad_group_id": "ad_group_id",
        "ad_group_name": "ad_group",
        "keyword": "keyword",
        "match_type": "match_type",
    }
    df = df.rename(columns=rename)

    # Aggregate daily keyword rows to monthly totals per keyword
    # (keyword metrics file has one row per keyword for the whole month already,
    #  but handle daily files too)
    group_cols = ["campaign_id", "campaign", "ad_group_id", "ad_group", "keyword", "match_type"]
    agg_cols = {"clicks": "sum", "cost": "sum"}
    if "impressions" in df.columns:
        agg_cols["impressions"] = "sum"
    if "conversions" in df.columns:
        agg_cols["conversions"] = "sum"

    # Ensure numeric types before aggregation
    for col in agg_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Ensure string types for group columns
    for col in group_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    df = df.groupby(group_cols, as_index=False).agg(agg_cols)
    print(f"  Loaded {len(df)} keyword rows from metrics")
    return df


def load_enriched_attribution(client_id: str, month: str) -> pd.DataFrame:
    """Load enriched daily attribution from data/{client_id}/enriched/daily/{month}_gclid.csv."""
    path = DATA_DIR / client_id / "enriched" / "daily" / f"{month}_gclid.csv"
    if not path.exists():
        print(f"  No enriched daily file: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    # Standardize column names to match keyword metrics
    rename = {
        "campaign_name": "campaign",
        "ad_group_name": "ad_group",
    }
    df = df.rename(columns=rename)

    # Ensure string types for join columns
    for col in ["campaign_id", "ad_group_id", "keyword", "match_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Aggregate daily rows to monthly per keyword (drop date dimension for Athena)
    group_cols = ["campaign_id", "campaign", "ad_group_id", "ad_group", "keyword", "match_type"]
    agg_cols = {
        "apps": "sum",
        "approved": "sum",
        "funded": "sum",
        "production": "sum",
        "value": "sum",
    }
    # Product family columns may or may not exist
    for col in ["cc_appvd", "deposit_appvd", "personal_appvd", "vehicle_appvd", "heloc_appvd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            agg_cols[col] = "sum"

    for col in ["apps", "approved", "funded", "production", "value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.groupby(group_cols, as_index=False).agg(agg_cols)
    print(f"  Loaded {len(df)} keyword rows from enriched attribution")
    return df


def join_and_build(metrics_df: pd.DataFrame, attribution_df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Outer-join keyword metrics with attribution data."""
    join_cols = ["campaign_id", "campaign", "ad_group_id", "ad_group", "keyword", "match_type"]

    if metrics_df.empty and attribution_df.empty:
        return pd.DataFrame()

    if attribution_df.empty:
        result = metrics_df.copy()
    elif metrics_df.empty:
        result = attribution_df.copy()
    else:
        result = metrics_df.merge(attribution_df, on=join_cols, how="outer")

    # Fill NaN for numeric columns
    for col in ["clicks", "cost", "apps", "approved", "funded", "production", "value",
                 "cc_appvd", "deposit_appvd", "personal_appvd", "vehicle_appvd", "heloc_appvd"]:
        if col not in result.columns:
            result[col] = 0
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

    # Add day column (first of month — represents the month)
    result["day"] = f"{month}-01"

    # Select and order columns to match DDL
    output_cols = [
        "campaign_id", "day", "campaign", "ad_group_id", "ad_group",
        "keyword", "match_type", "clicks", "cost",
        "apps", "approved", "funded", "production", "value",
        "cc_appvd", "deposit_appvd", "personal_appvd", "vehicle_appvd", "heloc_appvd",
    ]
    result = result[output_cols]

    # Cast types for Parquet schema
    result["day"] = pd.to_datetime(result["day"]).dt.date
    for col in ["clicks", "apps", "approved", "funded", "cc_appvd", "deposit_appvd",
                 "personal_appvd", "vehicle_appvd", "heloc_appvd"]:
        result[col] = result[col].astype(int)
    for col in ["cost", "production", "value"]:
        result[col] = result[col].astype(float)

    return result


def write_parquet_to_s3(df: pd.DataFrame, s3_client, athena_id: str, month: str, dry_run: bool = False) -> str:
    """Write DataFrame as Snappy Parquet to S3."""
    s3_key = f"{S3_PREFIX}/client_id={athena_id}/{month}.snappy.parquet"

    if dry_run:
        print(f"  [dry-run] Would write {len(df)} rows to s3://{S3_BUCKET}/{s3_key}")
        return s3_key

    # Define explicit Arrow schema to match Athena DDL
    schema = pa.schema([
        ("campaign_id", pa.string()),
        ("day", pa.date32()),
        ("campaign", pa.string()),
        ("ad_group_id", pa.string()),
        ("ad_group", pa.string()),
        ("keyword", pa.string()),
        ("match_type", pa.string()),
        ("clicks", pa.int64()),
        ("cost", pa.float64()),
        ("apps", pa.int64()),
        ("approved", pa.int64()),
        ("funded", pa.int64()),
        ("production", pa.float64()),
        ("value", pa.float64()),
        ("cc_appvd", pa.int64()),
        ("deposit_appvd", pa.int64()),
        ("personal_appvd", pa.int64()),
        ("vehicle_appvd", pa.int64()),
        ("heloc_appvd", pa.int64()),
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"  Wrote {len(df)} rows to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def repair_partitions(athena_client, dry_run: bool = False):
    """Run MSCK REPAIR TABLE to register any new partitions."""
    query = f"MSCK REPAIR TABLE {ATHENA_DATABASE}.{ATHENA_TABLE}"
    if dry_run:
        print(f"  [dry-run] Would run: {query}")
        return

    import time
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_BUCKET},
    )
    execution_id = response["QueryExecutionId"]
    print(f"  MSCK REPAIR TABLE submitted (execution_id={execution_id})")

    # Wait for completion
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print("  Partition repair completed")
            return
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            print(f"  WARNING: MSCK REPAIR TABLE {state}: {reason}")
            return
        time.sleep(2)


def process_client(client_id: str, client_cfg: dict, month: str,
                   s3_client, dry_run: bool = False):
    """Export keyword data for a single client/month."""
    prod_id = client_cfg.get("prod_id", client_id)

    print(f"\n{'='*60}")
    print(f"  {client_cfg['name']} ({client_id})  →  prod_id={prod_id}  —  {month}")
    print(f"{'='*60}")

    # 1. Load keyword metrics
    metrics_df = load_keyword_metrics(client_id, month)

    # 2. Load enriched attribution
    attribution_df = load_enriched_attribution(client_id, month)

    if metrics_df.empty and attribution_df.empty:
        print("  No data for this month — skipping")
        return

    # 3. Join
    result_df = join_and_build(metrics_df, attribution_df, month)
    if result_df.empty:
        print("  Join produced no rows — skipping")
        return

    print(f"  Joined result: {len(result_df)} rows")

    # 4. Write Parquet to S3 (use prod_id to match campaign_data partitioning)
    write_parquet_to_s3(result_df, s3_client, prod_id, month, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="Export keyword-level data to Athena as Parquet"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--client", help="Process a single client_id (e.g. kitsap_cu)")
    group.add_argument("--all", action="store_true", help="Process all clients")
    parser.add_argument("--month", help="Target month (YYYY-MM). If omitted, processes current + previous month.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without uploading")
    parser.add_argument("--no-repair", action="store_true",
                        help="Skip MSCK REPAIR TABLE after upload")
    args = parser.parse_args()

    clients = load_clients()
    aws_cfg = load_aws_config()

    s3_client = boto3.client("s3", region_name=aws_cfg["aws"]["region"])
    athena_client = boto3.client("athena", region_name=AWS_REGION)

    # Determine months to process
    if args.month:
        months = [args.month]
    else:
        now = datetime.utcnow()
        current_month = now.strftime("%Y-%m")
        prev = now.replace(day=1) - timedelta(days=1)
        prev_month = prev.strftime("%Y-%m")
        months = [prev_month, current_month]
        print(f"No --month specified, processing: {', '.join(months)}")

    # Determine clients
    if args.all:
        targets = list(clients.items())
    else:
        if args.client not in clients:
            sys.exit(f"Unknown client: {args.client}\nAvailable: {', '.join(clients)}")
        targets = [(args.client, clients[args.client])]

    # Process
    for month in months:
        for client_id, client_cfg in targets:
            try:
                process_client(client_id, client_cfg, month, s3_client, dry_run=args.dry_run)
            except Exception as e:
                print(f"  ERROR processing {client_id}/{month}: {e}")

    # Repair partitions once after all uploads
    if not args.no_repair:
        print("\nRepairing Athena partitions...")
        repair_partitions(athena_client, dry_run=args.dry_run)

    print("\nDone!")


if __name__ == "__main__":
    main()
