#!/usr/bin/env python3
"""
GCLID-based keyword-level attribution.

Joins click-level GCLID data (from S3) with application data (from Athena,
using the click_id field) to produce real keyword-level attribution —
replacing synthetic proportional distribution with exact click-to-application mapping.

Usage:
    python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01
    python scripts/gclid_attribution.py --all --month 2026-01
    python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01 --dry-run
"""

import argparse
import calendar
import sys
import time
from io import StringIO
from pathlib import Path

try:
    import boto3
    import pandas as pd
except ImportError:
    sys.exit("Required: pip install boto3 pandas")

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CLIENTS_CONFIG = PROJECT_ROOT / "config" / "clients.yaml"
AWS_CONFIG = PROJECT_ROOT / "config" / "config.yaml"

# Athena settings (same as export_athena_data.py)
ATHENA_DATABASE = "staging"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_BUCKET = "s3://etl.alpharank.airflow/athena-results/"
AWS_REGION = "us-west-2"
ATHENA_QUERY_TIMEOUT_SECONDS = 300

# Application data query — uses click_id field directly
APPLICATION_QUERY = """
SELECT
    click_id AS gclid,
    1 AS received,
    CASE WHEN approved = true THEN 1 ELSE 0 END AS approved,
    CASE WHEN funded = true THEN 1 ELSE 0 END AS funded,
    COALESCE(production_value, 0) AS production_value,
    COALESCE(lifetime_value, 0) AS lifetime_value,
    COALESCE(product_family, '') AS product_family
FROM prod.application_data
WHERE client_id = ?
    AND report_completion_timestamp >= CAST(? AS TIMESTAMP)
    AND report_completion_timestamp < CAST(? AS TIMESTAMP)
    AND click_id IS NOT NULL AND click_id != ''
"""


# ── Config loading ──────────────────────────────────────────────────

def load_clients() -> dict:
    """Load full clients config from clients.yaml."""
    with open(CLIENTS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def load_aws_config() -> dict:
    """Load AWS/S3 config from config.yaml."""
    with open(AWS_CONFIG, "r") as f:
        return yaml.safe_load(f)


# ── Athena helpers (reused from export_athena_data.py) ──────────────

def run_athena_query(client, query: str, params: list = None) -> str:
    """Submit an Athena query and wait for completion. Returns execution ID."""
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
        if state == "SUCCEEDED":
            return execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)


def fetch_results(client, execution_id: str) -> pd.DataFrame:
    """Fetch all result rows from a completed Athena query."""
    paginator = client.get_paginator("get_query_results")
    rows = []
    columns = None

    for page in paginator.paginate(QueryExecutionId=execution_id):
        result_set = page["ResultSet"]
        if columns is None:
            columns = [col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]
            data_rows = result_set["Rows"][1:]  # skip header row in first page
        else:
            data_rows = result_set["Rows"]

        for row in data_rows:
            rows.append([field.get("VarCharValue", "") for field in row["Data"]])

    return pd.DataFrame(rows, columns=columns)


# ── S3 click data loading ──────────────────────────────────────────

def load_click_data(s3_client, bucket: str, prefix: str, client_id: str, month: str) -> pd.DataFrame:
    """Load all click CSVs for a client/month from S3.

    Reads s3://{bucket}/{prefix}/{client_id}/clicks/{month}-*.csv
    """
    click_prefix = f"{prefix}/{client_id}/clicks/{month}-"

    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=click_prefix)
    csv_keys = [
        obj["Key"]
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    if not csv_keys:
        return pd.DataFrame()

    frames = []
    for key in csv_keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
        frames.append(df)

    clicks = pd.concat(frames, ignore_index=True)
    return clicks


# ── Athena application query ───────────────────────────────────────

def query_applications(athena_client, athena_id: str, month: str) -> pd.DataFrame:
    """Query application data from Athena for a given client and month.

    Date range: first of month through +2 months (captures delayed applications).
    """
    year, mo = month.split("-")
    start_ts = f"{year}-{mo}-01 00:00:00"

    # End date: +2 months from start of target month
    end_month = int(mo) + 2
    end_year = int(year)
    if end_month > 12:
        end_month -= 12
        end_year += 1
    end_ts = f"{end_year}-{end_month:02d}-01 00:00:00"

    print(f"  Querying Athena: client_id={athena_id}, range=[{start_ts}, {end_ts})")
    execution_id = run_athena_query(
        athena_client, APPLICATION_QUERY, params=[athena_id, start_ts, end_ts]
    )
    df = fetch_results(athena_client, execution_id)

    if df.empty:
        return df

    # Cast numeric columns
    for col in ["received", "approved", "funded", "production_value", "lifetime_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Ensure product_family is a clean string
    if "product_family" in df.columns:
        df["product_family"] = df["product_family"].fillna("").astype(str).str.strip()

    return df


# ── Join + aggregate ───────────────────────────────────────────────

def build_attribution(clicks_df: pd.DataFrame, apps_df: pd.DataFrame, dry_run: bool = False) -> tuple:
    """Join clicks to applications on GCLID; aggregate to campaign and daily-keyword levels.

    Returns (campaign_df, daily_keyword_df) or (None, None) if no matches.
    """
    if clicks_df.empty or apps_df.empty:
        print("  No data to join (clicks or apps empty)")
        return None, None

    # Inner join on gclid
    joined = clicks_df.merge(apps_df, on="gclid", how="inner")

    if joined.empty:
        print("  No GCLID matches found")
        return None, None

    # Product-level breakdown columns (approved only)
    joined["cc_appvd"] = ((joined["product_family"] == "Credit Card") & (joined["approved"] == 1)).astype(int)
    joined["deposit_appvd"] = ((joined["product_family"] == "Xpress App") & (joined["approved"] == 1)).astype(int)
    joined["personal_appvd"] = ((joined["product_family"] == "Personal Loan") & (joined["approved"] == 1)).astype(int)
    joined["vehicle_appvd"] = ((joined["product_family"] == "Vehicle Loan") & (joined["approved"] == 1)).astype(int)
    joined["heloc_appvd"] = ((joined["product_family"] == "Home Equity Loan") & (joined["approved"] == 1)).astype(int)

    # Stats
    n_clicks = len(clicks_df)
    n_apps = len(apps_df)
    n_matched = len(joined)
    n_unmatched_apps = n_apps - apps_df["gclid"].isin(joined["gclid"]).sum()
    print(f"  Clicks: {n_clicks:,}  |  Apps: {n_apps:,}  |  Matched: {n_matched:,}  |  Unmatched apps: {n_unmatched_apps:,}")
    print(f"  Matched apps: {joined['received'].sum():.0f} received, "
          f"{joined['funded'].sum():.0f} funded, "
          f"${joined['lifetime_value'].sum():,.0f} value")

    if dry_run:
        return None, None

    # ── Campaign-level aggregation ──
    campaign_agg = (
        joined.groupby(["campaign_id", "campaign_name"], as_index=False)
        .agg(
            apps=("received", "sum"),
            approved=("approved", "sum"),
            funded=("funded", "sum"),
            production=("production_value", "sum"),
            value=("lifetime_value", "sum"),
            cc_appvd=("cc_appvd", "sum"),
            deposit_appvd=("deposit_appvd", "sum"),
            personal_appvd=("personal_appvd", "sum"),
            vehicle_appvd=("vehicle_appvd", "sum"),
            heloc_appvd=("heloc_appvd", "sum"),
        )
    )
    campaign_agg["cpf"] = 0  # dashboard computes from cost data
    campaign_agg["avg_funded_value"] = campaign_agg.apply(
        lambda r: round(r["value"] / r["funded"], 2) if r["funded"] > 0 else 0, axis=1
    )

    # ── Daily + keyword-level aggregation ──
    group_cols = [
        "date", "campaign_id", "campaign_name",
        "ad_group_id", "ad_group_name", "keyword", "match_type",
    ]
    daily_kw_agg = (
        joined.groupby(group_cols, as_index=False)
        .agg(
            apps=("received", "sum"),
            approved=("approved", "sum"),
            funded=("funded", "sum"),
            production=("production_value", "sum"),
            value=("lifetime_value", "sum"),
            cc_appvd=("cc_appvd", "sum"),
            deposit_appvd=("deposit_appvd", "sum"),
            personal_appvd=("personal_appvd", "sum"),
            vehicle_appvd=("vehicle_appvd", "sum"),
            heloc_appvd=("heloc_appvd", "sum"),
        )
    )

    return campaign_agg, daily_kw_agg


# ── Output writing ─────────────────────────────────────────────────

def write_campaign_csv(df: pd.DataFrame, client_id: str, month: str) -> Path:
    """Write campaign-level enriched CSV."""
    out_dir = DATA_DIR / client_id / "enriched"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{month}.csv"

    cols = [
        "campaign_id", "campaign_name", "apps", "approved", "funded",
        "production", "value", "cpf", "avg_funded_value",
        "cc_appvd", "deposit_appvd", "personal_appvd", "vehicle_appvd", "heloc_appvd",
    ]
    df[cols].to_csv(out_path, index=False)
    print(f"  Wrote {out_path} ({len(df)} campaigns)")
    return out_path


def write_daily_keyword_csv(df: pd.DataFrame, client_id: str, month: str) -> Path:
    """Write daily keyword-level enriched CSV."""
    out_dir = DATA_DIR / client_id / "enriched" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{month}.csv"

    cols = [
        "date", "campaign_id", "campaign_name",
        "ad_group_id", "ad_group_name", "keyword", "match_type",
        "apps", "approved", "funded", "production", "value",
        "cc_appvd", "deposit_appvd", "personal_appvd", "vehicle_appvd", "heloc_appvd",
    ]
    df[cols].sort_values(["date", "campaign_id", "keyword"]).to_csv(out_path, index=False)
    print(f"  Wrote {out_path} ({len(df)} rows)")
    return out_path


# ── Main ───────────────────────────────────────────────────────────

def process_client(client_id: str, client_cfg: dict, month: str,
                   s3_client, athena_client, aws_cfg: dict, dry_run: bool = False):
    """Run GCLID attribution for a single client/month."""
    bucket = aws_cfg["aws"]["bucket"]
    prefix = aws_cfg["aws"]["prefix"]

    print(f"\n{'='*60}")
    print(f"  {client_cfg['name']} ({client_id})  —  {month}")
    print(f"{'='*60}")

    # 1. Load click data from S3
    print("  Loading click data from S3...")
    clicks_df = load_click_data(s3_client, bucket, prefix, client_id, month)
    if clicks_df.empty:
        print("  No click data found — skipping")
        return
    print(f"  Loaded {len(clicks_df):,} clicks")

    # 2. Query application data from Athena (same client_id as click data)
    apps_df = query_applications(athena_client, client_id, month)
    if apps_df.empty:
        print("  No application data found — skipping")
        return
    print(f"  Loaded {len(apps_df):,} applications with GCLIDs")

    # 3. Join + aggregate
    campaign_df, daily_kw_df = build_attribution(clicks_df, apps_df, dry_run=dry_run)

    if dry_run:
        print("  [dry-run] No files written")
        return

    if campaign_df is None:
        return

    # 4. Write outputs
    write_campaign_csv(campaign_df, client_id, month)
    write_daily_keyword_csv(daily_kw_df, client_id, month)


def main():
    parser = argparse.ArgumentParser(
        description="GCLID-based keyword-level attribution"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--client", help="Process a single client_id (e.g. kitsap_cu)")
    group.add_argument("--all", action="store_true", help="Process all clients")
    parser.add_argument("--month", required=True, help="Target month (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print match statistics without writing files")
    args = parser.parse_args()

    clients = load_clients()
    aws_cfg = load_aws_config()

    s3_client = boto3.client("s3", region_name=aws_cfg["aws"]["region"])
    athena_client = boto3.client("athena", region_name=AWS_REGION)

    if args.all:
        targets = list(clients.items())
    else:
        if args.client not in clients:
            sys.exit(f"Unknown client: {args.client}\nAvailable: {', '.join(clients)}")
        targets = [(args.client, clients[args.client])]

    for client_id, client_cfg in targets:
        try:
            process_client(
                client_id, client_cfg, args.month,
                s3_client, athena_client, aws_cfg,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"  ERROR processing {client_id}: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
