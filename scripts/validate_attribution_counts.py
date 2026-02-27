#!/usr/bin/env python3
"""
Validate attribution counts across all data sources.

Compares three independent data sources for each client+month:
  1. GCLID attribution  — exact click-level matching (gclid_attribution.py)
  2. Synthetic spread    — proportional cost-based distribution (generate_daily_attribution.py)
  3. Athena parquet      — auto-attribution pipeline ground truth (external team)

Usage:
    python scripts/validate_attribution_counts.py                          # All clients, Jan 2026
    python scripts/validate_attribution_counts.py --month 2026-01          # All clients, specific month
    python scripts/validate_attribution_counts.py --client kitsap_cu       # Single client
    python scripts/validate_attribution_counts.py --client kitsap_cu --month 2026-01 --verbose
"""

import argparse
import csv
import io
import os
import sys
from pathlib import Path

import yaml

try:
    import boto3
except ImportError:
    sys.exit("boto3 is required: pip install boto3")

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None
    print("WARNING: pyarrow not installed. Athena parquet validation will be skipped.")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CLIENTS_CONFIG = PROJECT_ROOT / "config" / "clients.yaml"

S3_BUCKET = "etl.alpharank.airflow"
S3_PREFIX = "staging/google_ads_campaign_data"


def load_clients():
    with open(CLIENTS_CONFIG) as f:
        return yaml.safe_load(f)


def read_local_csv(path):
    """Read a CSV file, return list of dicts. Returns [] if missing."""
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return list(csv.DictReader(f))


def safe_sum(rows, field):
    """Sum a numeric field across rows, handling missing/empty values."""
    return sum(float(r.get(field, 0) or 0) for r in rows)


def load_athena_parquet(s3_client, partition_ids, month):
    """Download and read the parquet file from S3 for the given month.

    Tries multiple partition IDs (prod_id, athena_id, client_key) since
    the auto-attribution pipeline uses inconsistent partition naming.

    Returns (DataFrame, partition_id_used) or (None, None).
    """
    if pq is None:
        return None, None

    for pid in partition_ids:
        if not pid:
            continue
        prefix = f"{S3_PREFIX}/client_id={pid}/"
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        if not resp.get("Contents"):
            continue

        for obj in resp["Contents"]:
            if month in obj["Key"] and obj["Key"].endswith(".parquet"):
                data = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                df = pq.read_table(io.BytesIO(data["Body"].read())).to_pandas()
                df.columns = [c.lower() for c in df.columns]
                return df, pid

    return None, None


def validate_client(client_key, cfg, month, s3_client, verbose=False):
    """Run validation for a single client+month. Returns a result dict."""
    data_dir = DATA_DIR / client_key
    result = {
        "client": client_key,
        "month": month,
        "gclid": None,
        "synthetic": None,
        "athena_daily": None,
        "athena_monthly": None,
        "issues": [],
    }

    # --- Source 1: GCLID attribution (enriched/{month}.csv) ---
    gclid_path = data_dir / "enriched" / f"{month}.csv"
    gclid_rows = read_local_csv(gclid_path)
    if gclid_rows is not None:
        result["gclid"] = {
            "apps": int(safe_sum(gclid_rows, "apps")),
            "approved": int(safe_sum(gclid_rows, "approved")),
            "funded": int(safe_sum(gclid_rows, "funded")),
            "campaigns": len(gclid_rows),
            "source": str(gclid_path.relative_to(PROJECT_ROOT)),
        }
    else:
        result["issues"].append(f"Missing GCLID file: {gclid_path.relative_to(PROJECT_ROOT)}")

    # --- Source 2: Synthetic spread (_first_click.csv) ---
    fc_path = data_dir / "enriched" / f"{month}_first_click.csv"
    fc_rows = read_local_csv(fc_path)
    if fc_rows is not None:
        result["synthetic"] = {
            "apps": round(safe_sum(fc_rows, "apps")),
            "approved": round(safe_sum(fc_rows, "approved")),
            "funded": round(safe_sum(fc_rows, "funded")),
            "campaigns": len(fc_rows),
            "source": str(fc_path.relative_to(PROJECT_ROOT)),
        }
    else:
        result["issues"].append(f"Missing _first_click file: {fc_path.relative_to(PROJECT_ROOT)}")

    # --- Source 3: Athena parquet (S3) ---
    partition_ids = [cfg.get("prod_id", ""), cfg.get("athena_id", ""), client_key]
    df, pid = load_athena_parquet(s3_client, partition_ids, month)
    if df is not None:
        result["athena_daily"] = {
            "apps": int(df["apps"].sum()),
            "approved": int(df["approved"].sum()) if "approved" in df.columns else None,
            "funded": int(df["funded"].sum()) if "funded" in df.columns else None,
            "rows": len(df),
            "days": int(df["day"].nunique()) if "day" in df.columns else None,
            "partition": pid,
        }
        # Campaign-month aggregation
        agg = {"apps": "sum"}
        if "approved" in df.columns:
            agg["approved"] = "sum"
        if "funded" in df.columns:
            agg["funded"] = "sum"
        monthly = df.groupby("campaign_id").agg(agg).reset_index()
        result["athena_monthly"] = {
            "apps": int(monthly["apps"].sum()),
            "approved": int(monthly["approved"].sum()) if "approved" in monthly.columns else None,
            "funded": int(monthly["funded"].sum()) if "funded" in monthly.columns else None,
            "campaigns": len(monthly),
        }
    else:
        result["issues"].append(f"No Athena parquet for {month} (tried partitions: {partition_ids})")

    # --- Cross-source checks ---
    if result["gclid"] and result["athena_monthly"]:
        gclid_apps = result["gclid"]["apps"]
        athena_apps = result["athena_monthly"]["apps"]
        if athena_apps > 0:
            capture_rate = gclid_apps / athena_apps * 100
            result["gclid_capture_rate"] = round(capture_rate, 1)
            if capture_rate < 50:
                result["issues"].append(
                    f"GCLID captures only {capture_rate:.0f}% of apps "
                    f"({gclid_apps} vs {athena_apps} in Athena)"
                )

    if result["synthetic"] and result["athena_monthly"]:
        synth_apps = result["synthetic"]["apps"]
        athena_apps = result["athena_monthly"]["apps"]
        if athena_apps > 0 and synth_apps > athena_apps * 2:
            result["issues"].append(
                f"Synthetic _first_click inflated: {synth_apps} apps vs "
                f"{athena_apps} in Athena (includes non-Google-Ads campaigns)"
            )

    if result["athena_daily"] and result["athena_monthly"]:
        daily_sum = result["athena_daily"]["apps"]
        monthly_agg = result["athena_monthly"]["apps"]
        if daily_sum != monthly_agg:
            result["issues"].append(
                f"Daily sum ({daily_sum}) != campaign-month agg ({monthly_agg}) "
                f"— {daily_sum - monthly_agg} apps appear in multiple daily rows"
            )

    return result


def print_result(result, verbose=False):
    """Pretty-print a validation result."""
    client = result["client"]
    month = result["month"]
    print(f"\n{'=' * 80}")
    print(f"  {client} — {month}")
    print(f"{'=' * 80}")

    header = f"  {'Source':<45s} {'Apps':>6s} {'Approved':>9s} {'Funded':>7s} {'Campaigns':>10s}"
    divider = f"  {'-'*45} {'-'*6} {'-'*9} {'-'*7} {'-'*10}"

    print(header)
    print(divider)

    if result["gclid"]:
        g = result["gclid"]
        print(f"  {'GCLID attribution (exact click match)':<45s} {g['apps']:>6d} {g['approved']:>9d} {g['funded']:>7d} {g['campaigns']:>10d}")
    else:
        print(f"  {'GCLID attribution':<45s} {'--':>6s} {'--':>9s} {'--':>7s} {'--':>10s}")

    if result["synthetic"]:
        s = result["synthetic"]
        print(f"  {'Synthetic _first_click (cost-proportional)':<45s} {s['apps']:>6d} {s['approved']:>9d} {s['funded']:>7d} {s['campaigns']:>10d}")
    else:
        print(f"  {'Synthetic _first_click':<45s} {'--':>6s} {'--':>9s} {'--':>7s} {'--':>10s}")

    if result["athena_monthly"]:
        a = result["athena_monthly"]
        appvd = f"{a['approved']:>9d}" if a["approved"] is not None else f"{'--':>9s}"
        fnded = f"{a['funded']:>7d}" if a["funded"] is not None else f"{'--':>7s}"
        print(f"  {'Athena parquet (campaign-month, TRUTH)':<45s} {a['apps']:>6d} {appvd} {fnded} {a['campaigns']:>10d}")
    else:
        print(f"  {'Athena parquet (not available)':<45s} {'--':>6s} {'--':>9s} {'--':>7s} {'--':>10s}")

    if result["athena_daily"]:
        d = result["athena_daily"]
        print(f"\n  Athena daily detail: {d['rows']} rows across {d['days']} days, sum(apps) = {d['apps']}")

    if "gclid_capture_rate" in result:
        print(f"\n  GCLID capture rate: {result['gclid_capture_rate']}%")

    if result["issues"]:
        print(f"\n  Issues:")
        for issue in result["issues"]:
            print(f"    ! {issue}")

    if verbose and result["gclid"] and result["athena_monthly"]:
        print(f"\n  Root cause: GCLID attribution uses `click_id IS NOT NULL AND click_id != ''`")
        print(f"  filter in prod.application_data. Applications without a Google click_id")
        print(f"  (organic, direct, other channels) are excluded from GCLID counts.")


def main():
    parser = argparse.ArgumentParser(description="Validate attribution counts across data sources")
    parser.add_argument("--month", default="2026-01", help="Month to validate (YYYY-MM)")
    parser.add_argument("--client", help="Validate a single client (default: all)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed analysis")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    clients = load_clients()
    s3_client = boto3.client("s3", region_name="us-west-2")

    if args.client:
        if args.client not in clients:
            sys.exit(f"Unknown client: {args.client}. Available: {list(clients.keys())}")
        targets = {args.client: clients[args.client]}
    else:
        targets = clients

    results = []
    for key, cfg in targets.items():
        result = validate_client(key, cfg, args.month, s3_client, args.verbose)
        results.append(result)
        if not args.json:
            print_result(result, args.verbose)

    if args.json:
        import json
        print(json.dumps(results, indent=2, default=str))
        return

    # Summary
    total_issues = sum(len(r["issues"]) for r in results)
    print(f"\n{'=' * 80}")
    print(f"  Summary: {len(results)} clients validated, {total_issues} issues found")
    print(f"{'=' * 80}")

    if total_issues:
        print("\n  All issues:")
        for r in results:
            for issue in r["issues"]:
                print(f"    [{r['client']}] {issue}")


if __name__ == "__main__":
    main()
