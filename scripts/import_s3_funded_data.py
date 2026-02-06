#!/usr/bin/env python3
"""
Import funded data from S3 and create enriched CSV files for the dashboard.

This script reads the digital_performance_ranking data from S3, extracts
campaign attribution, and creates enriched CSV files that merge with
Google Ads campaign data.

Usage:
    python scripts/import_s3_funded_data.py
    python scripts/import_s3_funded_data.py --client kitsap_cu --month 2026-01
"""

import argparse
import re
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path

try:
    import boto3
    import pandas as pd
except ImportError:
    sys.exit("Required: pip install boto3 pandas")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# S3 bucket containing first-party attribution data
S3_BUCKET = "ai.alpharank.core"

# Client ID to S3 folder mapping
CLIENT_S3_PATHS = {
    "kitsap_cu": "customers/kitsap/downloads",
    "californiacoast_cu": "customers/californiacoast/downloads",
    "commonwealth_one_fcu": "customers/commonwealth/downloads",
    "firstcommunity_cu": "customers/firstcommunity/downloads",
    "publicservice_cu": "customers/publicservice/downloads",
}


def extract_campaign_id(full_attribution: str) -> str:
    """Extract campaign_id from full_attribution string.

    The campaign_id is typically an 11-digit number in the attribution path.
    Example: "google_cpc/23430737916/..." -> "23430737916"
    """
    if not full_attribution or pd.isna(full_attribution):
        return None

    # Look for 11-digit campaign IDs in the attribution string
    match = re.search(r'/(\d{11})/', str(full_attribution))
    if match:
        return match.group(1)

    # Also try without surrounding slashes
    match = re.search(r'(\d{11})', str(full_attribution))
    if match:
        return match.group(1)

    return None


def load_dpr_from_s3(s3_client, client_id: str) -> pd.DataFrame:
    """Load digital performance ranking data from S3."""
    s3_path = CLIENT_S3_PATHS.get(client_id)
    if not s3_path:
        print(f"No S3 path configured for {client_id}")
        return pd.DataFrame()

    # Try different file naming patterns
    file_patterns = [
        f"{s3_path}/{client_id.replace('_cu', '')}_digital_performance_ranking.csv",
        f"{s3_path}/digital_performance_ranking.csv",
        f"{s3_path}/{client_id}_dpr.csv",
    ]

    for key in file_patterns:
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(content))
            print(f"  Loaded from s3://{S3_BUCKET}/{key}: {len(df)} rows")
            return df
        except s3_client.exceptions.NoSuchKey:
            continue
        except Exception as e:
            print(f"  Error loading {key}: {e}")
            continue

    print(f"  No DPR file found for {client_id}")
    return pd.DataFrame()


def process_funded_data(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Process DPR data to extract campaign-level funded metrics."""
    if df.empty:
        return pd.DataFrame()

    # Work on a copy
    funded_df = df.copy()

    # Filter by month - check for direct 'month' column first
    if 'month' in funded_df.columns:
        funded_df = funded_df[funded_df['month'] == month]
    else:
        # Try date columns
        date_cols = ['funded_date', 'date', 'created_date', 'loan_date']
        date_col = None
        for col in date_cols:
            if col in funded_df.columns:
                date_col = col
                break

        if date_col:
            funded_df[date_col] = pd.to_datetime(funded_df[date_col], errors='coerce')
            funded_df = funded_df[funded_df[date_col].dt.strftime('%Y-%m') == month]

    if funded_df.empty:
        return pd.DataFrame()

    # Extract campaign_id from full_attribution
    attr_col = None
    for col in ['full_attribution', 'attribution', 'campaign_path', 'utm_content']:
        if col in funded_df.columns:
            attr_col = col
            break

    if not attr_col:
        print("  No attribution column found")
        return pd.DataFrame()

    funded_df['campaign_id'] = funded_df[attr_col].apply(extract_campaign_id)

    # Filter to records with valid campaign_id
    funded_df = funded_df[funded_df['campaign_id'].notna()]

    if funded_df.empty:
        print("  No records with valid campaign_id")
        return pd.DataFrame()

    # The DPR data is already aggregated - each row is a unique combination
    # Parse numeric columns
    for col in ['Funded', 'Production', 'Lifetime Value', 'Unique Visitors', 'Application Starts', 'Completed', 'Approved']:
        if col in funded_df.columns:
            funded_df[col] = pd.to_numeric(funded_df[col], errors='coerce').fillna(0)

    # Aggregate by campaign_id
    agg_cols = {}
    if 'Funded' in funded_df.columns:
        agg_cols['Funded'] = 'sum'
    if 'Production' in funded_df.columns:
        agg_cols['Production'] = 'sum'
    if 'Lifetime Value' in funded_df.columns:
        agg_cols['Lifetime Value'] = 'sum'
    if 'Application Starts' in funded_df.columns:
        agg_cols['Application Starts'] = 'sum'
    if 'Approved' in funded_df.columns:
        agg_cols['Approved'] = 'sum'

    if not agg_cols:
        # Fallback to counting rows
        result = funded_df.groupby('campaign_id', as_index=False).size()
        result.columns = ['campaign_id', 'funded']
        result['value'] = 0
    else:
        result = funded_df.groupby('campaign_id', as_index=False).agg(agg_cols)
        # Rename columns to match dashboard expectations
        col_map = {
            'Funded': 'funded',
            'Production': 'production',
            'Lifetime Value': 'value',
            'Application Starts': 'apps',
            'Approved': 'approved'
        }
        result = result.rename(columns=col_map)

    # Ensure all expected columns exist
    for col in ['funded', 'production', 'value', 'apps', 'approved']:
        if col not in result.columns:
            result[col] = 0

    # Use production as value if value not present
    if 'value' not in result.columns or result['value'].sum() == 0:
        if 'production' in result.columns:
            result['value'] = result['production']
        else:
            result['value'] = 0

    if 'production' not in result.columns:
        result['production'] = result['value']

    # Add computed columns
    result['cpf'] = 0  # Will be computed in dashboard with cost data
    result['avg_funded_value'] = result.apply(
        lambda r: r['value'] / r['funded'] if r['funded'] > 0 else 0, axis=1
    )

    return result


def main():
    parser = argparse.ArgumentParser(description='Import funded data from S3')
    parser.add_argument('--client', help='Specific client to process')
    parser.add_argument('--month', help='Month to process (YYYY-MM)')
    parser.add_argument('--all-months', action='store_true', help='Process all available months')
    args = parser.parse_args()

    s3_client = boto3.client('s3', region_name='us-east-1')

    # Determine clients to process
    clients = [args.client] if args.client else list(CLIENT_S3_PATHS.keys())

    # Determine months to process
    if args.month:
        months = [args.month]
    elif args.all_months:
        months = ['2025-11', '2025-12', '2026-01', '2026-02']
    else:
        months = [datetime.utcnow().strftime('%Y-%m')]

    for client_id in clients:
        print(f"\nProcessing {client_id}...")

        # Load DPR data from S3
        dpr_df = load_dpr_from_s3(s3_client, client_id)
        if dpr_df.empty:
            continue

        for month in months:
            print(f"  Month: {month}")

            # Process funded data
            enriched = process_funded_data(dpr_df, month)

            if enriched.empty:
                print(f"    No funded data for {month}")
                continue

            # Save to enriched directory
            out_dir = DATA_DIR / client_id / "enriched"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{month}.csv"

            # Ensure column order matches what dashboard expects
            cols = ['campaign_id', 'apps', 'approved', 'funded', 'production', 'value', 'cpf', 'avg_funded_value']
            enriched = enriched.reindex(columns=cols, fill_value=0)

            enriched.to_csv(out_path, index=False)
            print(f"    Saved {out_path}: {len(enriched)} campaigns, {enriched['funded'].sum():.0f} total funded")

    print("\nDone!")


if __name__ == '__main__':
    main()
