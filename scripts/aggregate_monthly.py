#!/usr/bin/env python3
"""
Aggregate daily output CSVs into monthly data CSVs for the dashboard.
Includes campaign_id and ad_group_id columns that the daily files have.

Usage:
    python scripts/aggregate_monthly.py
"""

import json
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
DATA_DIR = PROJECT_ROOT / "data"
MANIFEST_PATH = PROJECT_ROOT / "data-manifest.json"
CLIENTS_CONFIG = PROJECT_ROOT / "config" / "clients.yaml"


def load_client_ids() -> list:
    """Load client IDs from the centralized clients.yaml config."""
    with open(CLIENTS_CONFIG, 'r') as f:
        clients = yaml.safe_load(f)
    return list(clients.keys())

CAMPAIGN_COLS = ["campaign_id", "campaign_name", "impressions", "clicks", "cost", "conversions"]
KEYWORD_COLS = ["campaign_id", "campaign_name", "ad_group_id", "ad_group_name", "keyword", "match_type", "impressions", "clicks", "cost", "conversions"]
DAILY_COLS = ["date", "campaign_id", "campaign_name", "impressions", "clicks", "cost", "conversions"]


def aggregate_daily_timeseries(client_id: str) -> dict[str, pd.DataFrame]:
    """Preserve per-day, per-campaign granularity for dashboard charts."""
    daily_dir = OUTPUT_DIR / client_id / "campaigns"
    if not daily_dir.exists():
        return {}

    months = defaultdict(list)
    for csv_file in sorted(daily_dir.glob("*.csv")):
        month = csv_file.stem[:7]
        df = pd.read_csv(csv_file)
        months[month].append(df)

    result = {}
    for month, frames in months.items():
        combined = pd.concat(frames, ignore_index=True)
        agg = combined.groupby(["date", "campaign_id", "campaign_name"], as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "cost": "sum",
            "conversions": "sum",
        })
        agg = agg.sort_values(["date", "campaign_name"])
        agg["campaign_id"] = agg["campaign_id"].astype(str)
        result[month] = agg[DAILY_COLS]

    return result


def aggregate_campaigns(client_id: str) -> dict[str, pd.DataFrame]:
    """Aggregate daily campaign CSVs into monthly DataFrames."""
    daily_dir = OUTPUT_DIR / client_id / "campaigns"
    if not daily_dir.exists():
        return {}

    months = defaultdict(list)
    for csv_file in sorted(daily_dir.glob("*.csv")):
        month = csv_file.stem[:7]  # e.g. "2026-01"
        df = pd.read_csv(csv_file)
        months[month].append(df)

    result = {}
    for month, frames in months.items():
        combined = pd.concat(frames, ignore_index=True)
        # Group by campaign_id + campaign_name (they map 1:1)
        agg = combined.groupby(["campaign_id", "campaign_name"], as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "cost": "sum",
            "conversions": "sum",
        })
        agg = agg.sort_values("clicks", ascending=False)
        # Ensure campaign_id is string (no scientific notation)
        agg["campaign_id"] = agg["campaign_id"].astype(str)
        result[month] = agg[CAMPAIGN_COLS]

    return result


def aggregate_keywords(client_id: str) -> dict[str, pd.DataFrame]:
    """Aggregate daily keyword CSVs into monthly DataFrames."""
    daily_dir = OUTPUT_DIR / client_id / "keywords"
    if not daily_dir.exists():
        return {}

    months = defaultdict(list)
    for csv_file in sorted(daily_dir.glob("*.csv")):
        month = csv_file.stem[:7]
        df = pd.read_csv(csv_file)
        months[month].append(df)

    result = {}
    for month, frames in months.items():
        combined = pd.concat(frames, ignore_index=True)
        group_keys = ["campaign_id", "campaign_name", "ad_group_id", "ad_group_name", "keyword", "match_type"]
        agg = combined.groupby(group_keys, as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "cost": "sum",
            "conversions": "sum",
        })
        agg = agg.sort_values("clicks", ascending=False)
        agg["campaign_id"] = agg["campaign_id"].astype(str)
        agg["ad_group_id"] = agg["ad_group_id"].astype(str)
        result[month] = agg[KEYWORD_COLS]

    return result


def main():
    manifest = {}

    for client_id in load_client_ids():
        print(f"Processing {client_id}...")
        available_months = set()

        # Daily timeseries (for charts)
        daily_months = aggregate_daily_timeseries(client_id)
        for month, df in daily_months.items():
            out_dir = DATA_DIR / client_id / "daily"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{month}.csv"
            df.to_csv(out_path, index=False)
            available_months.add(month)
            print(f"  daily/{month}.csv: {len(df)} rows")

        # Campaigns
        camp_months = aggregate_campaigns(client_id)
        for month, df in camp_months.items():
            out_dir = DATA_DIR / client_id / "campaigns"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{month}.csv"
            df.to_csv(out_path, index=False)
            available_months.add(month)
            print(f"  campaigns/{month}.csv: {len(df)} rows")

        # Keywords
        kw_months = aggregate_keywords(client_id)
        for month, df in kw_months.items():
            out_dir = DATA_DIR / client_id / "keywords"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{month}.csv"
            df.to_csv(out_path, index=False)
            available_months.add(month)
            print(f"  keywords/{month}.csv: {len(df)} rows")

        manifest[client_id] = sorted(available_months, reverse=True)

    # Update manifest
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nUpdated {MANIFEST_PATH}")
    print("Done!")


if __name__ == "__main__":
    main()
