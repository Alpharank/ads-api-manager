#!/usr/bin/env python3
"""
Pull Google Ads data and write to data/ directory for the dashboard.

Pulls campaign, daily, keyword, channel, device, location, and search term data
from the Google Ads API and writes monthly CSVs in the format the dashboard expects.

Usage:
    python scripts/pull_dashboard_data.py                  # Last 3 months
    python scripts/pull_dashboard_data.py --months 6       # Last 6 months
    python scripts/pull_dashboard_data.py --client embenauto  # Specific client only
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Project root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")


def load_config(path=None):
    if path is None:
        path = os.path.join(ROOT, "config", "config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def build_client(config):
    return GoogleAdsClient.load_from_dict({
        "developer_token": config["google_ads"]["developer_token"],
        "client_id": config["google_ads"]["client_id"],
        "client_secret": config["google_ads"]["client_secret"],
        "refresh_token": config["google_ads"]["refresh_token"],
        "login_customer_id": config["google_ads"]["login_customer_id"],
        "use_proto_plus": True,
    })


def get_accounts(client, config):
    """Get accessible child accounts from MCC."""
    mcc_id = config["google_ads"]["login_customer_id"]
    mapping = config.get("client_mapping", {})
    svc = client.get_service("GoogleAdsService")

    query = """
        SELECT
            customer_client.id,
            customer_client.descriptive_name,
            customer_client.manager,
            customer_client.status
        FROM customer_client
        WHERE customer_client.manager = FALSE
            AND customer_client.status = 'ENABLED'
    """

    accounts = []
    try:
        for row in svc.search(customer_id=mcc_id, query=query):
            cid = str(row.customer_client.id)
            accounts.append({
                "customer_id": cid,
                "name": row.customer_client.descriptive_name,
                "client_id": mapping.get(cid, f"account_{cid}"),
            })
    except GoogleAdsException as e:
        print(f"Error listing accounts: {e}")
        sys.exit(1)

    return accounts


def search(svc, customer_id, query, label="data"):
    try:
        return list(svc.search(customer_id=customer_id, query=query))
    except GoogleAdsException as e:
        print(f"  Warning: {label} query failed for {customer_id}: {e}")
        return []


def pull_all_data(client, customer_id, start_date, end_date):
    """Pull all data types for a date range. Returns dict of DataFrames keyed by month."""
    svc = client.get_service("GoogleAdsService")
    sd = start_date.strftime("%Y-%m-%d")
    ed = end_date.strftime("%Y-%m-%d")

    # --- Campaign data (daily) ---
    print(f"  Pulling campaign data ({sd} to {ed})...")
    q = f"""
        SELECT
            campaign.id, campaign.name, campaign.status,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM campaign
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    daily_rows = []
    for row in search(svc, customer_id, q, "campaigns"):
        daily_rows.append({
            "date": row.segments.date,
            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    daily_df = pd.DataFrame(daily_rows)

    # --- Keyword data ---
    print(f"  Pulling keyword data...")
    q = f"""
        SELECT
            campaign.id, campaign.name,
            ad_group.id, ad_group.name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM keyword_view
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    kw_rows = []
    for row in search(svc, customer_id, q, "keywords"):
        kw_rows.append({
            "date": row.segments.date,
            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
            "keyword": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    kw_df = pd.DataFrame(kw_rows)

    # --- Channel (network) data ---
    print(f"  Pulling channel data...")
    q = f"""
        SELECT
            segments.ad_network_type,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM campaign
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    ch_rows = []
    for row in search(svc, customer_id, q, "channels"):
        ch_rows.append({
            "date": row.segments.date,
            "network": row.segments.ad_network_type.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    ch_df = pd.DataFrame(ch_rows)

    # --- Device data ---
    print(f"  Pulling device data...")
    q = f"""
        SELECT
            segments.device,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM campaign
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    dev_rows = []
    for row in search(svc, customer_id, q, "devices"):
        dev_rows.append({
            "date": row.segments.date,
            "device": row.segments.device.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    dev_df = pd.DataFrame(dev_rows)

    # --- Location data ---
    print(f"  Pulling location data...")
    q = f"""
        SELECT
            campaign.id, campaign.name,
            geographic_view.country_criterion_id,
            geographic_view.location_type,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM geographic_view
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    loc_rows = []
    for row in search(svc, customer_id, q, "locations"):
        loc_rows.append({
            "date": row.segments.date,
            "location": f"geoTargetConstants/{row.geographic_view.country_criterion_id}",
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    loc_df = pd.DataFrame(loc_rows)

    # --- Search terms ---
    print(f"  Pulling search term data...")
    q = f"""
        SELECT
            search_term_view.search_term,
            segments.keyword.info.text,
            segments.keyword.info.match_type,
            campaign.id, campaign.name,
            ad_group.id, ad_group.name,
            metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
            segments.date
        FROM search_term_view
        WHERE segments.date BETWEEN '{sd}' AND '{ed}'
            AND metrics.impressions > 0
    """
    st_rows = []
    for row in search(svc, customer_id, q, "search_terms"):
        st_rows.append({
            "date": row.segments.date,
            "search_term": row.search_term_view.search_term,
            "keyword": row.segments.keyword.info.text if row.segments.keyword.info.text else "",
            "match_type": row.segments.keyword.info.match_type.name if row.segments.keyword.info.match_type else "",
            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": row.metrics.cost_micros / 1_000_000,
            "conversions": row.metrics.conversions,
        })
    st_df = pd.DataFrame(st_rows)

    return {
        "daily": daily_df,
        "keywords": kw_df,
        "channels": ch_df,
        "devices": dev_df,
        "locations": loc_df,
        "search_terms": st_df,
    }


def aggregate_monthly(df, date_col="date", group_cols=None, sum_cols=None):
    """Aggregate a dataframe by month."""
    if df.empty:
        return {}

    df = df.copy()
    df["_month"] = df[date_col].str[:7]  # YYYY-MM

    months = {}
    for month, group in df.groupby("_month"):
        if group_cols:
            agg = group.groupby(group_cols, as_index=False)[sum_cols].sum()
        else:
            agg = group[sum_cols].sum().to_frame().T if sum_cols else group
        months[month] = agg

    return months


def write_monthly_csvs(data, client_id, data_dir):
    """Write all data to monthly CSV files."""
    client_dir = os.path.join(data_dir, client_id)
    metrics = ["impressions", "clicks", "cost", "conversions"]

    # --- Daily (keep as-is, just split by month) ---
    if not data["daily"].empty:
        daily = data["daily"]
        daily["_month"] = daily["date"].str[:7]
        for month, group in daily.groupby("_month"):
            out_dir = os.path.join(client_dir, "daily")
            os.makedirs(out_dir, exist_ok=True)
            group.drop(columns=["_month"]).to_csv(
                os.path.join(out_dir, f"{month}.csv"), index=False
            )
            print(f"    daily/{month}.csv ({len(group)} rows)")

    # --- Campaigns (aggregate by campaign per month) ---
    if not data["daily"].empty:
        campaigns = aggregate_monthly(
            data["daily"],
            group_cols=["campaign_id", "campaign_name"],
            sum_cols=metrics,
        )
        for month, df in campaigns.items():
            out_dir = os.path.join(client_dir, "campaigns")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    campaigns/{month}.csv ({len(df)} rows)")

    # --- Keywords (aggregate by keyword per month) ---
    if not data["keywords"].empty:
        kw = aggregate_monthly(
            data["keywords"],
            group_cols=["campaign_id", "campaign_name", "ad_group_id", "ad_group_name", "keyword", "match_type"],
            sum_cols=metrics,
        )
        for month, df in kw.items():
            out_dir = os.path.join(client_dir, "keywords")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    keywords/{month}.csv ({len(df)} rows)")

    # --- Channels (aggregate by network per month) ---
    if not data["channels"].empty:
        ch = aggregate_monthly(
            data["channels"],
            group_cols=["network"],
            sum_cols=metrics,
        )
        for month, df in ch.items():
            out_dir = os.path.join(client_dir, "channels")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    channels/{month}.csv ({len(df)} rows)")

    # --- Devices (aggregate by device per month) ---
    if not data["devices"].empty:
        dev = aggregate_monthly(
            data["devices"],
            group_cols=["device"],
            sum_cols=metrics,
        )
        for month, df in dev.items():
            out_dir = os.path.join(client_dir, "devices")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    devices/{month}.csv ({len(df)} rows)")

    # --- Locations (aggregate by location per month) ---
    if not data["locations"].empty:
        loc = aggregate_monthly(
            data["locations"],
            group_cols=["location"],
            sum_cols=metrics,
        )
        for month, df in loc.items():
            out_dir = os.path.join(client_dir, "locations")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    locations/{month}.csv ({len(df)} rows)")

    # --- Search terms (aggregate per month) ---
    if not data["search_terms"].empty:
        st = aggregate_monthly(
            data["search_terms"],
            group_cols=["search_term", "keyword", "match_type", "campaign_id", "campaign_name", "ad_group_id", "ad_group_name"],
            sum_cols=metrics,
        )
        for month, df in st.items():
            out_dir = os.path.join(client_dir, "search_terms")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            print(f"    search_terms/{month}.csv ({len(df)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Pull Google Ads data for dashboard")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--months", type=int, default=3, help="Number of months to pull (default: 3)")
    parser.add_argument("--client", default=None, help="Only pull for this client_id")
    args = parser.parse_args()

    config = load_config(args.config)
    client = build_client(config)

    # Date range: first of (N months ago) to yesterday
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = (today.replace(day=1) - timedelta(days=1))  # last day of prev month
    for _ in range(args.months - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1))
    start_date = start_date.replace(day=1)

    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Data directory: {DATA_DIR}\n")

    accounts = get_accounts(client, config)
    if not accounts:
        print("No accounts found!")
        sys.exit(1)

    if args.client:
        accounts = [a for a in accounts if a["client_id"] == args.client]
        if not accounts:
            print(f"No account with client_id '{args.client}'")
            sys.exit(1)

    for acc in accounts:
        print(f"\n{'='*60}")
        print(f"Account: {acc['name']} ({acc['customer_id']}) -> {acc['client_id']}")
        print(f"{'='*60}")

        data = pull_all_data(client, acc["customer_id"], start_date, end_date)
        write_monthly_csvs(data, acc["client_id"], DATA_DIR)

    print(f"\nDone! Data written to {DATA_DIR}/")


if __name__ == "__main__":
    main()
