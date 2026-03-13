#!/usr/bin/env python3
"""
Pull Meta (Facebook/Instagram) Ads data and write to data/ directory for the dashboard.

Pulls campaign, daily, ad set, placement, and device data from the Meta Marketing API
and writes monthly CSVs in the same format the dashboard expects (matching Google Ads columns).

Usage:
    python scripts/pull_meta_dashboard_data.py                     # Last 3 months
    python scripts/pull_meta_dashboard_data.py --months 6          # Last 6 months
    python scripts/pull_meta_dashboard_data.py --client kitsap_cu  # Specific client only
    python scripts/pull_meta_dashboard_data.py --dry-run            # Show what would be pulled
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import yaml

# Project root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Meta API config
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

# Rate-limit: Meta uses a percentage-based system. We back off when usage > 75%.
RATE_LIMIT_THRESHOLD = 75
RATE_LIMIT_BACKOFF = 60  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

# Conversion action types we count as "conversions" to match Google Ads column.
# Adjust per client if needed — these are the most common for lead-gen / CU verticals.
CONVERSION_ACTION_TYPES = {
    "lead",
    "complete_registration",
    "submit_application",
    "contact",
    "schedule",
    "offsite_conversion.fb_pixel_lead",
    "offsite_conversion.fb_pixel_complete_registration",
    "offsite_conversion.fb_pixel_custom",
}


def load_config(path=None):
    if path is None:
        path = os.path.join(ROOT, "config", "config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def get_meta_clients(config):
    """Return list of clients that have Meta ad account configured."""
    clients_cfg = config.get("clients", config)
    meta_clients = []
    for client_id, client in clients_cfg.items():
        if isinstance(client, dict) and client.get("meta_ad_account_id"):
            token_env = client.get("meta_access_token_env", "META_ACCESS_TOKEN")
            token = os.environ.get(token_env)
            if not token:
                logger.warning(
                    "Skipping %s: env var %s not set", client_id, token_env
                )
                continue
            meta_clients.append({
                "client_id": client_id,
                "name": client.get("name", client_id),
                "ad_account_id": client["meta_ad_account_id"],
                "access_token": token,
            })
    return meta_clients


def _check_rate_limit(response):
    """Check Meta API rate limit headers and sleep if approaching threshold."""
    usage_header = response.headers.get("x-business-use-case-usage") or response.headers.get("x-app-usage")
    if not usage_header:
        return
    try:
        usage = json.loads(usage_header) if isinstance(usage_header, str) else usage_header
        # x-app-usage format: {"call_count": N, "total_cputime": N, "total_time": N}
        if isinstance(usage, dict):
            pct = max(
                usage.get("call_count", 0),
                usage.get("total_cputime", 0),
                usage.get("total_time", 0),
            )
            if pct > RATE_LIMIT_THRESHOLD:
                logger.warning("Rate limit at %d%%, backing off %ds", pct, RATE_LIMIT_BACKOFF)
                time.sleep(RATE_LIMIT_BACKOFF)
    except (json.JSONDecodeError, TypeError):
        pass


def meta_api_get(path, params, access_token, label="data"):
    """Make a GET request to Meta API with retry and rate-limit handling."""
    params["access_token"] = access_token
    url = f"{META_BASE_URL}/{path}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=120)
            _check_rate_limit(resp)

            if resp.status_code == 200:
                return resp.json()

            error = resp.json().get("error", {})
            code = error.get("code", 0)

            # Transient errors: rate limit (32), unknown error (1), service unavailable (2)
            if code in (1, 2, 32, 4):
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(
                    "%s: API error code %d, retrying in %ds (attempt %d/%d)",
                    label, code, wait, attempt + 1, MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            # Non-transient error
            logger.error("%s: Meta API error: %s", label, error.get("message", resp.text))
            return None

        except requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF * (2 ** attempt)
            logger.warning("%s: Request failed (%s), retrying in %ds", label, e, wait)
            time.sleep(wait)

    logger.error("%s: All %d retries exhausted", label, MAX_RETRIES)
    return None


def _paginate_insights(path, params, access_token, label="insights"):
    """Fetch all pages of a Meta Insights response."""
    all_data = []
    result = meta_api_get(path, params, access_token, label)
    while result:
        all_data.extend(result.get("data", []))
        paging = result.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        # next_url is a full URL; extract and re-request
        try:
            resp = requests.get(next_url, timeout=120)
            _check_rate_limit(resp)
            result = resp.json() if resp.status_code == 200 else None
        except requests.exceptions.RequestException:
            break
    return all_data


def _extract_conversions(row):
    """Extract conversion count from Meta 'actions' array."""
    actions = row.get("actions", [])
    total = 0.0
    for action in actions:
        if action.get("action_type") in CONVERSION_ACTION_TYPES:
            total += float(action.get("value", 0))
    return total


def _normalize_row(row, extra_fields=None):
    """Convert a Meta insights row to our standard CSV schema."""
    base = {
        "date": row.get("date_start", ""),
        "impressions": int(row.get("impressions", 0)),
        "clicks": int(row.get("clicks", 0)),
        "cost": round(float(row.get("spend", 0)), 2),
        "conversions": _extract_conversions(row),
    }
    if extra_fields:
        base.update(extra_fields)
    return base


def pull_meta_data(ad_account_id, access_token, start_date, end_date):
    """Pull all data types from Meta API. Returns dict of DataFrames."""
    sd = start_date.strftime("%Y-%m-%d")
    ed = end_date.strftime("%Y-%m-%d")
    base_params = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "time_increment": 1,  # daily
    }

    # --- Campaign-level daily data ---
    logger.info("  Pulling Meta campaign data (%s to %s)...", sd, ed)
    params = {
        **base_params,
        "level": "campaign",
        "fields": "campaign_id,campaign_name,impressions,clicks,spend,actions",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "campaigns")
    daily_rows = []
    for row in raw:
        daily_rows.append(_normalize_row(row, {
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
        }))
    daily_df = pd.DataFrame(daily_rows)

    # --- Ad set level data (Meta's equivalent of ad group / keyword targeting) ---
    logger.info("  Pulling Meta ad set data...")
    params = {
        **base_params,
        "level": "adset",
        "fields": "campaign_id,campaign_name,adset_id,adset_name,impressions,clicks,spend,actions",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "adsets")
    adset_rows = []
    for row in raw:
        adset_rows.append(_normalize_row(row, {
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "adset_id": row.get("adset_id", ""),
            "adset_name": row.get("adset_name", ""),
        }))
    adset_df = pd.DataFrame(adset_rows)

    # --- Placement breakdown (Facebook, Instagram, Audience Network, Messenger) ---
    logger.info("  Pulling Meta placement data...")
    params = {
        **base_params,
        "level": "account",
        "fields": "impressions,clicks,spend,actions",
        "breakdowns": "publisher_platform",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "placements")
    placement_rows = []
    for row in raw:
        placement_rows.append(_normalize_row(row, {
            "platform": row.get("publisher_platform", "unknown"),
        }))
    placement_df = pd.DataFrame(placement_rows)

    # --- Device breakdown ---
    logger.info("  Pulling Meta device data...")
    params = {
        **base_params,
        "level": "account",
        "fields": "impressions,clicks,spend,actions",
        "breakdowns": "device_platform",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "devices")
    device_rows = []
    for row in raw:
        device_rows.append(_normalize_row(row, {
            "device": row.get("device_platform", "unknown"),
        }))
    device_df = pd.DataFrame(device_rows)

    # --- Age/gender breakdown (for demographics view) ---
    logger.info("  Pulling Meta demographics data...")
    params = {
        **base_params,
        "level": "account",
        "fields": "impressions,clicks,spend,actions",
        "breakdowns": "age,gender",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "demographics")
    demo_rows = []
    for row in raw:
        demo_rows.append(_normalize_row(row, {
            "age": row.get("age", "unknown"),
            "gender": row.get("gender", "unknown"),
        }))
    demo_df = pd.DataFrame(demo_rows)

    # --- Region/country breakdown ---
    logger.info("  Pulling Meta location data...")
    params = {
        **base_params,
        "level": "account",
        "fields": "impressions,clicks,spend,actions",
        "breakdowns": "region",
    }
    raw = _paginate_insights(f"{ad_account_id}/insights", params, access_token, "locations")
    location_rows = []
    for row in raw:
        location_rows.append(_normalize_row(row, {
            "location": row.get("region", "unknown"),
        }))
    location_df = pd.DataFrame(location_rows)

    return {
        "daily": daily_df,
        "campaigns": daily_df,  # same data, will be aggregated differently
        "adsets": adset_df,
        "placements": placement_df,
        "devices": device_df,
        "demographics": demo_df,
        "locations": location_df,
    }


def aggregate_monthly(df, date_col="date", group_cols=None, sum_cols=None):
    """Aggregate a dataframe by month. Mirrors pull_dashboard_data.py."""
    if df.empty:
        return {}

    df = df.copy()
    df["_month"] = df[date_col].str[:7]

    months = {}
    for month, group in df.groupby("_month"):
        if group_cols:
            agg = group.groupby(group_cols, as_index=False)[sum_cols].sum()
        else:
            agg = group[sum_cols].sum().to_frame().T if sum_cols else group
        months[month] = agg

    return months


def write_monthly_csvs(data, client_id, data_dir):
    """Write Meta data to monthly CSVs under meta_* subdirectories."""
    client_dir = os.path.join(data_dir, client_id)
    metrics = ["impressions", "clicks", "cost", "conversions"]

    # --- Daily (keep daily granularity, split by month) ---
    if not data["daily"].empty:
        daily = data["daily"].copy()
        daily["_month"] = daily["date"].str[:7]
        for month, group in daily.groupby("_month"):
            out_dir = os.path.join(client_dir, "meta_daily")
            os.makedirs(out_dir, exist_ok=True)
            group.drop(columns=["_month"]).to_csv(
                os.path.join(out_dir, f"{month}.csv"), index=False
            )
            logger.info("    meta_daily/%s.csv (%d rows)", month, len(group))

    # --- Campaigns (aggregate by campaign per month) ---
    if not data["campaigns"].empty:
        campaigns = aggregate_monthly(
            data["campaigns"],
            group_cols=["campaign_id", "campaign_name"],
            sum_cols=metrics,
        )
        for month, df in campaigns.items():
            out_dir = os.path.join(client_dir, "meta_campaigns")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_campaigns/%s.csv (%d rows)", month, len(df))

    # --- Ad sets (aggregate by ad set per month) ---
    if not data["adsets"].empty:
        adsets = aggregate_monthly(
            data["adsets"],
            group_cols=["campaign_id", "campaign_name", "adset_id", "adset_name"],
            sum_cols=metrics,
        )
        for month, df in adsets.items():
            out_dir = os.path.join(client_dir, "meta_adsets")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_adsets/%s.csv (%d rows)", month, len(df))

    # --- Placements (aggregate by platform per month) ---
    if not data["placements"].empty:
        placements = aggregate_monthly(
            data["placements"],
            group_cols=["platform"],
            sum_cols=metrics,
        )
        for month, df in placements.items():
            out_dir = os.path.join(client_dir, "meta_placements")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_placements/%s.csv (%d rows)", month, len(df))

    # --- Devices (aggregate by device per month) ---
    if not data["devices"].empty:
        devices = aggregate_monthly(
            data["devices"],
            group_cols=["device"],
            sum_cols=metrics,
        )
        for month, df in devices.items():
            out_dir = os.path.join(client_dir, "meta_devices")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_devices/%s.csv (%d rows)", month, len(df))

    # --- Demographics (aggregate by age+gender per month) ---
    if not data["demographics"].empty:
        demos = aggregate_monthly(
            data["demographics"],
            group_cols=["age", "gender"],
            sum_cols=metrics,
        )
        for month, df in demos.items():
            out_dir = os.path.join(client_dir, "meta_demographics")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_demographics/%s.csv (%d rows)", month, len(df))

    # --- Locations (aggregate by region per month) ---
    if not data["locations"].empty:
        locs = aggregate_monthly(
            data["locations"],
            group_cols=["location"],
            sum_cols=metrics,
        )
        for month, df in locs.items():
            out_dir = os.path.join(client_dir, "meta_locations")
            os.makedirs(out_dir, exist_ok=True)
            df.to_csv(os.path.join(out_dir, f"{month}.csv"), index=False)
            logger.info("    meta_locations/%s.csv (%d rows)", month, len(df))


def update_data_manifest(client_id):
    """Add meta months to data-manifest.json by scanning local meta_campaigns/ CSVs."""
    manifest_path = os.path.join(ROOT, "data-manifest.json")
    manifest = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

    # Scan for meta campaign months on disk
    meta_campaigns_dir = os.path.join(DATA_DIR, client_id, "meta_campaigns")
    if os.path.isdir(meta_campaigns_dir):
        months = sorted(
            [f.replace(".csv", "") for f in os.listdir(meta_campaigns_dir) if f.endswith(".csv")],
            reverse=True,
        )
        if months:
            meta_key = f"{client_id}__meta"
            manifest[meta_key] = months
            logger.info("  Updated manifest: %s -> %s", meta_key, months)

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Pull Meta Ads data for dashboard")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--months", type=int, default=3, help="Number of months to pull (default: 3)")
    parser.add_argument("--client", default=None, help="Only pull for this client_id")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be pulled without calling API")
    args = parser.parse_args()

    config = load_config(args.config)
    meta_clients = get_meta_clients(config)

    if not meta_clients:
        logger.error("No Meta-enabled clients found. Check clients.yaml for meta_ad_account_id fields.")
        sys.exit(1)

    if args.client:
        meta_clients = [c for c in meta_clients if c["client_id"] == args.client]
        if not meta_clients:
            logger.error("No Meta-enabled client with id '%s'", args.client)
            sys.exit(1)

    # Date range: first of (N months ago) to yesterday
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = today.replace(day=1) - timedelta(days=1)
    for _ in range(args.months - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1))
    start_date = start_date.replace(day=1)

    logger.info("Date range: %s to %s", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    logger.info("Data directory: %s\n", DATA_DIR)

    for client in meta_clients:
        logger.info("=" * 60)
        logger.info("Account: %s -> %s (Meta: %s)", client["name"], client["client_id"], client["ad_account_id"])
        logger.info("=" * 60)

        if args.dry_run:
            logger.info("  [DRY RUN] Would pull: meta_daily, meta_campaigns, meta_adsets, meta_placements, meta_devices, meta_demographics, meta_locations")
            continue

        data = pull_meta_data(client["ad_account_id"], client["access_token"], start_date, end_date)
        write_monthly_csvs(data, client["client_id"], DATA_DIR)
        update_data_manifest(client["client_id"])

    logger.info("\nDone! Meta data written to %s/", DATA_DIR)


if __name__ == "__main__":
    main()
