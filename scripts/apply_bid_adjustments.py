#!/usr/bin/env python3
"""
Apply bid adjustments to Google Ads keywords via the API.

Reads bid_adjustments.csv, looks up each keyword's resource name via GAQL,
calculates a new bid (midpoint between current and market, capped at max increase),
and updates via AdGroupCriterionService.

Usage:
    python scripts/apply_bid_adjustments.py --client embenauto --dry-run
    python scripts/apply_bid_adjustments.py --client embenauto --max-increase 0.50
    python scripts/apply_bid_adjustments.py --client embenauto
"""

import argparse
import csv
import os
import sys

from google.api_core import protobuf_helpers
from google.ads.googleads.errors import GoogleAdsException

from gads_helpers import (
    load_config, build_client, get_customer_id, client_data_dir,
    search, log_action,
)


def load_bid_adjustments(client_slug):
    """Load bid adjustment recommendations from CSV."""
    csv_path = os.path.join(client_data_dir(client_slug), "recommendations", "bid_adjustments.csv")
    if not os.path.exists(csv_path):
        print(f"  No bid_adjustments.csv found at {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def find_keyword_criterion(svc, customer_id, keyword_text):
    """Look up a keyword's resource name and current bid via GAQL."""
    # Escape single quotes in keyword text
    escaped = keyword_text.replace("'", "\\'")
    query = f"""
        SELECT
            ad_group_criterion.resource_name,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.effective_cpc_bid_micros,
            ad_group_criterion.cpc_bid_micros,
            ad_group.id,
            ad_group.name,
            campaign.id,
            campaign.name
        FROM keyword_view
        WHERE ad_group_criterion.keyword.text = '{escaped}'
            AND ad_group_criterion.status != 'REMOVED'
            AND campaign.status = 'ENABLED'
    """
    rows = search(svc, customer_id, query, f"keyword:{keyword_text}")
    if not rows:
        return []

    results = []
    for row in rows:
        results.append({
            "resource_name": row.ad_group_criterion.resource_name,
            "keyword": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "current_bid_micros": row.ad_group_criterion.cpc_bid_micros,
            "effective_bid_micros": row.ad_group_criterion.effective_cpc_bid_micros,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
        })
    return results


def calculate_new_bid(current_cpc, market_cpc, max_increase_pct):
    """
    Calculate new bid: midpoint between current and market CPC,
    capped at max_increase_pct above current.
    """
    midpoint = (current_cpc + market_cpc) / 2
    max_bid = current_cpc * (1 + max_increase_pct)
    new_bid = min(midpoint, max_bid)
    # Never decrease below current
    return max(new_bid, current_cpc)


def apply_bids(gads_client, customer_id, changes, dry_run=False):
    """Apply bid changes via AdGroupCriterionService."""
    if not changes:
        print("\n  No bid changes to apply.")
        return []

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Applying {len(changes)} bid change(s):\n")
    print(f"  {'Keyword':<30} {'Campaign':<20} {'Current':<10} {'New':<10} {'Market':<10} {'Change':<8}")
    print(f"  {'-'*88}")

    applied = []
    for c in changes:
        pct_change = ((c["new_cpc"] - c["current_cpc"]) / c["current_cpc"] * 100) if c["current_cpc"] > 0 else 0
        print(
            f"  {c['keyword'][:29]:<30} "
            f"{c['campaign_name'][:19]:<20} "
            f"${c['current_cpc']:<9.2f} "
            f"${c['new_cpc']:<9.2f} "
            f"${c['market_cpc']:<9.2f} "
            f"+{pct_change:.0f}%"
        )
        applied.append(c)

    if dry_run:
        print(f"\n  Dry run complete. No changes made.")
        return applied

    svc = gads_client.get_service("AdGroupCriterionService")
    operations = []

    for c in changes:
        op = gads_client.get_type("AdGroupCriterionOperation")
        criterion = op.update
        criterion.resource_name = c["resource_name"]
        criterion.cpc_bid_micros = int(c["new_cpc"] * 1_000_000)

        field_mask = protobuf_helpers.field_mask(None, criterion._pb)
        op.update_mask.CopyFrom(field_mask)
        operations.append(op)

    try:
        response = svc.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations,
        )
        print(f"\n  Successfully updated {len(response.results)} keyword bid(s)")
        return applied
    except GoogleAdsException as e:
        print(f"\n  ERROR applying bids: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Apply bid adjustments from recommendations")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    parser.add_argument("--max-increase", type=float, default=0.50,
                        help="Max bid increase as decimal (default: 0.50 = 50%%)")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    adjustments = load_bid_adjustments(args.client)
    print(f"Loaded {len(adjustments)} bid adjustment recommendation(s) for {args.client}\n")

    svc = gads_client.get_service("GoogleAdsService")
    changes = []

    for adj in adjustments:
        keyword = adj["keyword"]
        current_cpc = float(adj["current_cpc"])
        market_cpc = float(adj["market_cpc"])

        criteria = find_keyword_criterion(svc, customer_id, keyword)
        if not criteria:
            print(f"  Keyword not found: '{keyword}' — skipping")
            continue

        new_cpc = calculate_new_bid(current_cpc, market_cpc, args.max_increase)

        for crit in criteria:
            changes.append({
                "keyword": keyword,
                "resource_name": crit["resource_name"],
                "campaign_name": crit["campaign_name"],
                "ad_group_name": crit["ad_group_name"],
                "current_cpc": current_cpc,
                "new_cpc": round(new_cpc, 2),
                "market_cpc": market_cpc,
            })

    result = apply_bids(gads_client, customer_id, changes, dry_run=args.dry_run)

    if not args.dry_run and result:
        log_action(args.client, "bid_adjustments", {
            "count": len(result),
            "max_increase_pct": args.max_increase,
            "changes": [
                {
                    "keyword": c["keyword"],
                    "campaign": c["campaign_name"],
                    "before": c["current_cpc"],
                    "after": c["new_cpc"],
                    "market": c["market_cpc"],
                }
                for c in result
            ],
        })


if __name__ == "__main__":
    main()
