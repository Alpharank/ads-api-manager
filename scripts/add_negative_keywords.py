#!/usr/bin/env python3
"""
Add negative keywords to Google Ads campaigns.

Reads negative_keywords.csv, deduplicates against existing 713+ negatives,
and creates via CampaignCriterionService with negative=True.

Usage:
    python scripts/add_negative_keywords.py --client embenauto --dry-run
    python scripts/add_negative_keywords.py --client embenauto
"""

import argparse
import csv
import os
import sys

from google.ads.googleads.errors import GoogleAdsException

from gads_helpers import (
    load_config, build_client, get_customer_id, client_data_dir,
    search, log_action,
)

# Default campaign to add account-wide negatives to (all 3 campaigns)
ALL_CAMPAIGNS = ["22941603101", "22942113116", "22975956367"]


def load_negative_recommendations(client_slug):
    """Load negative keyword recommendations from CSV."""
    csv_path = os.path.join(client_data_dir(client_slug), "recommendations", "negative_keywords.csv")
    if not os.path.exists(csv_path):
        print(f"  No negative_keywords.csv found at {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def get_existing_negatives(svc, customer_id):
    """Pull all existing negative keywords (ad group + campaign level)."""
    existing = set()

    # Ad group level negatives
    q1 = """
        SELECT
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type
        FROM ad_group_criterion
        WHERE ad_group_criterion.negative = TRUE
            AND ad_group_criterion.type = 'KEYWORD'
            AND ad_group_criterion.status = 'ENABLED'
    """
    for row in search(svc, customer_id, q1, "ag_negatives"):
        existing.add(row.ad_group_criterion.keyword.text.lower())

    # Campaign level negatives
    q2 = """
        SELECT
            campaign_criterion.keyword.text,
            campaign_criterion.keyword.match_type
        FROM campaign_criterion
        WHERE campaign_criterion.negative = TRUE
            AND campaign_criterion.type = 'KEYWORD'
    """
    for row in search(svc, customer_id, q2, "campaign_negatives"):
        existing.add(row.campaign_criterion.keyword.text.lower())

    return existing


def add_negatives(gads_client, customer_id, negatives_to_add, dry_run=False):
    """Add negative keywords at campaign level via CampaignCriterionService."""
    if not negatives_to_add:
        print("\n  No negative keywords to add.")
        return []

    # Each negative is added to all campaigns
    total_ops = len(negatives_to_add) * len(ALL_CAMPAIGNS)

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Adding {len(negatives_to_add)} negative keyword(s) to {len(ALL_CAMPAIGNS)} campaigns ({total_ops} operations):\n")
    for neg in negatives_to_add:
        print(f"    - \"{neg['term']}\" ({neg['reason']}, matched: {neg.get('matched_keyword', 'N/A')})")

    if dry_run:
        print(f"\n  Dry run complete. No changes made.")
        return negatives_to_add

    svc = gads_client.get_service("CampaignCriterionService")
    operations = []

    for neg in negatives_to_add:
        for campaign_id in ALL_CAMPAIGNS:
            op = gads_client.get_type("CampaignCriterionOperation")
            criterion = op.create
            criterion.campaign = gads_client.get_service("CampaignService").campaign_path(
                customer_id, campaign_id
            )
            criterion.negative = True
            criterion.keyword.text = neg["term"]
            criterion.keyword.match_type = gads_client.enums.KeywordMatchTypeEnum.KeywordMatchType.PHRASE
            operations.append(op)

    try:
        response = svc.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations,
        )
        print(f"\n  Successfully added {len(response.results)} negative keyword entries")
        return negatives_to_add
    except GoogleAdsException as e:
        print(f"\n  ERROR adding negative keywords: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Add negative keywords from recommendations")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    recommendations = load_negative_recommendations(args.client)
    print(f"Loaded {len(recommendations)} negative keyword recommendation(s) for {args.client}\n")

    # Get existing negatives for dedup
    svc = gads_client.get_service("GoogleAdsService")
    existing = get_existing_negatives(svc, customer_id)
    print(f"  Found {len(existing)} existing negative keywords in account")

    # Deduplicate
    negatives_to_add = []
    for rec in recommendations:
        term = rec["term"]
        if term.lower() in existing:
            print(f"  Skip (already exists): \"{term}\"")
            continue
        negatives_to_add.append(rec)

    print(f"  {len(negatives_to_add)} new negatives after deduplication")

    result = add_negatives(gads_client, customer_id, negatives_to_add, dry_run=args.dry_run)

    if not args.dry_run and result:
        log_action(args.client, "add_negative_keywords", {
            "count": len(result),
            "campaigns": ALL_CAMPAIGNS,
            "negatives": [
                {"term": n["term"], "reason": n["reason"]}
                for n in result
            ],
        })


if __name__ == "__main__":
    main()
