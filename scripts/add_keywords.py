#!/usr/bin/env python3
"""
Add keyword opportunities to Google Ads campaigns.

Reads keyword_opportunities.csv, routes keywords to the appropriate campaign
by intent, filters out competitor brand names and irrelevant terms,
and creates via AdGroupCriterionService.

Campaign routing (based on existing account structure):
  - "canada" or "snowbird" → Canada-US campaign (22941603101)
  - "quote", "cost", "price", "calculator", "estimate" → MOF campaign (22975956367)
  - Default → TOF US-US Broad campaign (22942113116)

Usage:
    python scripts/add_keywords.py --client embenauto --dry-run
    python scripts/add_keywords.py --client embenauto --max 20 --match-type BROAD
    python scripts/add_keywords.py --client embenauto
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

# Competitor brand names to filter out
COMPETITOR_BRANDS = {
    "montway", "sherpa", "nexus", "a1 auto", "a-1 auto", "mercury auto",
    "amerifreight", "ship a car direct", "uship",
}

# Campaign routing rules (campaign_id → ad_group_id)
CAMPAIGN_ROUTES = {
    "canada_us": {"campaign_id": "22941603101", "ad_group_id": "183793632959", "name": "Canada-US / TOF"},
    "mof": {"campaign_id": "22975956367", "ad_group_id": "186415713044", "name": "MOF - US to US"},
    "tof": {"campaign_id": "22942113116", "ad_group_id": "189741124772", "name": "TOF US-US Broad / TOF-US"},
}

MOF_SIGNALS = {"quote", "cost", "price", "calculator", "estimate", "how much", "rates", "pricing"}
CANADA_SIGNALS = {"canada", "snowbird", "canadian"}


def load_opportunities(client_slug):
    """Load keyword opportunities from CSV."""
    csv_path = os.path.join(client_data_dir(client_slug), "recommendations", "keyword_opportunities.csv")
    if not os.path.exists(csv_path):
        print(f"  No keyword_opportunities.csv found at {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def is_competitor_brand(keyword):
    """Check if keyword contains a competitor brand name."""
    kw_lower = keyword.lower()
    return any(brand in kw_lower for brand in COMPETITOR_BRANDS)


def route_keyword(keyword):
    """Determine which campaign/ad group a keyword should go to."""
    kw_lower = keyword.lower()

    if any(signal in kw_lower for signal in CANADA_SIGNALS):
        return CAMPAIGN_ROUTES["canada_us"]

    if any(signal in kw_lower for signal in MOF_SIGNALS):
        return CAMPAIGN_ROUTES["mof"]

    return CAMPAIGN_ROUTES["tof"]


def get_existing_keywords(svc, customer_id):
    """Pull all active keywords to deduplicate."""
    query = """
        SELECT
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            campaign.id
        FROM keyword_view
        WHERE ad_group_criterion.status != 'REMOVED'
            AND campaign.status = 'ENABLED'
    """
    rows = search(svc, customer_id, query, "existing_keywords")
    existing = set()
    for row in rows:
        existing.add(row.ad_group_criterion.keyword.text.lower())
    return existing


def add_keywords(gads_client, customer_id, keywords_to_add, match_type, dry_run=False):
    """Create keywords via AdGroupCriterionService."""
    if not keywords_to_add:
        print("\n  No keywords to add.")
        return []

    # Group by campaign for display
    by_campaign = {}
    for kw in keywords_to_add:
        route = kw["route"]
        by_campaign.setdefault(route["name"], []).append(kw)

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Adding {len(keywords_to_add)} keyword(s) as {match_type}:\n")
    for campaign_name, kws in by_campaign.items():
        print(f"  {campaign_name} ({len(kws)} keywords):")
        for kw in kws:
            print(f"    + {kw['keyword']:<40} vol={kw['volume']:<7} cpc=${kw['cpc']}")
        print()

    if dry_run:
        print(f"  Dry run complete. No changes made.")
        return keywords_to_add

    svc = gads_client.get_service("AdGroupCriterionService")
    match_enum = getattr(
        gads_client.enums.KeywordMatchTypeEnum.KeywordMatchType,
        match_type,
    )

    operations = []
    for kw in keywords_to_add:
        route = kw["route"]
        op = gads_client.get_type("AdGroupCriterionOperation")
        criterion = op.create
        criterion.ad_group = gads_client.get_service("AdGroupService").ad_group_path(
            customer_id, route["ad_group_id"]
        )
        criterion.keyword.text = kw["keyword"]
        criterion.keyword.match_type = match_enum
        criterion.status = gads_client.enums.AdGroupCriterionStatusEnum.AdGroupCriterionStatus.ENABLED
        operations.append(op)

    try:
        response = svc.mutate_ad_group_criteria(
            customer_id=customer_id,
            operations=operations,
        )
        print(f"\n  Successfully added {len(response.results)} keyword(s)")
        return keywords_to_add
    except GoogleAdsException as e:
        print(f"\n  ERROR adding keywords: {e}")
        # Try one at a time to identify which keywords fail
        if len(operations) > 1:
            print("  Retrying individually...")
            added = []
            for i, op in enumerate(operations):
                try:
                    svc.mutate_ad_group_criteria(
                        customer_id=customer_id,
                        operations=[op],
                    )
                    added.append(keywords_to_add[i])
                    print(f"    Added: {keywords_to_add[i]['keyword']}")
                except GoogleAdsException as e2:
                    print(f"    FAILED: {keywords_to_add[i]['keyword']} — {e2}")
            return added
        return []


def main():
    parser = argparse.ArgumentParser(description="Add keyword opportunities from recommendations")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    parser.add_argument("--max", type=int, default=None, help="Max keywords to add (default: all)")
    parser.add_argument("--match-type", default="BROAD",
                        choices=["BROAD", "PHRASE", "EXACT"],
                        help="Match type for new keywords (default: BROAD)")
    parser.add_argument("--priority", default=None, choices=["high", "medium"],
                        help="Only add keywords of this priority (default: all)")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    opportunities = load_opportunities(args.client)
    print(f"Loaded {len(opportunities)} keyword opportunities for {args.client}\n")

    # Get existing keywords for dedup
    svc = gads_client.get_service("GoogleAdsService")
    existing = get_existing_keywords(svc, customer_id)
    print(f"  Found {len(existing)} existing keywords in account\n")

    # Filter and route
    keywords_to_add = []
    skipped = {"brand": 0, "exists": 0, "priority": 0}

    for opp in opportunities:
        keyword = opp["keyword"]

        # Filter by priority
        if args.priority and opp.get("priority", "").lower() != args.priority:
            skipped["priority"] += 1
            continue

        # Filter competitor brands
        if is_competitor_brand(keyword):
            print(f"  Skip (competitor brand): {keyword}")
            skipped["brand"] += 1
            continue

        # Deduplicate
        if keyword.lower() in existing:
            print(f"  Skip (already exists): {keyword}")
            skipped["exists"] += 1
            continue

        route = route_keyword(keyword)
        keywords_to_add.append({
            "keyword": keyword,
            "volume": opp.get("volume", "0"),
            "cpc": opp.get("cpc", "0"),
            "source": opp.get("source", ""),
            "route": route,
        })

    print(f"\n  Filtered: {skipped['brand']} competitor brands, {skipped['exists']} duplicates, {skipped['priority']} priority mismatch")

    # Apply max limit
    if args.max and len(keywords_to_add) > args.max:
        keywords_to_add = keywords_to_add[:args.max]
        print(f"  Limited to first {args.max} keywords")

    result = add_keywords(gads_client, customer_id, keywords_to_add, args.match_type, dry_run=args.dry_run)

    if not args.dry_run and result:
        log_action(args.client, "add_keywords", {
            "count": len(result),
            "match_type": args.match_type,
            "keywords": [
                {
                    "keyword": kw["keyword"],
                    "campaign": kw["route"]["name"],
                    "volume": kw["volume"],
                    "cpc": kw["cpc"],
                }
                for kw in result
            ],
        })


if __name__ == "__main__":
    main()
