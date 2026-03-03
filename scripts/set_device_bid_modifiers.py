#!/usr/bin/env python3
"""
Set device bid modifiers for Google Ads campaigns.

Based on Feb 2026 conversion rate data:
  - Mobile: 15.63% CR → +20% bid modifier
  - Desktop: 8.70% CR → baseline (0%)
  - Tablet: 0% CR → -50% bid modifier

Uses CampaignCriterionService with device criterion IDs:
  30000 = MOBILE, 30001 = DESKTOP, 30002 = TABLET

Usage:
    python scripts/set_device_bid_modifiers.py --client embenauto --dry-run
    python scripts/set_device_bid_modifiers.py --client embenauto
"""

import argparse
import sys

from google.api_core import protobuf_helpers
from google.ads.googleads.errors import GoogleAdsException

from gads_helpers import (
    load_config, build_client, get_customer_id, search, log_action,
)

# All embenauto campaigns
ALL_CAMPAIGNS = ["22941603101", "22942113116", "22975956367"]

# Device bid modifiers (bid_modifier = 1.0 + adjustment)
# e.g., +20% = 1.20, -50% = 0.50, no change = 1.0 (but we don't set desktop since it's baseline)
DEVICE_MODIFIERS = {
    "MOBILE": {"criterion_id": 30000, "modifier": 1.20, "label": "+20%"},
    "TABLET": {"criterion_id": 30002, "modifier": 0.50, "label": "-50%"},
}


def get_existing_device_modifiers(svc, customer_id):
    """Pull current device bid modifiers."""
    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign_criterion.device.type,
            campaign_criterion.bid_modifier,
            campaign_criterion.resource_name
        FROM campaign_criterion
        WHERE campaign_criterion.type = 'DEVICE'
            AND campaign.status = 'ENABLED'
    """
    rows = search(svc, customer_id, query, "device_modifiers")
    modifiers = {}
    for row in rows:
        key = f"{row.campaign.id}_{row.campaign_criterion.device.type.name}"
        modifiers[key] = {
            "campaign_id": str(row.campaign.id),
            "campaign_name": row.campaign.name,
            "device": row.campaign_criterion.device.type.name,
            "current_modifier": row.campaign_criterion.bid_modifier,
            "resource_name": row.campaign_criterion.resource_name,
        }
    return modifiers


def apply_device_modifiers(gads_client, customer_id, dry_run=False):
    """Set device bid modifiers on all campaigns."""
    svc = gads_client.get_service("GoogleAdsService")
    existing = get_existing_device_modifiers(svc, customer_id)

    changes = []
    for campaign_id in ALL_CAMPAIGNS:
        for device, config in DEVICE_MODIFIERS.items():
            key = f"{campaign_id}_{device}"
            current = existing.get(key, {})
            current_mod = current.get("current_modifier", 1.0) if current else 1.0
            campaign_name = current.get("campaign_name", campaign_id)

            changes.append({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "device": device,
                "criterion_id": config["criterion_id"],
                "current_modifier": current_mod,
                "new_modifier": config["modifier"],
                "label": config["label"],
                "resource_name": current.get("resource_name"),
            })

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Setting device bid modifiers:\n")
    print(f"  {'Campaign':<25} {'Device':<10} {'Current':<10} {'New':<10} {'Change'}")
    print(f"  {'-'*70}")
    for c in changes:
        current_label = f"{(c['current_modifier'] - 1) * 100:+.0f}%" if c["current_modifier"] != 0 else "unset"
        print(
            f"  {c['campaign_name'][:24]:<25} "
            f"{c['device']:<10} "
            f"{current_label:<10} "
            f"{c['label']:<10} "
            f"{'(no change)' if abs(c['current_modifier'] - c['new_modifier']) < 0.01 else ''}"
        )

    if dry_run:
        print(f"\n  Dry run complete. No changes made.")
        return changes

    criterion_svc = gads_client.get_service("CampaignCriterionService")
    operations = []

    for c in changes:
        op = gads_client.get_type("CampaignCriterionOperation")

        if c["resource_name"]:
            # Update existing device criterion
            criterion = op.update
            criterion.resource_name = c["resource_name"]
            criterion.bid_modifier = c["new_modifier"]
            field_mask = protobuf_helpers.field_mask(None, criterion._pb)
            op.update_mask.CopyFrom(field_mask)
        else:
            # Create new device criterion
            criterion = op.create
            criterion.campaign = gads_client.get_service("CampaignService").campaign_path(
                customer_id, c["campaign_id"]
            )
            criterion.device.type_ = getattr(
                gads_client.enums.DeviceEnum.Device,
                c["device"],
            )
            criterion.bid_modifier = c["new_modifier"]

        operations.append(op)

    try:
        response = criterion_svc.mutate_campaign_criteria(
            customer_id=customer_id,
            operations=operations,
        )
        print(f"\n  Successfully set {len(response.results)} device bid modifier(s)")
        return changes
    except GoogleAdsException as e:
        print(f"\n  ERROR setting device modifiers: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Set device bid modifiers")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    print(f"Setting device bid modifiers for {args.client} (CID: {customer_id})")
    print(f"  Mobile: +20% (15.63% CR in Feb 2026)")
    print(f"  Tablet: -50% (0% CR in Feb 2026)")
    print(f"  Desktop: baseline (8.70% CR)")

    result = apply_device_modifiers(gads_client, customer_id, dry_run=args.dry_run)

    if not args.dry_run and result:
        log_action(args.client, "device_bid_modifiers", {
            "changes": [
                {
                    "campaign": c["campaign_name"],
                    "device": c["device"],
                    "before": c["current_modifier"],
                    "after": c["new_modifier"],
                }
                for c in result
            ],
        })


if __name__ == "__main__":
    main()
