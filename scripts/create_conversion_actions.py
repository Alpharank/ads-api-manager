#!/usr/bin/env python3
"""
Create missing Google Ads conversion actions for a client account.

Compares existing actions (from audit.json) against the 4 required actions
and creates any that don't exist.

Usage:
    python scripts/create_conversion_actions.py --client embenauto --dry-run
    python scripts/create_conversion_actions.py --client embenauto
"""

import argparse
import json
import os
import sys

from google.api_core import protobuf_helpers

from gads_helpers import (
    load_config, build_client, get_customer_id, client_data_dir, log_action,
)

# Required conversion actions for vehicle transport lead gen
REQUIRED_ACTIONS = [
    {
        "name": "Free Quote Form Submission",
        "type": "WEBPAGE",
        "category": "SUBMIT_LEAD_FORM",
        "counting_type": "ONE_PER_CLICK",
        "default_value": 50.0,
        "click_through_lookback_days": 30,
        "view_through_lookback_days": 1,
    },
    {
        "name": "Phone Call from Ad",
        "type": "PHONE_CALL",
        "category": "PHONE_CALL_LEAD",
        "counting_type": "ONE_PER_CLICK",
        "default_value": 25.0,
        "click_through_lookback_days": 30,
        "view_through_lookback_days": 1,
    },
    {
        "name": "Phone Call from Website",
        "type": "WEBPAGE",
        "category": "PHONE_CALL_LEAD",
        "counting_type": "ONE_PER_CLICK",
        "default_value": 25.0,
        "click_through_lookback_days": 30,
        "view_through_lookback_days": 1,
    },
    {
        "name": "Thank You Page View",
        "type": "WEBPAGE",
        "category": "PAGE_VIEW",
        "counting_type": "ONE_PER_CLICK",
        "default_value": 10.0,
        "click_through_lookback_days": 30,
        "view_through_lookback_days": 1,
    },
]


def load_existing_actions(client_slug):
    """Load existing conversion actions from audit.json."""
    audit_path = os.path.join(client_data_dir(client_slug), "conversion_actions", "audit.json")
    if not os.path.exists(audit_path):
        print(f"  No audit.json found at {audit_path}")
        print(f"  Run: python scripts/audit_conversion_actions.py --client {client_slug}")
        sys.exit(1)
    with open(audit_path) as f:
        return json.load(f)


def find_missing_actions(existing):
    """Compare required vs existing actions. Returns list of actions to create."""
    existing_names = {a["name"].lower().strip() for a in existing}
    missing = []
    for req in REQUIRED_ACTIONS:
        if req["name"].lower().strip() not in existing_names:
            missing.append(req)
    return missing


def create_actions(gads_client, customer_id, actions_to_create, dry_run=False):
    """Create conversion actions via ConversionActionService."""
    if not actions_to_create:
        print("\n  All required conversion actions already exist!")
        return []

    print(f"\n  {'[DRY RUN] ' if dry_run else ''}Creating {len(actions_to_create)} conversion action(s):\n")
    for a in actions_to_create:
        print(f"    - {a['name']} ({a['type']}, {a['category']}, default=${a['default_value']:.0f})")

    if dry_run:
        print("\n  Dry run complete. No changes made.")
        return actions_to_create

    svc = gads_client.get_service("ConversionActionService")
    operations = []

    for action_def in actions_to_create:
        op = gads_client.get_type("ConversionActionOperation")
        ca = op.create

        ca.name = action_def["name"]
        ca.type_ = getattr(
            gads_client.enums.ConversionActionTypeEnum.ConversionActionType,
            action_def["type"],
        )
        ca.category = getattr(
            gads_client.enums.ConversionActionCategoryEnum.ConversionActionCategory,
            action_def["category"],
        )
        ca.counting_type = getattr(
            gads_client.enums.ConversionActionCountingTypeEnum.ConversionActionCountingType,
            action_def["counting_type"],
        )
        ca.status = gads_client.enums.ConversionActionStatusEnum.ConversionActionStatus.ENABLED
        ca.value_settings.default_value = action_def["default_value"]
        ca.value_settings.always_use_default_value = False
        ca.click_through_lookback_window_days = action_def["click_through_lookback_days"]
        ca.view_through_lookback_window_days = action_def["view_through_lookback_days"]

        operations.append(op)

    try:
        response = svc.mutate_conversion_actions(
            customer_id=customer_id,
            operations=operations,
        )
        created = []
        for result in response.results:
            print(f"  Created: {result.resource_name}")
            created.append(result.resource_name)
        return created
    except Exception as e:
        print(f"  ERROR creating conversion actions: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Create missing conversion actions")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making changes")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    print(f"Checking conversion actions for {args.client} (CID: {customer_id})...")

    existing = load_existing_actions(args.client)
    print(f"  Found {len(existing)} existing conversion action(s)")

    missing = find_missing_actions(existing)
    if not missing:
        print("  All 4 required conversion actions already exist.")
        return

    print(f"  Missing {len(missing)} required action(s)")
    result = create_actions(gads_client, customer_id, missing, dry_run=args.dry_run)

    if not args.dry_run and result:
        log_action(args.client, "create_conversion_actions", {
            "created": [a["name"] for a in missing],
            "resource_names": result,
        })
        print(f"\n  Re-run audit to refresh: python scripts/audit_conversion_actions.py --client {args.client}")


if __name__ == "__main__":
    main()
