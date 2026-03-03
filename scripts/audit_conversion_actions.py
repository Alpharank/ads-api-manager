#!/usr/bin/env python3
"""
Audit Google Ads conversion actions for a client account.

Pulls all conversion actions via GAQL including tag snippets and attribution settings.
Outputs JSON (full detail) and CSV (flat summary) to data/{client}/conversion_actions/.

Usage:
    python scripts/audit_conversion_actions.py --client embenauto
"""

import argparse
import csv
import json
import os
import sys

from gads_helpers import (
    ROOT, load_config, build_client, search, get_customer_id, client_data_dir,
)


def audit_conversion_actions(client, customer_id):
    """Pull all non-removed conversion actions with full details."""
    svc = client.get_service("GoogleAdsService")

    query = """
        SELECT
            conversion_action.id,
            conversion_action.name,
            conversion_action.type,
            conversion_action.category,
            conversion_action.status,
            conversion_action.include_in_conversions_metric,
            conversion_action.counting_type,
            conversion_action.tag_snippets,
            conversion_action.attribution_model_settings.attribution_model,
            conversion_action.attribution_model_settings.data_driven_model_status,
            conversion_action.value_settings.default_value,
            conversion_action.value_settings.always_use_default_value,
            conversion_action.click_through_lookback_window_days,
            conversion_action.view_through_lookback_window_days
        FROM conversion_action
        WHERE conversion_action.status != 'REMOVED'
    """

    rows = search(svc, customer_id, query, "conversion_actions")
    actions = []

    for row in rows:
        ca = row.conversion_action

        # Extract tag snippets
        snippets = []
        if ca.tag_snippets:
            for snippet in ca.tag_snippets:
                snippets.append({
                    "type": snippet.type_.name if hasattr(snippet, 'type_') else str(snippet.type),
                    "page_header": snippet.page_header if hasattr(snippet, 'page_header') else "",
                    "event_snippet": snippet.event_snippet if hasattr(snippet, 'event_snippet') else "",
                    "global_site_tag": snippet.global_site_tag if hasattr(snippet, 'global_site_tag') else "",
                })

        action = {
            "id": str(ca.id),
            "name": ca.name,
            "type": ca.type_.name,
            "category": ca.category.name,
            "status": ca.status.name,
            "included_in_conversions": ca.include_in_conversions_metric,
            "counting_type": ca.counting_type.name,
            "attribution_model": ca.attribution_model_settings.attribution_model.name if ca.attribution_model_settings else "UNKNOWN",
            "data_driven_status": ca.attribution_model_settings.data_driven_model_status.name if ca.attribution_model_settings else "UNKNOWN",
            "default_value": ca.value_settings.default_value if ca.value_settings else 0,
            "always_use_default_value": ca.value_settings.always_use_default_value if ca.value_settings else False,
            "click_through_lookback_days": ca.click_through_lookback_window_days,
            "view_through_lookback_days": ca.view_through_lookback_window_days,
            "tag_snippets": snippets,
        }
        actions.append(action)

    return actions


def write_outputs(actions, out_dir):
    """Write audit.json and audit.csv."""
    os.makedirs(out_dir, exist_ok=True)

    # Full JSON
    json_path = os.path.join(out_dir, "audit.json")
    with open(json_path, "w") as f:
        json.dump(actions, f, indent=2)
    print(f"  Written: {json_path} ({len(actions)} actions)")

    # Flat CSV
    csv_path = os.path.join(out_dir, "audit.csv")
    fieldnames = [
        "id", "name", "type", "category", "status",
        "included_in_conversions", "counting_type",
        "attribution_model", "default_value",
        "click_through_lookback_days", "view_through_lookback_days",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(actions)
    print(f"  Written: {csv_path}")

    return json_path, csv_path


def print_audit_table(actions):
    """Print a formatted audit table to console."""
    print(f"\n{'='*100}")
    print(f"{'ID':<12} {'Name':<35} {'Type':<15} {'Category':<20} {'Status':<10} {'In Conv?':<8}")
    print(f"{'='*100}")
    for a in actions:
        print(
            f"{a['id']:<12} "
            f"{a['name'][:34]:<35} "
            f"{a['type']:<15} "
            f"{a['category']:<20} "
            f"{a['status']:<10} "
            f"{'Yes' if a['included_in_conversions'] else 'No':<8}"
        )
    print(f"{'='*100}")
    print(f"Total: {len(actions)} conversion actions\n")


def main():
    parser = argparse.ArgumentParser(description="Audit Google Ads conversion actions")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    gads_client = build_client(config)

    print(f"Auditing conversion actions for {args.client} (CID: {customer_id})...")
    actions = audit_conversion_actions(gads_client, customer_id)

    out_dir = os.path.join(client_data_dir(args.client), "conversion_actions")
    write_outputs(actions, out_dir)
    print_audit_table(actions)


if __name__ == "__main__":
    main()
