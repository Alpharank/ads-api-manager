#!/usr/bin/env python3
"""
Export all Google Ads account IDs from our MCC.

This script lists all accounts under our MCC so you can fill in the
client_mapping in config.yaml.

Usage:
    python export_account_ids.py

Output will look like:
    Customer ID     | Account Name
    9001645164      | California Coast Credit Union
    1234567890      | Hughes Federal Credit Union
    ...
"""

import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def main():
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Initialize client
    client = GoogleAdsClient.load_from_dict({
        "developer_token": config['google_ads']['developer_token'],
        "client_id": config['google_ads']['client_id'],
        "client_secret": config['google_ads']['client_secret'],
        "refresh_token": config['google_ads']['refresh_token'],
        "login_customer_id": config['google_ads']['login_customer_id'],
        "use_proto_plus": True
    })

    mcc_id = config['google_ads']['login_customer_id']
    customer_service = client.get_service("CustomerService")
    ga_service = client.get_service("GoogleAdsService")

    # Get accessible customers
    accessible = customer_service.list_accessible_customers()

    print()
    print("=" * 70)
    print("Google Ads Accounts Under Our MCC")
    print("=" * 70)
    print()
    print(f"{'Customer ID':<15} | {'Account Name':<50}")
    print("-" * 70)

    accounts = []

    for resource_name in accessible.resource_names:
        customer_id = resource_name.split('/')[-1]

        if customer_id == mcc_id:
            continue

        try:
            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.manager
                FROM customer
                LIMIT 1
            """
            response = ga_service.search(customer_id=customer_id, query=query)

            for row in response:
                if row.customer.manager:
                    continue  # Skip manager accounts

                accounts.append({
                    'id': str(row.customer.id),
                    'name': row.customer.descriptive_name
                })
                print(f"{row.customer.id:<15} | {row.customer.descriptive_name[:50]:<50}")

        except GoogleAdsException as e:
            print(f"{customer_id:<15} | [ACCESS DENIED]")

    print()
    print("=" * 70)
    print(f"Total: {len(accounts)} accounts")
    print()
    print("Copy the Customer IDs above to the config.yaml client_mapping section.")
    print()

    # Also output as YAML format for easy copy-paste
    print("YAML format for config.yaml:")
    print("-" * 40)
    print("client_mapping:")
    for acc in accounts:
        # Generate a suggested client_id from the name
        suggested_id = acc['name'].lower().replace(' ', '_').replace('-', '_')
        suggested_id = ''.join(c for c in suggested_id if c.isalnum() or c == '_')[:30]
        print(f'  "{acc["id"]}": "{suggested_id}"  # {acc["name"][:40]}')


if __name__ == '__main__':
    main()
