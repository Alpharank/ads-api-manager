#!/usr/bin/env python3
"""
Google Ads to S3 Pipeline

Pulls campaign metrics, keyword metrics, and GCLID click data from Google Ads
and uploads to S3 for all accounts under an MCC.

Usage:
    python google_ads_to_s3.py                    # Pull yesterday's data
    python google_ads_to_s3.py --backfill 90      # Backfill last 90 days
    python google_ads_to_s3.py --date 2024-01-15  # Pull specific date
"""

import argparse
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, List, Optional

import boto3
import pandas as pd
import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoogleAdsToS3:
    """Main class to pull Google Ads data and upload to S3."""

    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize with configuration file."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Initialize Google Ads client
        self.google_ads_client = GoogleAdsClient.load_from_dict({
            "developer_token": self.config['google_ads']['developer_token'],
            "client_id": self.config['google_ads']['client_id'],
            "client_secret": self.config['google_ads']['client_secret'],
            "refresh_token": self.config['google_ads']['refresh_token'],
            "login_customer_id": self.config['google_ads']['login_customer_id'],
            "use_proto_plus": True
        })

        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=self.config['aws']['region'])
        self.bucket = self.config['aws']['bucket']
        self.prefix = self.config['aws']['prefix']

        # Client ID mapping
        self.client_mapping = self.config.get('client_mapping', {})

    def get_accessible_accounts(self) -> List[Dict]:
        """Get all child customer accounts under the MCC using CustomerClient."""
        mcc_id = self.config['google_ads']['login_customer_id']
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        # Query the MCC for all child accounts
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
            response = ga_service.search(customer_id=mcc_id, query=query)

            for row in response:
                customer_id = str(row.customer_client.id)
                accounts.append({
                    'customer_id': customer_id,
                    'name': row.customer_client.descriptive_name,
                    'client_id': self.client_mapping.get(
                        customer_id,
                        f"account_{customer_id}"
                    )
                })
        except GoogleAdsException as e:
            logger.error(f"Error listing accounts from MCC {mcc_id}: {e}")

        logger.info(f"Found {len(accounts)} accessible accounts")
        return accounts

    def pull_campaign_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull campaign performance data for a specific date."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM campaign
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                rows.append({
                    'date': row.segments.date,
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'campaign_status': row.campaign.status.name,
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1_000_000,  # Convert to dollars
                    'conversions': row.metrics.conversions
                })
        except GoogleAdsException as e:
            logger.error(f"Error pulling campaign data for {customer_id}: {e}")

        return pd.DataFrame(rows)

    def pull_keyword_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull keyword performance data for a specific date."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM keyword_view
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                rows.append({
                    'date': row.segments.date,
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': row.ad_group.name,
                    'keyword': row.ad_group_criterion.keyword.text,
                    'match_type': row.ad_group_criterion.keyword.match_type.name,
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1_000_000,
                    'conversions': row.metrics.conversions
                })
        except GoogleAdsException as e:
            # keyword_view might not exist for display campaigns, etc.
            logger.warning(f"Error pulling keyword data for {customer_id}: {e}")

        return pd.DataFrame(rows)

    def pull_click_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull click-level data with GCLIDs for a specific date."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                click_view.gclid,
                click_view.area_of_interest.city,
                click_view.area_of_interest.region,
                click_view.area_of_interest.country,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                segments.date,
                segments.ad_network_type
            FROM click_view
            WHERE segments.date = '{date}'
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                rows.append({
                    'date': row.segments.date,
                    'gclid': row.click_view.gclid,
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': row.ad_group.name,
                    'network': row.segments.ad_network_type.name,
                    'city': row.click_view.area_of_interest.city if row.click_view.area_of_interest else None,
                    'region': row.click_view.area_of_interest.region if row.click_view.area_of_interest else None,
                    'country': row.click_view.area_of_interest.country if row.click_view.area_of_interest else None
                })
        except GoogleAdsException as e:
            logger.error(f"Error pulling click data for {customer_id}: {e}")

        return pd.DataFrame(rows)

    def upload_to_s3(self, df: pd.DataFrame, client_id: str, data_type: str, date: str):
        """Upload dataframe to S3 as CSV."""
        if df.empty:
            logger.info(f"No {data_type} data for {client_id} on {date}, skipping upload")
            return

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        key = f"{self.prefix}/{data_type}/{client_id}/{date}.csv"

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        logger.info(f"Uploaded: s3://{self.bucket}/{key} ({len(df)} rows)")

    def process_account(self, account: Dict, date: str):
        """Process a single account for a specific date."""
        customer_id = account['customer_id']
        client_id = account['client_id']

        logger.info(f"Processing {account['name']} ({client_id}) for {date}")

        # Pull and upload campaign data
        campaign_df = self.pull_campaign_data(customer_id, date)
        self.upload_to_s3(campaign_df, client_id, 'campaigns', date)

        # Pull and upload keyword data
        keyword_df = self.pull_keyword_data(customer_id, date)
        self.upload_to_s3(keyword_df, client_id, 'keywords', date)

        # Pull and upload click/GCLID data
        click_df = self.pull_click_data(customer_id, date)
        self.upload_to_s3(click_df, client_id, 'clicks', date)

        return {
            'client_id': client_id,
            'campaigns': len(campaign_df),
            'keywords': len(keyword_df),
            'clicks': len(click_df)
        }

    def run(self, start_date: str, end_date: Optional[str] = None, client_filter: Optional[str] = None):
        """Run the pipeline for a date range."""
        if end_date is None:
            end_date = start_date

        # Get all accounts
        accounts = self.get_accessible_accounts()

        if not accounts:
            logger.error("No accessible accounts found. Check the MCC configuration.")
            return

        # Filter to specific client if requested
        if client_filter:
            accounts = [a for a in accounts if a['client_id'] == client_filter]
            if not accounts:
                logger.error(f"No account found with client_id '{client_filter}'")
                return
            logger.info(f"Filtered to: {accounts[0]['name']} ({client_filter})")

        # Process each date
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            logger.info(f"=== Processing date: {date_str} ===")

            for account in accounts:
                try:
                    result = self.process_account(account, date_str)
                    logger.info(f"  {result['client_id']}: {result['campaigns']} campaigns, "
                                f"{result['keywords']} keywords, {result['clicks']} clicks")
                except Exception as e:
                    logger.error(f"  Error processing {account['client_id']}: {e}")

            current += timedelta(days=1)

        logger.info("Pipeline complete!")


def main():
    parser = argparse.ArgumentParser(description='Google Ads to S3 Pipeline')
    parser.add_argument('--config', default='config/config.yaml', help='Path to config file')
    parser.add_argument('--date', help='Specific date to pull (YYYY-MM-DD)')
    parser.add_argument('--backfill', type=int, help='Number of days to backfill')
    parser.add_argument('--list-accounts', action='store_true', help='List all accessible accounts')
    parser.add_argument('--client', help='Only process this client_id (e.g., californiacoast_cu)')

    args = parser.parse_args()

    pipeline = GoogleAdsToS3(args.config)

    if args.list_accounts:
        accounts = pipeline.get_accessible_accounts()
        print("\nAccessible Accounts:")
        print("-" * 60)
        for acc in accounts:
            print(f"  {acc['customer_id']} | {acc['name'][:40]:<40} | {acc['client_id']}")
        return

    # Determine date range
    if args.date:
        start_date = args.date
        end_date = args.date
    elif args.backfill:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.backfill)).strftime('%Y-%m-%d')
    else:
        # Default: yesterday
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = start_date

    pipeline.run(start_date, end_date, client_filter=args.client)


if __name__ == '__main__':
    main()
