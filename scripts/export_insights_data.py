#!/usr/bin/env python3
"""
Export additional Google Ads insights data for the dashboard.

This script exports:
- Search terms: What users actually searched for that triggered ads
- Channel data: Performance by network (Search, Display, YouTube, etc.)
- Device data: Performance by device type (Mobile, Desktop, Tablet)
- Location data: Performance by geographic location

Usage:
    python scripts/export_insights_data.py                    # Pull yesterday's data
    python scripts/export_insights_data.py --backfill 30      # Backfill last 30 days
    python scripts/export_insights_data.py --date 2026-01-15  # Pull specific date
    python scripts/export_insights_data.py --client kitsap_cu # Specific client only
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    sys.exit("google-ads package required. Install with: pip install google-ads")

try:
    import boto3
except ImportError:
    boto3 = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


class InsightsExporter:
    """Export additional Google Ads insights data."""

    def __init__(self, config_path: str = None):
        """Initialize with configuration file."""
        if config_path is None:
            config_path = PROJECT_ROOT / "config" / "config.yaml"

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

        # S3 client (optional)
        self.s3_client = None
        if boto3 and self.config.get('aws', {}).get('bucket'):
            self.s3_client = boto3.client('s3', region_name=self.config['aws'].get('region', 'us-east-1'))
            self.bucket = self.config['aws']['bucket']
            self.prefix = self.config['aws'].get('prefix', 'ad-spend-reports')

        self.client_mapping = self.config.get('client_mapping', {})

    def get_accessible_accounts(self) -> List[Dict]:
        """Get all child customer accounts under the MCC."""
        mcc_id = self.config['google_ads']['login_customer_id']
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

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
                    'client_id': self.client_mapping.get(customer_id, f"account_{customer_id}")
                })
        except GoogleAdsException as e:
            logger.error(f"Error listing accounts from MCC {mcc_id}: {e}")

        logger.info(f"Found {len(accounts)} accessible accounts")
        return accounts

    def pull_search_terms(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull search terms report for a specific date."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                search_term_view.search_term,
                segments.keyword.info.text,
                segments.keyword.info.match_type,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM search_term_view
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                rows.append({
                    'date': row.segments.date,
                    'search_term': row.search_term_view.search_term,
                    'keyword': row.segments.keyword.info.text if row.segments.keyword.info.text else '',
                    'match_type': row.segments.keyword.info.match_type.name if row.segments.keyword.info.match_type else '',
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': row.ad_group.name,
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1_000_000,
                    'conversions': row.metrics.conversions
                })
        except GoogleAdsException as e:
            logger.warning(f"Error pulling search terms for {customer_id}: {e}")

        return pd.DataFrame(rows)

    def pull_network_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull performance data by ad network/channel."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                segments.ad_network_type,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM campaign
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        # Aggregate by network
        network_agg = {}
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                network = row.segments.ad_network_type.name
                if network not in network_agg:
                    network_agg[network] = {
                        'network': network,
                        'impressions': 0,
                        'clicks': 0,
                        'cost': 0,
                        'conversions': 0
                    }
                network_agg[network]['impressions'] += row.metrics.impressions
                network_agg[network]['clicks'] += row.metrics.clicks
                network_agg[network]['cost'] += row.metrics.cost_micros / 1_000_000
                network_agg[network]['conversions'] += row.metrics.conversions
        except GoogleAdsException as e:
            logger.warning(f"Error pulling network data for {customer_id}: {e}")

        return pd.DataFrame(list(network_agg.values()))

    def pull_device_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull performance data by device type."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                segments.device,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM campaign
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        # Aggregate by device
        device_agg = {}
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                device = row.segments.device.name
                if device not in device_agg:
                    device_agg[device] = {
                        'device': device,
                        'impressions': 0,
                        'clicks': 0,
                        'cost': 0,
                        'conversions': 0
                    }
                device_agg[device]['impressions'] += row.metrics.impressions
                device_agg[device]['clicks'] += row.metrics.clicks
                device_agg[device]['cost'] += row.metrics.cost_micros / 1_000_000
                device_agg[device]['conversions'] += row.metrics.conversions
        except GoogleAdsException as e:
            logger.warning(f"Error pulling device data for {customer_id}: {e}")

        return pd.DataFrame(list(device_agg.values()))

    def pull_location_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull performance data by geographic location."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                geographic_view.country_criterion_id,
                geographic_view.location_type,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date,
                segments.geo_target_region,
                segments.geo_target_city
            FROM geographic_view
            WHERE segments.date = '{date}'
                AND metrics.impressions > 0
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                # Try to get a readable location name
                location = row.segments.geo_target_city or row.segments.geo_target_region or f"Location {row.geographic_view.country_criterion_id}"
                rows.append({
                    'location': location,
                    'location_type': row.geographic_view.location_type.name if row.geographic_view.location_type else '',
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1_000_000,
                    'conversions': row.metrics.conversions
                })
        except GoogleAdsException as e:
            # geographic_view might not be available for all campaign types
            logger.warning(f"Error pulling location data for {customer_id}: {e}")

        # Aggregate by location
        if rows:
            df = pd.DataFrame(rows)
            return df.groupby('location', as_index=False).agg({
                'impressions': 'sum',
                'clicks': 'sum',
                'cost': 'sum',
                'conversions': 'sum'
            })
        return pd.DataFrame(rows)

    def save_data(self, df: pd.DataFrame, client_id: str, data_type: str, month: str):
        """Save dataframe to local CSV file."""
        if df.empty:
            logger.info(f"No {data_type} data for {client_id} in {month}, skipping")
            return

        out_dir = DATA_DIR / client_id / data_type
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{month}.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"  {data_type}/{month}.csv: {len(df)} rows")

    def process_account_month(self, account: Dict, month: str):
        """Process a single account for an entire month."""
        customer_id = account['customer_id']
        client_id = account['client_id']

        logger.info(f"Processing {account['name']} ({client_id}) for {month}")

        # Calculate date range for the month
        year, mo = month.split('-')
        import calendar
        last_day = calendar.monthrange(int(year), int(mo))[1]
        start_date = f"{year}-{mo}-01"
        end_date = f"{year}-{mo}-{last_day:02d}"

        # Aggregate data for the month
        search_terms_all = []
        network_all = {}
        device_all = {}
        location_all = {}

        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        while current <= end:
            date_str = current.strftime('%Y-%m-%d')

            # Search terms
            st_df = self.pull_search_terms(customer_id, date_str)
            if not st_df.empty:
                search_terms_all.append(st_df)

            # Network data - aggregate
            net_df = self.pull_network_data(customer_id, date_str)
            for _, row in net_df.iterrows():
                key = row['network']
                if key not in network_all:
                    network_all[key] = {'network': key, 'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0}
                network_all[key]['impressions'] += row['impressions']
                network_all[key]['clicks'] += row['clicks']
                network_all[key]['cost'] += row['cost']
                network_all[key]['conversions'] += row['conversions']

            # Device data - aggregate
            dev_df = self.pull_device_data(customer_id, date_str)
            for _, row in dev_df.iterrows():
                key = row['device']
                if key not in device_all:
                    device_all[key] = {'device': key, 'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0}
                device_all[key]['impressions'] += row['impressions']
                device_all[key]['clicks'] += row['clicks']
                device_all[key]['cost'] += row['cost']
                device_all[key]['conversions'] += row['conversions']

            # Location data - aggregate
            loc_df = self.pull_location_data(customer_id, date_str)
            for _, row in loc_df.iterrows():
                key = row['location']
                if key not in location_all:
                    location_all[key] = {'location': key, 'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0}
                location_all[key]['impressions'] += row['impressions']
                location_all[key]['clicks'] += row['clicks']
                location_all[key]['cost'] += row['cost']
                location_all[key]['conversions'] += row['conversions']

            current += timedelta(days=1)

        # Save aggregated data
        if search_terms_all:
            combined_st = pd.concat(search_terms_all, ignore_index=True)
            # Aggregate search terms by search_term + keyword + campaign
            agg_st = combined_st.groupby(
                ['search_term', 'keyword', 'match_type', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name'],
                as_index=False
            ).agg({
                'impressions': 'sum',
                'clicks': 'sum',
                'cost': 'sum',
                'conversions': 'sum'
            })
            self.save_data(agg_st, client_id, 'search_terms', month)
        else:
            self.save_data(pd.DataFrame(), client_id, 'search_terms', month)

        self.save_data(pd.DataFrame(list(network_all.values())), client_id, 'channels', month)
        self.save_data(pd.DataFrame(list(device_all.values())), client_id, 'devices', month)
        self.save_data(pd.DataFrame(list(location_all.values())), client_id, 'locations', month)

    def run(self, month: str = None, client_filter: Optional[str] = None):
        """Run the export for a specific month."""
        if month is None:
            month = datetime.utcnow().strftime('%Y-%m')

        accounts = self.get_accessible_accounts()
        if not accounts:
            logger.error("No accessible accounts found.")
            return

        if client_filter:
            accounts = [a for a in accounts if a['client_id'] == client_filter]
            if not accounts:
                logger.error(f"No account found with client_id '{client_filter}'")
                return

        for account in accounts:
            try:
                self.process_account_month(account, month)
            except Exception as e:
                logger.error(f"Error processing {account['client_id']}: {e}")

        logger.info("Export complete!")


def main():
    parser = argparse.ArgumentParser(description='Export Google Ads insights data')
    parser.add_argument('--config', default=None, help='Path to config file')
    parser.add_argument('--month', help='Month to export (YYYY-MM)')
    parser.add_argument('--date', help='Specific date (will export that month)')
    parser.add_argument('--backfill', type=int, help='Number of months to backfill')
    parser.add_argument('--client', help='Only process this client_id')

    args = parser.parse_args()

    exporter = InsightsExporter(args.config)

    # Determine month(s) to process
    months = []
    if args.month:
        months = [args.month]
    elif args.date:
        months = [args.date[:7]]
    elif args.backfill:
        for i in range(args.backfill):
            dt = datetime.utcnow() - timedelta(days=30 * i)
            months.append(dt.strftime('%Y-%m'))
    else:
        months = [datetime.utcnow().strftime('%Y-%m')]

    for month in sorted(set(months)):
        logger.info(f"=== Processing month: {month} ===")
        exporter.run(month, client_filter=args.client)


if __name__ == '__main__':
    main()
