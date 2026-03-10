#!/usr/bin/env python3
"""
Export additional Google Ads insights data for the dashboard.

This script exports:
- Search terms: What users actually searched for that triggered ads
- Channel data: Performance by network (Search, Display, YouTube, etc.)
- Device data: Performance by device type (Mobile, Desktop, Tablet)
- Location data: Performance by geographic location
- Negative keywords: Ad-group-level negative keyword exclusions (account state snapshot)
- Auction data: Campaign-level competitive search metrics (impression share, top/abs top IS, lost IS)
- Campaign scores: Optimization score + quality score breakdown per keyword

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

    def pull_search_terms(self, customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Pull search terms report for a date range."""
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
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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

    def pull_network_data(self, customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Pull performance data by ad network/channel for a date range."""
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
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """

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

    def pull_device_data(self, customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Pull performance data by device type for a date range."""
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
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """

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

    def pull_location_data(self, customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Pull performance data by geographic location for a date range."""
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
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
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
            logger.warning(f"Error pulling location data for {customer_id}: {e}")

        if rows:
            df = pd.DataFrame(rows)
            return df.groupby('location', as_index=False).agg({
                'impressions': 'sum',
                'clicks': 'sum',
                'cost': 'sum',
                'conversions': 'sum'
            })
        return pd.DataFrame(rows)

    def pull_negative_keywords(self, customer_id: str) -> pd.DataFrame:
        """Pull ad-group-level negative keywords (account state, not time-series)."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = """
            SELECT
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name
            FROM ad_group_criterion
            WHERE ad_group_criterion.type = 'KEYWORD'
                AND ad_group_criterion.negative = TRUE
                AND ad_group_criterion.status != 'REMOVED'
        """

        rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                rows.append({
                    'keyword': row.ad_group_criterion.keyword.text,
                    'match_type': row.ad_group_criterion.keyword.match_type.name,
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': row.ad_group.name,
                })
        except GoogleAdsException as e:
            logger.warning(f"Error pulling negative keywords for {customer_id}: {e}")

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['campaign_name', 'ad_group_name', 'keyword']).reset_index(drop=True)
        return df

    def pull_auction_data(self, customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Pull campaign-level competitive search metrics for a date range."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                metrics.search_impression_share,
                metrics.search_top_impression_share,
                metrics.search_absolute_top_impression_share,
                metrics.search_budget_lost_impression_share,
                metrics.search_rank_lost_impression_share,
                metrics.search_exact_match_impression_share,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status = 'ENABLED'
                AND metrics.impressions > 0
        """

        camp_agg = {}
        try:
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                cid = str(row.campaign.id)
                if cid not in camp_agg:
                    camp_agg[cid] = {
                        'campaign_id': cid,
                        'campaign_name': row.campaign.name,
                        'impressions': 0, 'clicks': 0, 'cost': 0, 'conversions': 0,
                        '_sis_sum': 0, '_tis_sum': 0, '_atis_sum': 0,
                        '_blis_sum': 0, '_rlis_sum': 0, '_emis_sum': 0,
                        '_days': 0
                    }
                c = camp_agg[cid]
                c['impressions'] += row.metrics.impressions
                c['clicks'] += row.metrics.clicks
                c['cost'] += row.metrics.cost_micros / 1_000_000
                c['conversions'] += row.metrics.conversions
                c['_sis_sum'] += row.metrics.search_impression_share or 0
                c['_tis_sum'] += row.metrics.search_top_impression_share or 0
                c['_atis_sum'] += row.metrics.search_absolute_top_impression_share or 0
                c['_blis_sum'] += row.metrics.search_budget_lost_impression_share or 0
                c['_rlis_sum'] += row.metrics.search_rank_lost_impression_share or 0
                c['_emis_sum'] += row.metrics.search_exact_match_impression_share or 0
                c['_days'] += 1
        except GoogleAdsException as e:
            logger.warning(f"Error pulling auction data for {customer_id}: {e}")

        rows = []
        for c in camp_agg.values():
            d = c['_days'] or 1
            rows.append({
                'campaign_id': c['campaign_id'],
                'campaign_name': c['campaign_name'],
                'impressions': c['impressions'],
                'clicks': c['clicks'],
                'cost': round(c['cost'], 2),
                'conversions': round(c['conversions'], 2),
                'search_impr_share': round(c['_sis_sum'] / d, 4),
                'search_top_impr_share': round(c['_tis_sum'] / d, 4),
                'search_abs_top_impr_share': round(c['_atis_sum'] / d, 4),
                'search_budget_lost_impr_share': round(c['_blis_sum'] / d, 4),
                'search_rank_lost_impr_share': round(c['_rlis_sum'] / d, 4),
                'search_exact_match_impr_share': round(c['_emis_sum'] / d, 4),
            })
        return pd.DataFrame(rows)

    def pull_campaign_scores(self, customer_id: str) -> pd.DataFrame:
        """Pull campaign optimization scores and keyword quality scores."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")

        # Campaign optimization score (snapshot — no date segment)
        opt_query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign.optimization_score
            FROM campaign
            WHERE campaign.status = 'ENABLED'
        """

        camp_scores = {}
        try:
            response = ga_service.search(customer_id=customer_id, query=opt_query)
            for row in response:
                camp_scores[str(row.campaign.id)] = {
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'optimization_score': round(row.campaign.optimization_score, 4) if row.campaign.optimization_score else None,
                }
        except GoogleAdsException as e:
            logger.warning(f"Error pulling optimization scores for {customer_id}: {e}")

        # Keyword quality scores (snapshot)
        qs_query = """
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.quality_info.quality_score,
                ad_group_criterion.quality_info.creative_quality_score,
                ad_group_criterion.quality_info.post_click_quality_score,
                ad_group_criterion.quality_info.search_predicted_ctr
            FROM keyword_view
            WHERE ad_group_criterion.status = 'ENABLED'
                AND campaign.status = 'ENABLED'
        """

        kw_rows = []
        try:
            response = ga_service.search(customer_id=customer_id, query=qs_query)
            for row in response:
                qi = row.ad_group_criterion.quality_info
                kw_rows.append({
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name,
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': row.ad_group.name,
                    'keyword': row.ad_group_criterion.keyword.text,
                    'match_type': row.ad_group_criterion.keyword.match_type.name,
                    'quality_score': qi.quality_score if qi.quality_score else None,
                    'creative_quality': qi.creative_quality_score.name if qi.creative_quality_score else None,
                    'landing_page_quality': qi.post_click_quality_score.name if qi.post_click_quality_score else None,
                    'expected_ctr': qi.search_predicted_ctr.name if qi.search_predicted_ctr else None,
                })
        except GoogleAdsException as e:
            logger.warning(f"Error pulling quality scores for {customer_id}: {e}")

        # Build campaign-level summary with avg quality score
        if kw_rows:
            kw_df = pd.DataFrame(kw_rows)
            scored = kw_df[kw_df['quality_score'].notna()]
            if not scored.empty:
                camp_avg_qs = scored.groupby('campaign_id')['quality_score'].mean().to_dict()
                camp_kw_count = scored.groupby('campaign_id')['quality_score'].count().to_dict()
                for cid, info in camp_scores.items():
                    info['avg_quality_score'] = round(camp_avg_qs.get(cid, 0), 1) if cid in camp_avg_qs else None
                    info['scored_keywords'] = camp_kw_count.get(cid, 0)
        else:
            kw_df = pd.DataFrame()

        camp_df = pd.DataFrame(list(camp_scores.values()))
        return camp_df, kw_df

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
        """Process a single account for an entire month using date-range queries."""
        import calendar

        customer_id = account['customer_id']
        client_id = account['client_id']

        logger.info(f"Processing {account['name']} ({client_id}) for {month}")

        year, mo = month.split('-')
        last_day = calendar.monthrange(int(year), int(mo))[1]
        start_date = f"{year}-{mo}-01"
        end_date = f"{year}-{mo}-{last_day:02d}"

        # Fetch all data with date-range queries (4 API calls instead of 4*days)
        st_df = self.pull_search_terms(customer_id, start_date, end_date)
        net_df = self.pull_network_data(customer_id, start_date, end_date)
        dev_df = self.pull_device_data(customer_id, start_date, end_date)
        loc_df = self.pull_location_data(customer_id, start_date, end_date)

        # Save search terms (aggregate by search_term + keyword + campaign)
        if not st_df.empty:
            agg_st = st_df.groupby(
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

        self.save_data(net_df, client_id, 'channels', month)
        self.save_data(dev_df, client_id, 'devices', month)
        self.save_data(loc_df, client_id, 'locations', month)

        # Negative keywords (account state snapshot, no date range needed)
        neg_df = self.pull_negative_keywords(customer_id)
        self.save_data(neg_df, client_id, 'negative_keywords', month)

        # Auction data (competitive search metrics)
        auction_df = self.pull_auction_data(customer_id, start_date, end_date)
        self.save_data(auction_df, client_id, 'auction_data', month)

        # Campaign scores (optimization score + quality scores)
        camp_scores_df, kw_quality_df = self.pull_campaign_scores(customer_id)
        self.save_data(camp_scores_df, client_id, 'campaign_scores', month)
        self.save_data(kw_quality_df, client_id, 'quality_scores', month)

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
