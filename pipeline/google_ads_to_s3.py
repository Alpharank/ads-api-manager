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
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
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

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 5  # seconds


def _validate_date(date_str: str) -> str:
    """Validate and return a YYYY-MM-DD date string."""
    if not _DATE_RE.match(date_str):
        raise ValueError(f"Invalid date format: {date_str!r}. Expected YYYY-MM-DD.")
    # Verify it's a real date
    datetime.strptime(date_str, "%Y-%m-%d")
    return date_str


class GoogleAdsApiError(Exception):
    """Raised when a Google Ads API call fails (distinct from empty results)."""
    pass


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

        # Registry path in S3
        self.registry_key = f"{self.prefix}/_registry/accounts.json"

    # ------------------------------------------------------------------
    # Auto-discovery helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_client_slug(name: str) -> str:
        """Convert an account name to a client_id slug.

        Example: "Altura Credit Union" -> "altura_credit_union"
        """
        slug = name.lower().replace(' ', '_').replace('-', '_')
        slug = ''.join(c for c in slug if c.isalnum() or c == '_')
        # Collapse consecutive underscores
        slug = re.sub(r'_+', '_', slug).strip('_')
        return slug[:30]

    @staticmethod
    def _generate_dashboard_token(client_id: str) -> str:
        """SHA-256 dashboard token: sha256(b'google-ads-{client_id}')."""
        return hashlib.sha256(f"google-ads-{client_id}".encode()).hexdigest()

    def _load_registry(self) -> Dict:
        """Load the account registry from S3. Returns empty dict on first run."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.registry_key)
            return json.loads(obj['Body'].read().decode('utf-8'))
        except self.s3_client.exceptions.NoSuchKey:
            logger.info("No existing registry found in S3 — will create one")
            return {}

    def _save_registry(self, registry: Dict):
        """Write the account registry to S3."""
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.registry_key,
            Body=json.dumps(registry, indent=2),
            ContentType='application/json',
        )
        logger.info(f"Registry saved to s3://{self.bucket}/{self.registry_key} "
                     f"({len(registry)} accounts)")

    def _unique_slug(self, slug: str, existing_slugs: set) -> str:
        """Return *slug* if unique, otherwise append a counter suffix."""
        if slug not in existing_slugs:
            return slug
        for i in range(2, 100):
            candidate = f"{slug}_{i}"[:30]
            if candidate not in existing_slugs:
                return candidate
        raise ValueError(f"Could not generate unique slug for {slug}")

    def discover_accounts(self) -> Dict:
        """Query the MCC, reconcile with the S3 registry, and return the full
        account list along with any additions / removals.

        Returns a dict with keys:
            accounts  – list of active account dicts (customer_id, client_id,
                        name, dashboard_token, is_new)
            added     – list of newly discovered accounts this run
            removed   – list of accounts present in the registry but no longer
                        in the MCC
        """
        # 1. Load current registry
        registry = self._load_registry()

        # 2. Seed from config.yaml client_mapping on first run
        if not registry:
            logger.info("Seeding registry from config.yaml client_mapping")
            # We don't have names yet; the MCC query below will fill them in.
            for cust_id, client_id in self.client_mapping.items():
                registry[str(cust_id)] = {
                    "client_id": client_id,
                    "name": "",
                    "dashboard_token": self._generate_dashboard_token(client_id),
                    "discovered_at": datetime.utcnow().isoformat(timespec='seconds'),
                }

        # 3. Query MCC for live accounts
        mcc_accounts = self.get_accessible_accounts()
        live_ids = {str(a['customer_id']) for a in mcc_accounts}

        existing_slugs = {v['client_id'] for v in registry.values()}
        changed = False
        added: List[Dict] = []

        for acc in mcc_accounts:
            cust_id = str(acc['customer_id'])
            if cust_id in registry:
                # Update name if it was blank (seed case) or changed
                if registry[cust_id]['name'] != acc['name']:
                    registry[cust_id]['name'] = acc['name']
                    changed = True
                # Clear removed_at if the account reappeared
                if registry[cust_id].get('removed_at'):
                    logger.info(f"Account reappeared: {acc['name']} ({cust_id})")
                    del registry[cust_id]['removed_at']
                    changed = True
            else:
                # --- New account ---
                slug = self._generate_client_slug(acc['name'])
                slug = self._unique_slug(slug, existing_slugs)
                existing_slugs.add(slug)

                registry[cust_id] = {
                    "client_id": slug,
                    "name": acc['name'],
                    "dashboard_token": self._generate_dashboard_token(slug),
                    "discovered_at": datetime.utcnow().isoformat(timespec='seconds'),
                }
                changed = True
                added.append({
                    "customer_id": cust_id,
                    "client_id": slug,
                    "name": acc['name'],
                })
                logger.info(f"New account discovered: {acc['name']} -> {slug}")

        # 4. Detect removals — registry entries no longer in the MCC
        removed: List[Dict] = []
        for cust_id, entry in registry.items():
            if cust_id not in live_ids and not entry.get('removed_at'):
                entry['removed_at'] = datetime.utcnow().isoformat(timespec='seconds')
                changed = True
                removed.append({
                    "customer_id": cust_id,
                    "client_id": entry['client_id'],
                    "name": entry['name'],
                })
                logger.info(f"Account removed from MCC: {entry['name']} ({cust_id})")

        if changed:
            self._save_registry(registry)

        # 5. Build return list scoped to accounts currently in the MCC
        accounts = []
        for cust_id, entry in registry.items():
            if cust_id not in live_ids:
                continue
            accounts.append({
                'customer_id': cust_id,
                'client_id': entry['client_id'],
                'name': entry['name'],
                'dashboard_token': entry['dashboard_token'],
                'is_new': cust_id not in self.client_mapping,
            })

        logger.info(f"discover_accounts: {len(accounts)} active accounts "
                     f"({len(added)} added, {len(removed)} removed)")
        return {
            "accounts": accounts,
            "added": added,
            "removed": removed,
        }

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

    def _search_with_retry(self, customer_id: str, query: str, label: str):
        """Execute a Google Ads search with exponential backoff retry."""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return ga_service.search(customer_id=customer_id, query=query)
            except GoogleAdsException as e:
                if attempt == MAX_RETRIES:
                    raise GoogleAdsApiError(
                        f"Failed to pull {label} for {customer_id} after {MAX_RETRIES} attempts: {e}"
                    ) from e
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed for {label} "
                    f"({customer_id}), retrying in {delay}s: {e}"
                )
                time.sleep(delay)

    def pull_campaign_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull campaign performance data for a specific date."""
        safe_date = _validate_date(date)

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
            WHERE segments.date = '{safe_date}'
                AND metrics.impressions > 0
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "campaign data")
        for row in response:
            rows.append({
                'date': row.segments.date,
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'campaign_status': row.campaign.status.name,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'cost': row.metrics.cost_micros / 1_000_000,
                'conversions': row.metrics.conversions
            })

        return pd.DataFrame(rows)

    def pull_keyword_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull keyword performance data for a specific date."""
        safe_date = _validate_date(date)

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
            WHERE segments.date = '{safe_date}'
                AND metrics.impressions > 0
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "keyword data")
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

        return pd.DataFrame(rows)

    def pull_click_data(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull click-level data with GCLIDs for a specific date."""
        safe_date = _validate_date(date)

        query = f"""
            SELECT
                click_view.gclid,
                click_view.keyword,
                click_view.keyword_info.text,
                click_view.keyword_info.match_type,
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
            WHERE segments.date = '{safe_date}'
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "click data")
        for row in response:
            rows.append({
                'date': row.segments.date,
                'gclid': row.click_view.gclid,
                'keyword': row.click_view.keyword_info.text,
                'match_type': row.click_view.keyword_info.match_type.name if row.click_view.keyword_info.match_type else None,
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'ad_group_id': str(row.ad_group.id),
                'ad_group_name': row.ad_group.name,
                'network': row.segments.ad_network_type.name,
                'city': row.click_view.area_of_interest.city if row.click_view.area_of_interest else None,
                'region': row.click_view.area_of_interest.region if row.click_view.area_of_interest else None,
                'country': row.click_view.area_of_interest.country if row.click_view.area_of_interest else None
            })

        return pd.DataFrame(rows)

    def pull_bidding_config(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull bidding strategy config per campaign/ad group (snapshot, no date filter)."""
        query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign.bidding_strategy_type,
                campaign.target_cpa.target_cpa_micros,
                campaign.maximize_conversions.target_cpa_micros,
                campaign.target_roas.target_roas,
                campaign.target_impression_share.location,
                campaign.target_impression_share.location_fraction_micros,
                ad_group.id,
                ad_group.name,
                ad_group.type,
                ad_group.cpc_bid_micros
            FROM ad_group
            WHERE campaign.status = 'ENABLED'
                AND ad_group.status = 'ENABLED'
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "bidding config")
        for row in response:
            # Coalesce target CPA from two possible fields
            target_cpa_micros = (
                row.campaign.target_cpa.target_cpa_micros
                or row.campaign.maximize_conversions.target_cpa_micros
            )
            target_cpa = target_cpa_micros / 1_000_000 if target_cpa_micros else None

            cpc_bid = row.ad_group.cpc_bid_micros / 1_000_000 if row.ad_group.cpc_bid_micros else None

            rows.append({
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'bidding_strategy': row.campaign.bidding_strategy_type.name,
                'target_cpa': target_cpa,
                'target_roas': row.campaign.target_roas.target_roas or None,
                'impression_share_location': row.campaign.target_impression_share.location.name if row.campaign.target_impression_share.location else None,
                'impression_share_fraction': row.campaign.target_impression_share.location_fraction_micros / 1_000_000 if row.campaign.target_impression_share.location_fraction_micros else None,
                'ad_group_id': str(row.ad_group.id),
                'ad_group_name': row.ad_group.name,
                'ad_group_type': row.ad_group.type.name,
                'ad_group_cpc_bid': cpc_bid,
            })

        return pd.DataFrame(rows)

    def pull_conversion_actions(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull account-level conversion action definitions (snapshot, no date filter)."""
        query = """
            SELECT
                conversion_action.id,
                conversion_action.name,
                conversion_action.type,
                conversion_action.category,
                conversion_action.status,
                conversion_action.include_in_conversions_metric,
                conversion_action.counting_type
            FROM conversion_action
            WHERE conversion_action.status != 'REMOVED'
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "conversion actions")
        for row in response:
            rows.append({
                'conversion_action_id': str(row.conversion_action.id),
                'name': row.conversion_action.name,
                'type': row.conversion_action.type.name,
                'category': row.conversion_action.category.name,
                'status': row.conversion_action.status.name,
                'included_in_conversions': row.conversion_action.include_in_conversions_metric,
                'counting_type': row.conversion_action.counting_type.name,
            })

        return pd.DataFrame(rows)

    def pull_ad_creatives(self, customer_id: str, date: str) -> pd.DataFrame:
        """Pull ad copy, creative assets, and per-ad metrics for a specific date."""
        safe_date = _validate_date(date)

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.ad.final_urls,
                ad_group_ad.status,
                ad_group_ad.ad_strength,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM ad_group_ad
            WHERE segments.date = '{safe_date}'
                AND ad_group_ad.status != 'REMOVED'
                AND metrics.impressions > 0
        """

        rows = []
        response = self._search_with_retry(customer_id, query, "ad creatives")
        for row in response:
            # Concatenate RSA headlines/descriptions; guard for non-RSA ad types
            headlines_list = row.ad_group_ad.ad.responsive_search_ad.headlines
            headlines = ' | '.join([h.text for h in headlines_list]) if headlines_list else ''

            descriptions_list = row.ad_group_ad.ad.responsive_search_ad.descriptions
            descriptions = ' | '.join([d.text for d in descriptions_list]) if descriptions_list else ''

            final_urls = ' | '.join(row.ad_group_ad.ad.final_urls) if row.ad_group_ad.ad.final_urls else ''

            rows.append({
                'date': row.segments.date,
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'ad_group_id': str(row.ad_group.id),
                'ad_group_name': row.ad_group.name,
                'ad_id': str(row.ad_group_ad.ad.id),
                'ad_type': row.ad_group_ad.ad.type.name,
                'headlines': headlines,
                'descriptions': descriptions,
                'final_urls': final_urls,
                'status': row.ad_group_ad.status.name,
                'ad_strength': row.ad_group_ad.ad_strength.name if row.ad_group_ad.ad_strength else None,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'cost': row.metrics.cost_micros / 1_000_000,
                'conversions': row.metrics.conversions,
            })

        return pd.DataFrame(rows)

    def upload_to_s3(self, df: pd.DataFrame, client_id: str, data_type: str, date: str):
        """Upload dataframe to S3 as CSV and save locally."""
        if df.empty:
            logger.info(f"No {data_type} data for {client_id} on {date}, skipping")
            return

        csv_content = df.to_csv(index=False)

        # Save locally
        local_dir = os.path.join("output", client_id, data_type)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, f"{date}.csv")
        with open(local_path, 'w') as f:
            f.write(csv_content)

        # Upload to S3
        key = f"{self.prefix}/{client_id}/{data_type}/{date}.csv"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=csv_content,
            ContentType='text/csv'
        )
        logger.info(f"Saved: {local_path} + s3://{self.bucket}/{key} ({len(df)} rows)")

    def process_account(self, account: Dict, date: str):
        """Process a single account for a specific date.

        Raises GoogleAdsApiError if any data pull fails (API error).
        Empty results (no data for that date) are handled gracefully.
        """
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

        # Pull and upload bidding config
        bidding_df = self.pull_bidding_config(customer_id, date)
        self.upload_to_s3(bidding_df, client_id, 'bidding_config', date)

        # Pull and upload conversion actions
        conv_df = self.pull_conversion_actions(customer_id, date)
        self.upload_to_s3(conv_df, client_id, 'conversion_actions', date)

        # Pull and upload ad creatives
        creative_df = self.pull_ad_creatives(customer_id, date)
        self.upload_to_s3(creative_df, client_id, 'creatives', date)

        return {
            'client_id': client_id,
            'campaigns': len(campaign_df),
            'keywords': len(keyword_df),
            'clicks': len(click_df),
            'bidding_config': len(bidding_df),
            'conversion_actions': len(conv_df),
            'creatives': len(creative_df),
        }

    def export_for_attribution(self, year_month: str) -> list[str]:
        """Export monthly campaign data in the format the ROI attribution pipeline expects.

        Reads daily campaign CSVs for the given month, concatenates them, renames
        columns, and writes a single file per client with 2 blank rows prepended
        (the ROI pipeline reads with ``skiprows=2``).

        Args:
            year_month: Month to export in YYYY-MM format.

        Returns:
            List of client_ids that were exported.
        """
        registry = self._load_registry()
        exported: list[str] = []

        for _cust_id, entry in registry.items():
            if entry.get("removed_at"):
                continue

            client_id = entry["client_id"]
            campaign_prefix = f"{self.prefix}/{client_id}/campaigns/{year_month}-"

            resp = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=campaign_prefix,
            )

            csv_keys = [
                obj["Key"]
                for obj in resp.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ]

            if not csv_keys:
                logger.info(f"No campaign data for {client_id} in {year_month}, skipping")
                continue

            frames = []
            for key in sorted(csv_keys):
                obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                frames.append(pd.read_csv(obj["Body"]))

            combined = pd.concat(frames, ignore_index=True)

            combined = combined.rename(columns={
                "campaign_id": "Campaign ID",
                "campaign_name": "Ad group",
                "date": "Day",
                "clicks": "Clicks",
                "cost": "Cost",
                "conversions": "Conversions",
            })
            combined = combined.drop(
                columns=["campaign_status", "impressions"], errors="ignore"
            )
            combined = combined[
                ["Campaign ID", "Ad group", "Day", "Clicks", "Cost", "Conversions"]
            ]

            csv_content = "\n\n" + combined.to_csv(index=False)

            output_key = f"adspend_reports/{client_id}_{year_month}_daily.csv"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=output_key,
                Body=csv_content,
                ContentType="text/csv",
            )
            logger.info(
                f"Exported attribution file: s3://{self.bucket}/{output_key} "
                f"({len(combined)} rows, {len(csv_keys)} days)"
            )
            exported.append(client_id)

        logger.info(
            f"export_for_attribution: exported {len(exported)} clients for {year_month}"
        )
        return exported

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
                                f"{result['keywords']} keywords, {result['clicks']} clicks, "
                                f"{result['bidding_config']} bidding configs, "
                                f"{result['conversion_actions']} conversion actions, "
                                f"{result['creatives']} creatives")
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
