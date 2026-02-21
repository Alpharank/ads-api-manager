#!/usr/bin/env python3
"""Backfill only bidding_config, conversion_actions, and creatives for N days."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from pipeline.google_ads_to_s3 import GoogleAdsToS3
import logging

logger = logging.getLogger(__name__)

days = int(sys.argv[1]) if len(sys.argv) > 1 else 30

pipeline = GoogleAdsToS3("config/config.yaml")
accounts = pipeline.get_accessible_accounts()

end = datetime.now() - timedelta(days=1)
start = end - timedelta(days=days - 1)

current = start
while current <= end:
    date_str = current.strftime('%Y-%m-%d')
    logger.info(f"=== Backfilling {date_str} ===")

    for account in accounts:
        customer_id = account['customer_id']
        client_id = account['client_id']
        try:
            bidding_df = pipeline.pull_bidding_config(customer_id, date_str)
            pipeline.upload_to_s3(bidding_df, client_id, 'bidding_config', date_str)

            conv_df = pipeline.pull_conversion_actions(customer_id, date_str)
            pipeline.upload_to_s3(conv_df, client_id, 'conversion_actions', date_str)

            creative_df = pipeline.pull_ad_creatives(customer_id, date_str)
            pipeline.upload_to_s3(creative_df, client_id, 'creatives', date_str)

            logger.info(f"  {client_id}: {len(bidding_df)} bidding, "
                        f"{len(conv_df)} conv actions, {len(creative_df)} creatives")
        except Exception as e:
            logger.error(f"  Error on {client_id}: {e}")

    current += timedelta(days=1)

logger.info("Backfill complete!")
