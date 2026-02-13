"""
Airflow DAG: Google Ads to S3 Daily Pipeline

Discovers all MCC child accounts automatically, pulls data in parallel via
dynamic task mapping, and updates the dashboard files in S3.

Flow:
  discover_accounts  -->  pull_account.expand(N)  -->  update_dashboard_files  -->  export_for_attribution
                     \--> notify_account_changes (parallel, only when changes)
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Ensure the project root is on sys.path so `pipeline` is importable.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "alpharank",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="google_ads_to_s3_daily",
    default_args=default_args,
    description="Discover MCC accounts, pull Google Ads data, and update dashboard files",
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["google_ads", "s3", "etl"],
)
def google_ads_to_s3_daily():

    @task
    def discover_accounts() -> dict:
        """Query the MCC and return the full account list from the registry,
        along with any additions/removals."""
        from pipeline.google_ads_to_s3 import GoogleAdsToS3

        pipeline = GoogleAdsToS3(config_path=CONFIG_PATH)
        return pipeline.discover_accounts()

    @task
    def pull_account(account: dict, **context) -> dict:
        """Pull Google Ads data for a single account for yesterday's date."""
        from pipeline.google_ads_to_s3 import GoogleAdsToS3

        ds = context.get("ds")  # Airflow logical date (YYYY-MM-DD)
        if ds is None:
            ds = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        pipeline = GoogleAdsToS3(config_path=CONFIG_PATH)
        return pipeline.process_account(account, ds)

    @task(trigger_rule="all_done")
    def update_dashboard_files() -> None:
        """Rebuild clients.json and data-manifest.json in S3 from the registry."""
        from pipeline.google_ads_to_s3 import GoogleAdsToS3

        pipeline = GoogleAdsToS3(config_path=CONFIG_PATH)
        registry = pipeline._load_registry()
        if not registry:
            return

        bucket = pipeline.bucket
        prefix = pipeline.prefix
        s3 = pipeline.s3_client

        # --- clients.json ---
        # Load existing clients.json from S3 to preserve extra fields (e.g. stripPrefix)
        clients_key = f"{prefix}/_dashboard/clients.json"
        existing_clients = {}
        try:
            obj = s3.get_object(Bucket=bucket, Key=clients_key)
            existing_clients = json.loads(obj['Body'].read().decode('utf-8'))
        except s3.exceptions.NoSuchKey:
            pass

        clients = {}
        for entry in registry.values():
            token = entry['dashboard_token']
            new_data = {
                "id": entry['client_id'],
                "name": entry['name'],
            }
            # Preserve any extra fields from the existing entry (e.g. stripPrefix)
            if token in existing_clients:
                merged = {**existing_clients[token], **new_data}
            else:
                merged = new_data
            clients[token] = merged

        s3.put_object(
            Bucket=bucket,
            Key=clients_key,
            Body=json.dumps(clients, indent=2),
            ContentType='application/json',
        )

        # --- data-manifest.json ---
        manifest = {}
        for entry in registry.values():
            client_id = entry['client_id']
            # List month-level CSVs for this client's campaigns folder
            resp = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{prefix}/{client_id}/campaigns/",
                Delimiter='/',
            )
            months = set()
            for obj_meta in resp.get('Contents', []):
                key = obj_meta['Key']
                basename = key.rsplit('/', 1)[-1]
                if basename.endswith('.csv') and len(basename) >= 10:
                    # daily files: YYYY-MM-DD.csv -> extract month
                    months.add(basename[:7])
            if months:
                manifest[client_id] = sorted(months, reverse=True)

        manifest_key = f"{prefix}/_dashboard/data-manifest.json"
        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2),
            ContentType='application/json',
        )

    @task(trigger_rule="all_done")
    def export_for_attribution(**context) -> list:
        """Export monthly campaign CSV in the format the ROI attribution pipeline expects."""
        from pipeline.google_ads_to_s3 import GoogleAdsToS3

        ds = context.get("ds")  # Airflow logical date (yesterday)
        year_month = ds[:7]  # YYYY-MM from YYYY-MM-DD

        pipeline = GoogleAdsToS3(config_path=CONFIG_PATH)
        exported = pipeline.export_for_attribution(year_month)
        logger.info(f"Exported attribution files for {len(exported)} clients: {exported}")
        return exported

    # # EC2 stop — commented out until we confirm which instance runs this DAG
    # INSTANCE_ID = "i-044c661e5fb2c0c37"  # auto-attribution-prod
    #
    # @task(trigger_rule="all_done")
    # def stop_instance() -> None:
    #     """Stop the EC2 instance after the DAG completes."""
    #     import boto3
    #     ec2 = boto3.client("ec2", region_name="us-west-2")
    #     ec2.stop_instances(InstanceIds=[INSTANCE_ID])
    #     logger.info(f"Stopping EC2 instance {INSTANCE_ID}")

    @task
    def notify_account_changes(discovery_result: dict) -> None:
        """Send a Slack alert to #customer-success when MCC accounts are
        added or removed.  No-op when nothing changed."""
        added = discovery_result.get("added", [])
        removed = discovery_result.get("removed", [])

        if not added and not removed:
            logger.info("No account changes detected — skipping Slack notification")
            return

        from pipeline.slack import SlackNotifier

        lines = [":rotating_light: *Google Ads MCC Account Changes*"]

        if added:
            lines.append("")
            lines.append("*New accounts added:*")
            for acc in added:
                lines.append(
                    f"\u2022 {acc['name']} (`{acc['client_id']}`) "
                    f"\u2014 CID {acc['customer_id']}"
                )

        if removed:
            lines.append("")
            lines.append("*Accounts removed:*")
            for acc in removed:
                lines.append(
                    f"\u2022 {acc['name']} (`{acc['client_id']}`) "
                    f"\u2014 CID {acc['customer_id']}"
                )

        lines.append("")
        lines.append("_Pipeline will automatically pull data for new accounts._")

        message = "\n".join(lines)
        SlackNotifier().send_message(message, "#customer-success")

    # --- Wire the DAG ---
    discovery_result = discover_accounts()

    # pull_account needs just the account list
    pulls = pull_account.expand(account=discovery_result["accounts"])
    dashboard = update_dashboard_files()
    attribution = export_for_attribution()
    pulls >> dashboard >> attribution

    # notify runs in parallel with pulls (no dependency on pull completion)
    notify_account_changes(discovery_result)


google_ads_to_s3_daily()
