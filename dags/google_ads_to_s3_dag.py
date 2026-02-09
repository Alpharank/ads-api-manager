"""
Airflow DAG: Google Ads to S3 Daily Pipeline

Pulls Google Ads data for all configured clients and uploads to S3.
Client list is driven by the Airflow Variable `google_ads_clients` (JSON list).
To add/remove a client, edit that variable in the Airflow UI.
"""

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Ensure the project root is on sys.path so `pipeline` is importable.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DEFAULT_CLIENTS = [
    "californiacoast_cu",
    "commonwealth_one_fcu",
    "firstcommunity_cu",
    "kitsap_cu",
    "publicservice_cu",
]

default_args = {
    "owner": "alpharank",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def pull_client(client_id: str, **context):
    """Run the Google Ads pipeline for a single client for the execution date."""
    from pipeline.google_ads_to_s3 import GoogleAdsToS3

    # Use Airflow's logical execution date (ds = YYYY-MM-DD)
    yesterday = context["ds"]
    config_path = os.path.join(PROJECT_ROOT, "config", "config.yaml")

    pipeline = GoogleAdsToS3(config_path=config_path)
    pipeline.run(start_date=yesterday, client_filter=client_id)


with DAG(
    dag_id="google_ads_to_s3_daily",
    default_args=default_args,
    description="Pull Google Ads data for all clients and upload to S3",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["google_ads", "s3", "etl"],
) as dag:
    clients = json.loads(
        Variable.get("google_ads_clients", default_var=json.dumps(DEFAULT_CLIENTS))
    )

    for client_id in clients:
        PythonOperator(
            task_id=f"pull_{client_id}",
            python_callable=pull_client,
            op_kwargs={"client_id": client_id},
        )
