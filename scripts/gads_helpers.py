#!/usr/bin/env python3
"""
Shared Google Ads API utilities.

Provides load_config(), build_client(), search(), get_customer_id(), and log_action()
used across all optimization scripts.
"""

import json
import os
from datetime import datetime

import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")


def load_config(path=None):
    """Load config/config.yaml."""
    if path is None:
        path = os.path.join(ROOT, "config", "config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def build_client(config):
    """Build authenticated GoogleAdsClient from config dict."""
    return GoogleAdsClient.load_from_dict({
        "developer_token": config["google_ads"]["developer_token"],
        "client_id": config["google_ads"]["client_id"],
        "client_secret": config["google_ads"]["client_secret"],
        "refresh_token": config["google_ads"]["refresh_token"],
        "login_customer_id": config["google_ads"]["login_customer_id"],
        "use_proto_plus": True,
    })


def search(svc, customer_id, query, label="data"):
    """Execute a GAQL query, return list of rows. Logs warning on failure."""
    try:
        return list(svc.search(customer_id=customer_id, query=query))
    except GoogleAdsException as e:
        print(f"  Warning: {label} query failed for {customer_id}: {e}")
        return []


def get_customer_id(config, client_slug):
    """Reverse-lookup: client slug (e.g. 'embenauto') -> Google Ads customer ID."""
    mapping = config.get("client_mapping", {})
    for cid, slug in mapping.items():
        if slug == client_slug:
            return str(cid)
    raise ValueError(f"No customer ID found for client '{client_slug}' in client_mapping")


def client_data_dir(client_slug):
    """Return the data directory for a client, e.g. data/embenauto/."""
    return os.path.join(DATA_DIR, client_slug)


def log_action(client_slug, action_type, details):
    """
    Append an optimization action to data/{client}/optimization_log/.
    Each action is a JSON file named by timestamp.
    """
    log_dir = os.path.join(DATA_DIR, client_slug, "optimization_log")
    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.utcnow()
    entry = {
        "timestamp": ts.isoformat() + "Z",
        "action_type": action_type,
        **details,
    }

    filename = f"{ts.strftime('%Y%m%d_%H%M%S')}_{action_type}.json"
    filepath = os.path.join(log_dir, filename)
    with open(filepath, "w") as f:
        json.dump(entry, f, indent=2)

    print(f"  Logged: {filepath}")
    return filepath
