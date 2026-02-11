#!/usr/bin/env python3
"""
Sync the S3 account registry to local git-tracked dashboard files.

Reads  : s3://ai.alpharank.core/ad-spend-reports/_registry/accounts.json
Writes : clients.json, data-manifest.json (project root)

Usage:
    python scripts/sync_registry.py              # preview changes
    python scripts/sync_registry.py --commit     # also git add/commit/push
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLIENTS_PATH = PROJECT_ROOT / "clients.json"
MANIFEST_PATH = PROJECT_ROOT / "data-manifest.json"


def load_config():
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_registry(s3, bucket, prefix):
    key = f"{prefix}/_registry/accounts.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        print(f"Registry not found at s3://{bucket}/{key}")
        sys.exit(1)


def update_clients_json(registry):
    """Rebuild clients.json, preserving extra fields (e.g. stripPrefix)."""
    existing = {}
    if CLIENTS_PATH.exists():
        with open(CLIENTS_PATH) as f:
            existing = json.load(f)

    updated = {}
    for entry in registry.values():
        token = entry['dashboard_token']
        new_data = {"id": entry['client_id'], "name": entry['name']}
        if token in existing:
            merged = {**existing[token], **new_data}
        else:
            merged = new_data
        updated[token] = merged

    with open(CLIENTS_PATH, 'w') as f:
        json.dump(updated, f, indent=2)
        f.write('\n')

    print(f"Updated {CLIENTS_PATH.name}: {len(updated)} clients")
    return updated


def update_data_manifest(registry, s3, bucket, prefix):
    """Scan S3 for available month CSVs per client and update data-manifest.json."""
    manifest = {}
    for entry in registry.values():
        client_id = entry['client_id']
        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/{client_id}/campaigns/",
            Delimiter='/',
        )
        months = set()
        for obj_meta in resp.get('Contents', []):
            basename = obj_meta['Key'].rsplit('/', 1)[-1]
            if basename.endswith('.csv') and len(basename) >= 10:
                months.add(basename[:7])
        if months:
            manifest[client_id] = sorted(months, reverse=True)

    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)
        f.write('\n')

    print(f"Updated {MANIFEST_PATH.name}: {len(manifest)} clients with data")
    return manifest


def git_commit_and_push():
    """Stage, commit, and push the updated dashboard files."""
    os.chdir(PROJECT_ROOT)
    files = ["clients.json", "data-manifest.json"]
    subprocess.run(["git", "add"] + files, check=True)

    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode == 0:
        print("No changes to commit.")
        return

    subprocess.run(
        ["git", "commit", "-m", "chore: sync dashboard files from S3 registry"],
        check=True,
    )
    subprocess.run(["git", "push"], check=True)
    print("Committed and pushed.")


def main():
    parser = argparse.ArgumentParser(description="Sync S3 registry to local dashboard files")
    parser.add_argument("--commit", action="store_true", help="Git add/commit/push after updating")
    args = parser.parse_args()

    config = load_config()
    bucket = config['aws']['bucket']
    prefix = config['aws']['prefix']
    s3 = boto3.client('s3', region_name=config['aws']['region'])

    registry = load_registry(s3, bucket, prefix)
    print(f"Registry: {len(registry)} accounts\n")

    update_clients_json(registry)
    update_data_manifest(registry, s3, bucket, prefix)

    if args.commit:
        git_commit_and_push()

    print("\nDone!")


if __name__ == "__main__":
    main()
