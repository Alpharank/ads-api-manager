# CLI Reference

## Pipeline

```bash
python pipeline/google_ads_to_s3.py                               # Pull yesterday's data (all 6 datasets)
python pipeline/google_ads_to_s3.py --date 2026-01-15             # Pull specific date
python pipeline/google_ads_to_s3.py --backfill 90                 # Last 90 days
python pipeline/google_ads_to_s3.py --client kitsap_cu --backfill 7  # Single client
python pipeline/google_ads_to_s3.py --list-accounts               # List MCC accounts
```

Datasets pulled per run: campaigns, keywords, clicks, bidding_config, conversion_actions, creatives.

To backfill only specific date ranges, use `--backfill` with `--client` for targeted re-pulls.

## Enrichment

```bash
python scripts/gclid_attribution.py --client {id} --month 2026-01           # GCLID attribution
python scripts/gclid_attribution.py --all --month 2026-01 --dry-run         # Dry run all
python scripts/gclid_attribution.py --all                                   # Scheduled: uses last month
python scripts/import_s3_funded_data.py --client {id} --month 2026-01       # S3 funded import
python scripts/export_athena_data.py 2026-01                                # Athena export
python scripts/export_athena_data.py --list-clients                         # List Athena clients
```

## Utilities

```bash
python scripts/aggregate_monthly.py               # Aggregate daily CSVs → monthly
python scripts/export_insights_data.py             # Pull search terms, channels, devices, locations, negative keywords
python scripts/sync_registry.py                    # Preview dashboard file sync
python scripts/sync_registry.py --commit           # Sync + git commit + push
python scripts/export_account_ids.py               # List all MCC child accounts
```
