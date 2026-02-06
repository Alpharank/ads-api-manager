# Future State: Multi-Model Attribution Integration

## Overview

This document outlines how to connect the Google Ads dashboard to the auto-attribution pipeline for real first-touch, last-touch, and linear attribution models.

---

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CURRENT STATE                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  GOOGLE_ADS_TO_S3 (this repo)                                                        │
│  ─────────────────────────────                                                       │
│  google_ads_to_s3_dag.py @ 6:00 AM UTC                                              │
│                                                                                      │
│  Writes to:                                                                          │
│    s3://ai.alpharank.core/ad-spend-reports/{client_id}/campaigns/{date}.csv         │
│    s3://ai.alpharank.core/ad-spend-reports/{client_id}/keywords/{date}.csv          │
│    s3://ai.alpharank.core/ad-spend-reports/{client_id}/clicks/{date}.csv            │
│                                                                                      │
│                                  ✗ NO CONNECTION ✗                                   │
│                                                                                      │
│  AUTO-ATTRIBUTION (separate repo)                                                    │
│  ─────────────────────────────────                                                   │
│  google_ads.py @ 6:30 AM EST                                                         │
│                                                                                      │
│  Reads from:                                                                         │
│    s3://ai.alpharank.core/adspend_reports/{client_id}_{year_month}_daily.csv        │
│                                                                                      │
│  Writes to:                                                                          │
│    staging.google_ads_campaign_data (Athena)                                         │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### The Gap

| Aspect | google_ads_to_s3 | auto-attribution |
|--------|------------------|------------------|
| **S3 Prefix** | `ad-spend-reports/` | `adspend_reports/` |
| **File Format** | `{client}/campaigns/{date}.csv` | `{client}_{month}_daily.csv` |
| **Schedule** | 6:00 AM UTC | 6:30 AM EST |
| **Output** | Raw campaign/keyword/click data | Enriched data with apps/funded/value |

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              FUTURE STATE                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  GOOGLE_ADS_TO_S3                                                                    │
│  ─────────────────                                                                   │
│  1. Pull daily data from Google Ads API                                             │
│  2. Write to ad-spend-reports/{client}/campaigns/{date}.csv                         │
│  3. NEW: Aggregate to adspend_reports/{client}_{month}_daily.csv                    │
│  4. NEW: Trigger auto-attribution DAG                                               │
│                                                                                      │
│                                      │                                               │
│                                      ▼                                               │
│                                                                                      │
│  AUTO-ATTRIBUTION                                                                    │
│  ────────────────                                                                    │
│  1. Read adspend_reports/{client}_{month}_daily.csv                                 │
│  2. Query prod.application_data for attributed apps                                 │
│  3. NEW: Compute all attribution models (first, last, linear)                       │
│  4. Write to staging.google_ads_campaign_data                                       │
│                                                                                      │
│                                      │                                               │
│                                      ▼                                               │
│                                                                                      │
│  DASHBOARD                                                                           │
│  ─────────                                                                           │
│  1. export_athena_data.py queries staging.google_ads_campaign_data                  │
│  2. Writes to data/{client}/enriched/{month}.csv                                    │
│  3. Dashboard loads real attribution data                                           │
│  4. Model selector shows actual differences                                          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Steps

### Phase 1: Connect S3 Paths

Add aggregation script to transform daily files into the format auto-attribution expects.

**New file: `scripts/aggregate_for_attribution.py`**

```python
#!/usr/bin/env python3
"""
Aggregate daily campaign files into monthly format for auto-attribution.
Writes to: s3://ai.alpharank.core/adspend_reports/{client_id}_{year_month}_daily.csv
"""

import boto3
import pandas as pd
from io import StringIO

S3_BUCKET = "ai.alpharank.core"

def aggregate_month(client_id: str, year_month: str):
    """Aggregate daily files into monthly format expected by auto-attribution."""
    s3 = boto3.client('s3')

    # List all daily files for this month
    prefix = f"ad-spend-reports/{client_id}/campaigns/{year_month}"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    all_data = []
    for obj in response.get('Contents', []):
        content = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
        df = pd.read_csv(content['Body'])
        all_data.append(df)

    if not all_data:
        print(f"No data found for {client_id} {year_month}")
        return

    combined = pd.concat(all_data, ignore_index=True)

    # Rename columns to match auto-attribution expected format
    combined = combined.rename(columns={
        'campaign_id': 'Campaign ID',
        'campaign_name': 'Ad group',
        'date': 'Day',
        'clicks': 'Clicks',
        'cost': 'Cost',
        'conversions': 'Conversions'
    })

    if 'Ad group ID' not in combined.columns:
        combined['Ad group ID'] = combined['Campaign ID']

    # Write to expected location
    output_key = f"adspend_reports/{client_id}_{year_month}_daily.csv"
    csv_buffer = StringIO()
    combined.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=csv_buffer.getvalue()
    )
    print(f"Wrote s3://{S3_BUCKET}/{output_key}")
```

**Update DAG: `dags/google_ads_to_s3_dag.py`**

```python
# Add after pull task
aggregate_task = PythonOperator(
    task_id='aggregate_for_attribution',
    python_callable=aggregate_for_attribution,
    op_kwargs={'year_month': '{{ ds[:7] }}'},
)

pull_task >> aggregate_task
```

### Phase 2: Enable Multi-Model Attribution

Requires changes to auto-attribution repo (see `docs/future_state/google_ads_multi_model_attribution.md` in that repo).

**Key changes needed:**

1. Update `attribution_service.py` to compute all models
2. Add columns to `prod.application_data`:
   - `attribution_first_touch`
   - `attribution_last_touch`
   - `attribution_linear`
3. Update `google_ads.py` to aggregate by model

### Phase 3: Dashboard Integration

**Update `scripts/export_athena_data.py`**

```python
# Query each attribution model
MODELS = ['', '_first_touch', '_linear']  # '' = last_touch (default)

for model in MODELS:
    query = f"""
    SELECT campaign_id, apps, funded, value
    FROM staging.google_ads_campaign_data{model}
    WHERE client_id = '{client_id}'
      AND year_month = '{year_month}'
    """
    # Export to data/{client}/enriched/{month}{model}.csv
```

---

## Attribution Models Reference

| Model | Logic | Example: Customer clicks A → B → C → Converts |
|-------|-------|-----------------------------------------------|
| **Last Click** | 100% credit to final touchpoint | Ad C gets 1.0 conversion |
| **First Click** | 100% credit to first touchpoint | Ad A gets 1.0 conversion |
| **Linear** | Equal split across all touchpoints | A=0.33, B=0.33, C=0.33 |

### Why Models Currently Show Similar Values

1. **Single model stored**: Pipeline only computes last-touch attribution
2. **Tier system**: Highest tier (Paid=4) wins over recency
3. **Single-touch journeys**: Most conversions have only 1 touchpoint

---

## Current Workaround: Simulated Data

The `_first_click.csv` and `_linear.csv` files were generated with synthetic multipliers to demonstrate the dashboard UI. They do not represent real multi-model attribution.

**To replace with real data:**

1. Complete Phase 1 (connect S3 paths)
2. Complete Phase 2 (multi-model in auto-attribution)
3. Run `export_athena_data.py` to pull real data
4. Dashboard will automatically show real differences

---

## Files Reference

| File | Purpose |
|------|---------|
| `scripts/export_athena_data.py` | Query Athena for enriched campaign data |
| `scripts/import_s3_funded_data.py` | Alternative: Import from S3 DPR files |
| `scripts/generate_daily_attribution.py` | Distribute monthly totals to daily/keyword level |
| `index.html` | Dashboard with attribution model selector |

---

## Related Documentation

- [Auto-Attribution: Multi-Model Attribution](https://github.com/Alpharank/auto-attribution/blob/attribution_change_ideal_refactor/docs/future_state/google_ads_multi_model_attribution.md)
- [Auto-Attribution: README](https://github.com/Alpharank/auto-attribution/blob/attribution_change_ideal_refactor/README.md)
