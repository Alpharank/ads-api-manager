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

---
---

# Future State: Ad Group Optimization Engine

## Overview

An automated optimization layer that reads the data already collected by the pipeline (campaigns, keywords, clicks, bidding config, conversion actions, creatives) and makes bid adjustments, pauses underperformers, reallocates budget, and surfaces actionable recommendations. The system operates in two modes: **advisory** (generates recommendations for human review) and **autonomous** (executes changes via the Google Ads API with guardrails).

---

## Current State vs. Target

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CURRENT STATE                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Pipeline pulls data → S3 → Dashboard (read-only)                                   │
│  Humans manually review dashboard and make changes in Google Ads UI                 │
│  No feedback loop from performance data back to campaign settings                   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              FUTURE STATE                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Pipeline pulls data → S3 → Optimization Engine                                     │
│                                      │                                               │
│                          ┌───────────┴───────────┐                                  │
│                          ▼                       ▼                                   │
│                   ADVISORY MODE          AUTONOMOUS MODE                             │
│                   (Slack/email            (API write-back                            │
│                    recommendations)       with guardrails)                           │
│                          │                       │                                   │
│                          ▼                       ▼                                   │
│                   Human approves         Changes applied                             │
│                   in Ads UI              automatically                               │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture

### New Files

| File | Purpose |
|------|---------|
| `optimizer/engine.py` | Core optimization engine — loads data, runs rules, produces actions |
| `optimizer/rules/bid_adjustment.py` | Bid increase/decrease rules based on CPA, ROAS, impression share |
| `optimizer/rules/budget_reallocation.py` | Shift budget from underperforming to outperforming campaigns |
| `optimizer/rules/pause_underperformers.py` | Pause keywords/ads/ad groups below thresholds |
| `optimizer/rules/creative_rotation.py` | Flag weak ad strength, suggest creative refresh |
| `optimizer/actions.py` | Google Ads API mutate layer — applies changes with dry-run support |
| `optimizer/guardrails.py` | Spend caps, change-rate limits, rollback triggers |
| `optimizer/notifications.py` | Slack/email integration for advisory mode |
| `config/optimization_rules.yaml` | Per-client rule configuration and thresholds |
| `dags/optimization_dag.py` | Airflow DAG — runs after data pull completes |

### Data Flow

```
S3 (daily CSVs)
    │
    ├── campaigns/{date}.csv
    ├── keywords/{date}.csv
    ├── clicks/{date}.csv
    ├── bidding_config/{date}.csv
    ├── conversion_actions/{date}.csv
    └── creatives/{date}.csv
          │
          ▼
┌─────────────────────┐
│  Optimization Engine │
│                      │
│  1. Load last N days │
│  2. Compute metrics  │
│  3. Apply rules      │
│  4. Generate actions │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌──────────────────┐
│  Action Queue        │────▶│  Google Ads API   │
│  (with guardrails)   │     │  (mutate calls)   │
└──────────┬──────────┘     └──────────────────┘
           │
           ▼
┌─────────────────────┐
│  Audit Log (S3)      │
│  {client}/audit/     │
│  {date}_actions.json │
└─────────────────────┘
```

---

## Optimization Rules

### 1. Bid Adjustments

The system evaluates keyword and ad group performance over a rolling window (default: 14 days) and adjusts bids based on CPA and ROAS relative to targets pulled from `bidding_config`.

**Rule: CPA-Based Bid Adjustment**

```
IF    keyword has >= 10 clicks in window
AND   keyword CPA > (target_cpa * 1.3)       # 30% above target
THEN  reduce bid by 15%

IF    keyword has >= 10 clicks in window
AND   keyword CPA < (target_cpa * 0.7)       # 30% below target
AND   impression_share < 0.90                  # room to grow
THEN  increase bid by 10%

IF    keyword has >= 50 clicks in window
AND   keyword conversions == 0
THEN  reduce bid by 30% (or pause — see pause rules)
```

**Rule: ROAS-Based Bid Adjustment**

```
IF    campaign bidding_strategy == TARGET_ROAS
AND   actual_roas < (target_roas * 0.7)
AND   spend > $50 in window
THEN  reduce bid by 20%

IF    campaign bidding_strategy == TARGET_ROAS
AND   actual_roas > (target_roas * 1.5)
THEN  increase bid by 10%
```

**Rule: Impression Share Recovery**

```
IF    bidding_strategy == TARGET_IMPRESSION_SHARE
AND   actual_impression_share < impression_share_fraction target
AND   CPA is within acceptable range
THEN  increase bid by 5%
```

**Configuration (`config/optimization_rules.yaml`):**

```yaml
bid_adjustment:
  lookback_days: 14
  min_clicks_for_action: 10
  min_clicks_for_pause: 50
  cpa_high_threshold: 1.3      # multiplier on target CPA
  cpa_low_threshold: 0.7
  roas_high_threshold: 1.5
  roas_low_threshold: 0.7
  max_bid_increase_pct: 0.15
  max_bid_decrease_pct: 0.30
  max_daily_bid_changes_per_account: 50
```

### 2. Budget Reallocation

Redistribute daily budget across campaigns within the same client account based on marginal CPA efficiency.

**Rule: Cross-Campaign Budget Shift**

```
FOR each client account:
    rank campaigns by efficiency = conversions / cost

    IF    bottom 20% campaigns have CPA > 2x account average
    AND   top 20% campaigns have CPA < 0.5x account average
    AND   top campaigns are impression-share constrained
    THEN  shift 10% of bottom campaign budget to top campaigns
```

**Rule: Day-of-Week Budget Adjustment**

```
FOR each campaign:
    compute day-of-week conversion rates over last 30 days

    IF    weekday conversion_rate > weekend conversion_rate * 1.5
    THEN  recommend weekday bid modifier +15%, weekend -10%
```

**Constraints:**
- Never reduce a campaign budget below $10/day
- Never increase a campaign budget by more than 25% in a single action
- Total account spend must remain within 5% of current total
- Require at least 30 days of data before making budget shifts

### 3. Pause Underperformers

**Rule: Keyword Pause**

```
IF    keyword clicks >= 50 over 30 days
AND   keyword conversions == 0
THEN  PAUSE keyword
      LOG reason: "zero_conversions_high_spend"

IF    keyword clicks >= 20 over 30 days
AND   keyword CPA > (account_avg_CPA * 3.0)
THEN  PAUSE keyword
      LOG reason: "cpa_3x_account_average"

IF    keyword quality_score <= 3  (requires pulling quality_score)
AND   keyword CPA > target_CPA
THEN  PAUSE keyword
      LOG reason: "low_quality_high_cpa"
```

**Rule: Ad Group Pause**

```
IF    ad_group has 0 active keywords remaining
THEN  PAUSE ad_group
      LOG reason: "no_active_keywords"

IF    ad_group spend > $200 over 30 days
AND   ad_group conversions == 0
THEN  PAUSE ad_group
      LOG reason: "zero_conversions_significant_spend"
```

**Rule: Ad Pause (Creative)**

```
IF    ad has ad_strength == 'POOR'
AND   ad impressions > 1000 over 14 days
AND   ad CTR < (ad_group_avg_CTR * 0.5)
THEN  PAUSE ad
      LOG reason: "poor_strength_low_ctr"
```

### 4. Creative Analysis & Rotation

Uses data from `pull_ad_creatives()` to evaluate ad performance and recommend creative changes.

**Rule: Ad Strength Alerts**

```
IF    ad_strength == 'POOR' or ad_strength == 'AVERAGE'
AND   ad has been running > 14 days
THEN  FLAG for creative refresh
      RECOMMEND: add more headline/description variations
```

**Rule: Creative Fatigue Detection**

```
FOR each ad over a 30-day rolling window:
    IF    CTR has declined > 25% from first-week CTR
    AND   impressions remain stable (not a seasonality drop)
    THEN  FLAG as "creative fatigue"
          RECOMMEND: new ad variant
```

**Rule: Headline/Description Performance**

```
FOR each ad group with multiple RSA ads:
    rank ads by conversion_rate

    IF    top ad conversion_rate > 2x bottom ad conversion_rate
    AND   both have >= 100 clicks
    THEN  RECOMMEND: pause bottom ad, create new variant
          using top ad's pinned headlines
```

---

## Guardrails & Safety

### Spend Protection

```python
class Guardrails:
    MAX_DAILY_SPEND_CHANGE_PCT = 0.10      # 10% max daily spend swing
    MAX_BID_CHANGE_PER_ACTION = 0.30       # 30% max single bid change
    MAX_ACTIONS_PER_ACCOUNT_PER_DAY = 100  # rate limit
    MIN_DATA_DAYS = 7                       # minimum data before acting
    ROLLBACK_TRIGGER_CPA_SPIKE = 2.0       # if CPA doubles next day, rollback
    COOL_DOWN_HOURS = 24                    # min time between changes to same entity
```

### Change Approval Workflow

```
Level 1 (Auto-apply):
    - Bid adjustments <= 10%
    - Pausing keywords with 0 conversions and 50+ clicks

Level 2 (Slack notification, auto-apply after 4 hours if no objection):
    - Bid adjustments 10-20%
    - Budget reallocation <= 15%
    - Pausing ads with poor strength

Level 3 (Requires human approval):
    - Bid adjustments > 20%
    - Budget reallocation > 15%
    - Pausing ad groups or campaigns
    - Any action on campaigns spending > $500/day
```

### Audit Log

Every action is logged to S3 as JSON for full traceability:

```json
{
  "timestamp": "2026-02-20T08:15:00Z",
  "client_id": "kitsap_cu",
  "customer_id": "1234567890",
  "action": "bid_decrease",
  "entity_type": "keyword",
  "entity_id": "987654321",
  "entity_name": "credit union near me",
  "campaign": "Brand - Search",
  "ad_group": "Brand Terms",
  "old_value": 2.50,
  "new_value": 2.13,
  "change_pct": -0.15,
  "reason": "cpa_above_target",
  "metrics_snapshot": {
    "lookback_days": 14,
    "clicks": 45,
    "conversions": 1,
    "cost": 112.50,
    "cpa": 112.50,
    "target_cpa": 75.00
  },
  "mode": "autonomous",
  "approval_level": 1,
  "dry_run": false
}
```

### Rollback Mechanism

```
After each optimization run:
    Wait 24 hours
    Compare post-change CPA to pre-change CPA

    IF    post_CPA > pre_CPA * ROLLBACK_TRIGGER_CPA_SPIKE
    THEN  revert all changes from that run
          ALERT via Slack: "Rollback triggered for {client_id}"
          DISABLE autonomous mode for this account for 72 hours
```

---

## Implementation Phases

### Phase 1: Advisory Mode (Read-Only)

1. Build `optimizer/engine.py` — loads last N days of CSVs from S3
2. Implement bid adjustment and pause rules
3. Output recommendations as a daily Slack digest per client
4. No API write-back

**Slack Digest Format:**

```
Optimization Report: Kitsap CU (2026-02-20)

Recommended Bid Decreases (5):
  • "auto loan rates" — CPA $142 vs target $75 → reduce bid $2.50 → $2.13
  • "credit union near me" — 0 conversions on 52 clicks → reduce bid 30%
  ...

Recommended Bid Increases (2):
  • "free checking account" — CPA $31 vs target $75, 72% imp share → increase bid 10%
  ...

Recommended Pauses (3):
  • Keyword "loan calculator" — 68 clicks, 0 conversions, $204 spend
  • Ad #12345678 — POOR strength, 0.8% CTR vs 2.1% group avg
  ...

Creative Refresh Needed (1):
  • Ad Group "Auto Loans - Broad" — top ad CTR declined 31% over 30 days

Budget Shift Opportunity:
  • Move 10% ($15/day) from "Display - Awareness" (CPA $210) to "Brand - Search" (CPA $28)
```

### Phase 2: Autonomous Mode with Guardrails

1. Build `optimizer/actions.py` — Google Ads API mutate layer
2. Implement guardrails, approval levels, and cool-down periods
3. Build audit log writer
4. Enable Level 1 auto-apply for 2 weeks, monitor
5. Gradually enable Level 2

### Phase 3: Rollback & Learning

1. Build rollback mechanism
2. Track optimization impact over time (before/after CPA per action type)
3. Auto-tune thresholds based on historical action outcomes
4. Build a dashboard view showing optimization history and ROI of changes

---

## Google Ads API Mutations Required

```python
# Bid adjustment — keyword level
operation = client.get_type("AdGroupCriterionOperation")
criterion = operation.update
criterion.resource_name = f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}"
criterion.cpc_bid_micros = new_bid_micros
# field_mask for cpc_bid_micros

# Pause keyword
criterion.status = client.enums.AdGroupCriterionStatusEnum.PAUSED

# Pause ad
ad_group_ad = operation.update
ad_group_ad.resource_name = f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}"
ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

# Budget update
campaign_budget = operation.update
campaign_budget.resource_name = f"customers/{customer_id}/campaignBudgets/{budget_id}"
campaign_budget.amount_micros = new_budget_micros
```

---

## Per-Client Configuration

```yaml
# config/optimization_rules.yaml

defaults:
  mode: advisory                 # advisory | autonomous
  lookback_days: 14
  min_data_days: 7
  bid_adjustment:
    enabled: true
    max_increase_pct: 0.15
    max_decrease_pct: 0.30
    min_clicks: 10
    cpa_high_threshold: 1.3
    cpa_low_threshold: 0.7
  pause_rules:
    enabled: true
    zero_conv_min_clicks: 50
    cpa_multiplier: 3.0
  budget_reallocation:
    enabled: false               # off by default — opt-in per client
    max_shift_pct: 0.10
    min_campaign_budget: 10.00
  creative_analysis:
    enabled: true
    fatigue_ctr_decline_pct: 0.25
    min_impressions: 1000

overrides:
  kitsap_cu:
    mode: autonomous
    bid_adjustment:
      cpa_high_threshold: 1.2   # tighter threshold for this client
    budget_reallocation:
      enabled: true
  californiacoast_cu:
    mode: advisory              # this client wants manual control
    pause_rules:
      zero_conv_min_clicks: 75  # higher bar before pausing
```

---
---

# Future State: Retargeting & Remarketing Audiences

## Overview

Extend the pipeline to support retargeting workflows: pull remarketing audience data into the pipeline for reporting, create and manage remarketing lists programmatically from CRM/conversion data, and configure RLSA (Remarketing Lists for Search Ads) bid adjustments. This connects the existing click/GCLID data and conversion tracking with audience-based strategies.

---

## Current State vs. Target

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CURRENT STATE                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  • Pipeline collects clicks with GCLIDs                                             │
│  • Conversion actions are now tracked (new pull_conversion_actions)                 │
│  • No audience/remarketing data is pulled                                            │
│  • No programmatic audience management                                               │
│  • No RLSA bid adjustments                                                           │
│  • Retargeting lists are managed manually in Google Ads UI                          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              FUTURE STATE                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  DATA LAYER (Pipeline Extensions)                                                    │
│  ────────────────────────────────                                                    │
│  • pull_audience_data()        → audience lists, sizes, membership status            │
│  • pull_audience_performance() → metrics segmented by audience                       │
│                                                                                      │
│  AUDIENCE MANAGEMENT (New Module)                                                    │
│  ─────────────────────────────────                                                   │
│  • CRM-based audience sync     → upload customer lists from Alpharank CRM           │
│  • Website visitor audiences   → configure tag-based audiences                       │
│  • Conversion-based audiences  → auto-create from conversion action data            │
│  • Lookalike/Similar audiences → seed from best-performing segments                 │
│                                                                                      │
│  RLSA & BID STRATEGY (Optimization)                                                  │
│  ─────────────────────────────────                                                   │
│  • RLSA bid adjustments        → boost bids for past visitors/converters            │
│  • Audience exclusions         → exclude existing members/funded accounts            │
│  • Cross-sell targeting        → target checking-only members for loan ads          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture

### New Files

| File | Purpose |
|------|---------|
| `pipeline/google_ads_to_s3.py` | Add `pull_audience_data()` and `pull_audience_performance()` methods |
| `retargeting/audience_manager.py` | Create, update, and sync remarketing lists via Google Ads API |
| `retargeting/crm_sync.py` | Map Alpharank CRM segments to Google Ads Customer Match lists |
| `retargeting/rlsa_optimizer.py` | Manage RLSA bid adjustments based on audience performance |
| `retargeting/exclusion_manager.py` | Maintain exclusion lists (existing members, funded accounts) |
| `config/audiences.yaml` | Audience definitions, CRM mappings, RLSA rules per client |
| `dags/audience_sync_dag.py` | Airflow DAG for periodic audience refresh |

### Data Flow

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Google Ads API  │     │  Alpharank CRM   │     │  Conversion Data │
│  (audience data) │     │  (member lists)  │     │  (S3 pipeline)   │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                         │
         ▼                        ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       AUDIENCE ENGINE                                │
│                                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐          │
│  │ Pull &      │  │ CRM Segment  │  │ Conversion-Based  │          │
│  │ Report      │  │ Sync         │  │ Audience Builder   │          │
│  │ Audiences   │  │              │  │                    │          │
│  └──────┬──────┘  └──────┬───────┘  └─────────┬─────────┘          │
│         │                │                     │                     │
│         ▼                ▼                     ▼                     │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │               Audience Registry (S3 + Config)           │        │
│  │  {client}/audiences/registry.json                       │        │
│  │  {client}/audiences/performance/{date}.csv              │        │
│  └────────────────────────┬────────────────────────────────┘        │
│                           │                                          │
│         ┌─────────────────┼─────────────────┐                       │
│         ▼                 ▼                 ▼                        │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────────┐            │
│  │ RLSA Bid   │  │ Exclusion    │  │ Cross-Sell       │            │
│  │ Optimizer  │  │ Manager      │  │ Targeting        │            │
│  └────────────┘  └──────────────┘  └──────────────────┘            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Part 1: Pipeline Extensions (Data Pull)

### `pull_audience_data(customer_id, date)`

Snapshot of all remarketing/audience lists and their sizes.

```sql
SELECT
    user_list.id,
    user_list.name,
    user_list.type,
    user_list.membership_status,
    user_list.size_for_search,
    user_list.size_for_display,
    user_list.membership_life_span,
    user_list.match_rate_percentage,
    user_list.eligible_for_search,
    user_list.eligible_for_display
FROM user_list
WHERE user_list.membership_status = 'OPEN'
```

**Columns:**

`audience_id, name, type, membership_status, size_search, size_display, life_span_days, match_rate, eligible_search, eligible_display`

**Output:** `{client}/audiences/{date}.csv`

### `pull_audience_performance(customer_id, date)`

Campaign/ad group performance segmented by audience. Date-filtered for daily metrics.

```sql
SELECT
    campaign.id,
    campaign.name,
    ad_group.id,
    ad_group.name,
    ad_group_criterion.user_list.user_list,
    ad_group_criterion.bid_modifier,
    ad_group_criterion.status,
    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros,
    metrics.conversions,
    metrics.conversions_value,
    segments.date
FROM ad_group_audience_view
WHERE segments.date = '{date}'
    AND metrics.impressions > 0
```

**Columns:**

`date, campaign_id, campaign_name, ad_group_id, ad_group_name, audience_resource, bid_modifier, status, impressions, clicks, cost, conversions, conversion_value`

**Output:** `{client}/audience_performance/{date}.csv`

---

## Part 2: CRM-Based Audience Sync

### Overview

Upload Alpharank CRM segments as Google Ads Customer Match lists. This allows targeting (or excluding) specific member segments: existing members, high-value members, members with only checking accounts (cross-sell candidates), lapsed members, etc.

### Audience Segments

```yaml
# config/audiences.yaml

crm_audiences:
  all_members:
    description: "All current credit union members"
    purpose: "exclusion"          # exclude from acquisition campaigns
    crm_query: "SELECT email, phone FROM members WHERE status = 'active'"
    refresh_frequency: weekly
    match_type: customer_match    # uses email + phone for matching

  high_value_members:
    description: "Members with >$50k in deposits or >2 products"
    purpose: "cross_sell"
    crm_query: >
      SELECT email, phone FROM members
      WHERE status = 'active'
      AND (total_deposits > 50000 OR product_count > 2)
    refresh_frequency: weekly
    match_type: customer_match

  checking_only:
    description: "Members with only a checking account"
    purpose: "cross_sell"         # target for loan, savings, credit card ads
    crm_query: >
      SELECT email, phone FROM members
      WHERE status = 'active'
      AND products = 'checking'
      AND product_count = 1
    refresh_frequency: weekly
    match_type: customer_match

  recent_converters:
    description: "People who converted in last 90 days"
    purpose: "exclusion"          # don't re-target people who just applied
    source: "conversion_data"     # built from pipeline conversion data
    lookback_days: 90
    refresh_frequency: daily

  website_visitors:
    description: "Visitors to specific pages (loan pages, rate pages)"
    purpose: "retarget"
    source: "google_ads_tag"      # managed via Google Ads remarketing tag
    tag_rules:
      - name: "loan_page_visitors"
        url_contains: "/loans"
        membership_days: 30
      - name: "rate_page_visitors"
        url_contains: "/rates"
        membership_days: 14
      - name: "app_abandoners"
        url_contains: "/apply"
        did_not_convert: true
        membership_days: 7
```

### CRM Sync Process

```python
# retargeting/crm_sync.py

class CRMToAudienceSync:
    """Sync Alpharank CRM segments to Google Ads Customer Match lists."""

    def sync_audience(self, client_id: str, audience_config: dict):
        """
        1. Query CRM for member segment
        2. Hash PII (SHA-256 as required by Customer Match)
        3. Upload to Google Ads as OfflineUserDataJob
        4. Log sync result
        """

    def hash_pii(self, email: str = None, phone: str = None) -> dict:
        """
        Google Customer Match requires SHA-256 hashed, lowercased,
        trimmed email/phone. No plaintext PII leaves the system.
        """
        import hashlib
        result = {}
        if email:
            result['hashed_email'] = hashlib.sha256(
                email.strip().lower().encode()
            ).hexdigest()
        if phone:
            # E.164 format, remove non-digits
            normalized = re.sub(r'[^\d+]', '', phone)
            result['hashed_phone_number'] = hashlib.sha256(
                normalized.encode()
            ).hexdigest()
        return result
```

### Google Ads API: Customer Match Upload

```python
# Create user list
user_list_operation = client.get_type("UserListOperation")
user_list = user_list_operation.create
user_list.name = f"CRM - {audience_name} - {client_id}"
user_list.description = audience_config['description']
user_list.membership_life_span = 90  # days
user_list.crm_based_user_list.upload_key_type = (
    client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
)

# Upload members via OfflineUserDataJob
job_operation = client.get_type("OfflineUserDataJobOperation")
user_data = job_operation.create
user_identifier = client.get_type("UserIdentifier")
user_identifier.hashed_email = hashed_email
user_data.user_identifiers.append(user_identifier)
```

### Privacy & Compliance

- **No plaintext PII** is sent to Google — all emails and phone numbers are SHA-256 hashed before upload
- CRM queries run within the Alpharank data environment; only hashes leave the system
- Customer Match lists are subject to Google's minimum list size (1,000 matched users)
- All audience syncs are logged with timestamp, list size, and match rate
- Members who opt out of marketing are excluded from all audience uploads
- Consent status is checked in the CRM query (`WHERE marketing_consent = true`)

---

## Part 3: RLSA (Remarketing Lists for Search Ads) Optimization

### Strategy Overview

RLSA allows bid adjustments on search campaigns for users who are already in a remarketing audience. This combines intent (search query) with familiarity (past visitor/member).

### RLSA Bid Adjustment Rules

```yaml
# config/audiences.yaml

rlsa_rules:
  website_visitors:
    # People who visited the site but didn't convert
    bid_modifier: 1.30            # +30% bid boost
    apply_to: all_search_campaigns
    rationale: "Past visitors convert at 2-3x rate of new visitors"

  loan_page_visitors:
    bid_modifier: 1.50            # +50% bid boost
    apply_to:
      - campaign_name_contains: "Loan"
      - campaign_name_contains: "Auto"
    rationale: "Showed strong loan intent, high conversion likelihood"

  app_abandoners:
    bid_modifier: 1.75            # +75% bid boost
    apply_to:
      - campaign_name_contains: "Loan"
      - campaign_name_contains: "Checking"
      - campaign_name_contains: "Brand"
    rationale: "Started application, highest intent segment"

  existing_members:
    bid_modifier: 0.50            # -50% bid reduction (or exclude)
    apply_to: acquisition_campaigns
    rationale: "Already a member, acquisition spend is wasted"
    alternative: "exclude"        # can exclude entirely instead

  high_value_members:
    bid_modifier: 1.20            # +20% for cross-sell campaigns
    apply_to:
      - campaign_name_contains: "Cross-Sell"
      - campaign_name_contains: "Credit Card"
    rationale: "Proven relationship, likely to add products"
```

### RLSA Optimizer Implementation

```python
# retargeting/rlsa_optimizer.py

class RLSAOptimizer:
    """Manage RLSA bid adjustments based on audience performance data."""

    def optimize(self, client_id: str, date: str):
        """
        1. Load audience_performance data from S3
        2. Compare audience-segmented CPA/ROAS to non-audience baseline
        3. Adjust bid modifiers based on actual performance
        4. Apply rules from config/audiences.yaml
        """

    def calculate_optimal_modifier(
        self,
        audience_cpa: float,
        baseline_cpa: float,
        current_modifier: float
    ) -> float:
        """
        Adjust bid modifier based on relative CPA performance.

        If audience CPA is 50% lower than baseline → increase modifier
        If audience CPA is higher than baseline → decrease modifier

        Formula:
            optimal = baseline_cpa / audience_cpa
            # Dampen: move 25% toward optimal from current
            new_modifier = current + (optimal - current) * 0.25
            # Clamp to [0.5, 3.0] range
        """
        optimal = baseline_cpa / audience_cpa if audience_cpa > 0 else current_modifier
        new_modifier = current_modifier + (optimal - current_modifier) * 0.25
        return max(0.5, min(3.0, round(new_modifier, 2)))
```

### Auto-Tuning Bid Modifiers

Rather than using static modifiers from config, the system auto-tunes based on observed performance:

```
Every 7 days:
    FOR each (campaign, audience) pair:
        audience_cpa = cost / conversions for this audience segment
        baseline_cpa = cost / conversions for non-audience traffic

        IF audience_cpa < baseline_cpa:
            # Audience outperforms → increase modifier
            new_modifier = move 25% toward (baseline_cpa / audience_cpa)
        ELSE:
            # Audience underperforms → decrease modifier
            new_modifier = move 25% toward (baseline_cpa / audience_cpa)

        CLAMP new_modifier to [0.5, 3.0]

        IF abs(new_modifier - current_modifier) > 0.05:
            APPLY change via Google Ads API
            LOG adjustment to audit trail
```

---

## Part 4: Audience Exclusions

### Why Exclusions Matter for Credit Unions

Credit unions pay per click. Showing acquisition ads to existing members is wasted spend. Excluding existing members from acquisition campaigns and recent converters from all campaigns is one of the highest-ROI optimizations.

### Exclusion Matrix

| Audience | Exclude From | Reason |
|----------|-------------|--------|
| All current members | Acquisition campaigns (checking, savings, generic brand) | Already a member |
| Recent converters (90 days) | All campaigns for that product | Just applied/funded |
| Funded accounts (loan funded) | Loan campaigns for that product | Already have the product |
| Checking-only members | Checking acquisition campaigns | Already have checking |
| High-value members | Acquisition campaigns | Already deeply engaged |

### Implementation

```python
# retargeting/exclusion_manager.py

class ExclusionManager:
    """Manage audience exclusions across campaigns."""

    def apply_exclusions(self, client_id: str, customer_id: str):
        """
        1. Load exclusion rules from config
        2. Ensure each exclusion audience exists in Google Ads
        3. Apply as negative audience criteria on target campaigns
        4. Log all exclusions applied
        """

    def add_campaign_exclusion(
        self,
        customer_id: str,
        campaign_id: str,
        user_list_id: str
    ):
        """Add a negative audience criterion to a campaign."""
        operation = client.get_type("CampaignCriterionOperation")
        criterion = operation.create
        criterion.campaign = f"customers/{customer_id}/campaigns/{campaign_id}"
        criterion.negative = True
        criterion.user_list.user_list = (
            f"customers/{customer_id}/userLists/{user_list_id}"
        )
```

### Estimated Impact

For a typical credit union spending $10,000/month on search ads:

```
Assumption: 15% of clicks are from existing members
             (based on observed match rates in similar CU accounts)

Wasted spend:     $10,000 × 15% = $1,500/month
Annual savings:   $18,000/year per client

With 10 clients:  $180,000/year in recovered ad spend
```

---

## Part 5: Cross-Sell Targeting

### Strategy

Target existing members with ads for products they don't have. Uses CRM data to build specific segments.

### Cross-Sell Audience Matrix

| Member Has | Target With | Campaign Type |
|-----------|-------------|---------------|
| Checking only | Savings, Auto Loans, Credit Cards | Cross-sell - Savings, etc. |
| Checking + Savings | Auto Loans, Home Equity, Credit Cards | Cross-sell - Loans |
| Auto Loan only | Checking, Savings, Refinance | Cross-sell - Deposit |
| Any product, high balance | Investment services, Premium checking | Cross-sell - Premium |

### Configuration

```yaml
# config/audiences.yaml

cross_sell:
  enabled: true

  segments:
    checking_to_savings:
      source_audience: "checking_only"
      target_campaigns:
        - name_contains: "Savings"
        - name_contains: "Money Market"
      bid_modifier: 1.40
      ad_copy_override:
        headline: "Already a Member? Earn More on Savings"
        description: "Open a high-yield savings account alongside your checking"

    checking_to_auto:
      source_audience: "checking_only"
      target_campaigns:
        - name_contains: "Auto Loan"
      bid_modifier: 1.30
      ad_copy_override:
        headline: "Member-Exclusive Auto Loan Rates"
        description: "As a member, you qualify for rates as low as X.XX% APR"

    deposit_to_loan:
      source_audience: "deposit_only"
      target_campaigns:
        - name_contains: "Loan"
        - name_contains: "HELOC"
      bid_modifier: 1.25
```

---

## Implementation Phases

### Phase 1: Audience Data Pull (Pipeline Extension)

**Timeline: Immediate — extends existing pipeline**

1. Add `pull_audience_data()` and `pull_audience_performance()` to `google_ads_to_s3.py`
2. Add calls in `process_account()` after creatives
3. Output to `{client}/audiences/{date}.csv` and `{client}/audience_performance/{date}.csv`
4. Dashboard extension to show audience performance metrics

### Phase 2: Member Exclusions (Highest ROI)

**Timeline: After CRM access is established**

1. Build `retargeting/crm_sync.py` with Customer Match upload
2. Create "all members" audience from CRM
3. Apply as negative audience on all acquisition campaigns
4. Measure spend reduction over 30 days
5. This single step likely saves 10-15% of acquisition spend

### Phase 3: RLSA Bid Adjustments

**Timeline: After 30 days of audience performance data**

1. Build `retargeting/rlsa_optimizer.py`
2. Start with static modifiers from config (website visitors +30%, app abandoners +75%)
3. After 30 days, enable auto-tuning based on observed performance
4. Monitor CPA impact weekly

### Phase 4: Cross-Sell Campaigns

**Timeline: After exclusions are stable**

1. Build CRM segment queries for cross-sell audiences (checking-only, deposit-only, etc.)
2. Upload via Customer Match
3. Create dedicated cross-sell campaigns or apply as RLSA on existing campaigns
4. Build custom ad copy per segment
5. Track incremental product adoption

### Phase 5: Advanced Retargeting

**Timeline: After cross-sell is proven**

1. Application abandoner retargeting (tag-based, 7-day window, high bid boost)
2. Rate page visitor retargeting (14-day window)
3. Lookalike/Similar audience creation from top converters
4. Dynamic remarketing with product-specific ads
5. Sequential messaging (awareness → consideration → conversion)

---

## Metrics & Reporting

### Key Metrics to Track

| Metric | Definition | Target |
|--------|-----------|--------|
| Member exclusion savings | Monthly spend reduction from excluding existing members | 10-15% of acquisition spend |
| RLSA lift | Conversion rate of RLSA audiences vs. non-audience | 2-3x improvement |
| Cross-sell CAC | Cost to acquire a new product from existing member | 50% lower than new member CAC |
| Audience match rate | % of CRM records matched in Google Ads | > 40% (email), > 30% (phone) |
| Audience freshness | Days since last sync | < 7 days |
| App abandoner recovery | % of abandoners who return and complete application | 5-10% |

### Reporting Output

Add to daily pipeline output:

```
{client}/audiences/{date}.csv               — audience list sizes
{client}/audience_performance/{date}.csv     — per-audience metrics
{client}/audience_sync/{date}.json           — CRM sync log
{client}/rlsa_adjustments/{date}.json        — bid modifier changes
```

---

## Dependencies & Prerequisites

| Dependency | Status | Required For |
|-----------|--------|-------------|
| Google Ads remarketing tag on CU websites | Check per client | Website visitor audiences |
| CRM database access (read-only) | Requires Alpharank engineering | Customer Match uploads |
| Google Ads Customer Match eligibility | Requires account-level check | CRM-based audiences |
| Minimum audience size (1,000 users) | Depends on CU member count | Customer Match lists |
| Conversion tracking (pull_conversion_actions) | ✅ Implemented | Conversion-based audiences |
| Click data with GCLIDs (pull_click_data) | ✅ Implemented | Conversion attribution |
| Bidding config (pull_bidding_config) | ✅ Implemented | RLSA optimization baseline |

---

## Related Documentation

- [Auto-Attribution: Multi-Model Attribution](https://github.com/Alpharank/auto-attribution/blob/attribution_change_ideal_refactor/docs/future_state/google_ads_multi_model_attribution.md)
- [Auto-Attribution: README](https://github.com/Alpharank/auto-attribution/blob/attribution_change_ideal_refactor/README.md)
