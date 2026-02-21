# Google Ads to S3 Pipeline

Pulls daily Google Ads data, enriches it with first-party attribution, and serves per-client dashboards via GitHub Pages.

**Dashboard:** `https://alpharank.github.io/Google-Ads-Automation/?client=TOKEN` : see [`clients/`](clients/) for tokens

---

## Table of Contents

- [System Overview](#system-overview)
- [Data Flow](#data-flow)
- [What the Dashboard Shows](#what-the-dashboard-shows)
- [Enrichment Pipeline](#enrichment-pipeline)
- [Adding a New Client](#adding-a-new-client)
- [Dry Runs & Validation](#dry-runs--validation)
- [Setup](#setup)
- [CLI Reference](#cli-reference)
- [Project Structure](#project-structure)

---

## System Overview

```
                        DAILY (8 AM UTC)
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Airflow DAG: google_ads_to_s3_daily                            │
│                                                                  │
│  1. Discover accounts under MCC (auto-onboard new ones)         │
│  2. Pull campaigns, keywords, clicks, bidding config,           │
│     conversion actions, creatives per account        ──► S3      │
│  3. Rebuild dashboard manifest files                ──► S3      │
│  4. Export monthly attribution file                 ──► S3      │
│  5. Slack notify #customer-success if accounts changed          │
└──────────────────────────────────────────────────────────────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
     ┌────────────┐  ┌─────────────┐  ┌─────────────────┐
     │  Dashboard  │  │  ROI        │  │  Enrichment     │
     │  (GitHub    │  │  Pipeline   │  │  Scripts         │
     │   Pages)    │  │  (11:30 AM) │  │  (on-demand)    │
     └────────────┘  └─────────────┘  └─────────────────┘
```

---

## Data Flow

### 1. Google Ads API → S3 (Daily, Automated)

The [pipeline](pipeline/google_ads_to_s3.py) pulls six datasets per account per day:

| Dataset | Description | S3 Path |
|---------|-------------|---------|
| Campaigns | Daily spend, clicks, conversions per campaign | `ad-spend-reports/{client}/campaigns/{date}.csv` |
| Keywords | Performance by keyword within each ad group | `ad-spend-reports/{client}/keywords/{date}.csv` |
| Clicks | Individual click events with GCLIDs + geo | `ad-spend-reports/{client}/clicks/{date}.csv` |
| Bidding Config | Bidding strategy, target CPA/ROAS, ad group CPC bids | `ad-spend-reports/{client}/bidding_config/{date}.csv` |
| Conversion Actions | Account-level conversion action definitions | `ad-spend-reports/{client}/conversion_actions/{date}.csv` |
| Creatives | Ad copy, headlines, descriptions, ad strength + metrics | `ad-spend-reports/{client}/creatives/{date}.csv` |

### 2. S3 → Enrichment → Local CSVs (On-Demand)

Enrichment scripts join Google Ads data with first-party application/funded data:

```
S3 Click Data ─────┐
                    ├──► GCLID Attribution ──► data/{client}/enriched/{month}.csv
Athena App Data ───┘                          data/{client}/enriched/daily/{month}.csv
```

### 3. Local CSVs → GitHub Pages Dashboard

The [dashboard](index.html) loads CSV files directly from the repo via GitHub Pages:

```
data/{client}/campaigns/{month}.csv         ← campaign metrics (required)
data/{client}/keywords/{month}.csv          ← keyword metrics
data/{client}/daily/{month}.csv             ← daily timeseries
data/{client}/enriched/{month}.csv          ← funded/attribution data
data/{client}/search_terms/{month}.csv      ← search query data
data/{client}/channels/{month}.csv          ← network breakdown
data/{client}/devices/{month}.csv           ← device type breakdown
data/{client}/locations/{month}.csv         ← geographic breakdown
data/{client}/negative_keywords/{month}.csv ← negative keyword exclusions (account state snapshot, not month-specific)
```

### 4. Attribution Bridge → ROI Pipeline

After each daily pull, a monthly file is exported for the downstream ROI pipeline:

```
ad-spend-reports/{client}/campaigns/*.csv
        │
        ▼  export_for_attribution (daily)
        │
adspend_reports/{client}_{month}_daily.csv
        │
        ▼  update_google_ads_roi (11:30 AM UTC)
        │
staging.google_ads_campaign_data (Athena)
```

---

## What the Dashboard Shows

```
┌──────────────────────────────────────────────────────────────────────┐
│  [=] AlphaRank  │  Client Name  │                  Month: Jan 2026 ▼│
├─────────┬────────────────────────────────────────────────────────────┤
│         │  Impr │ Clicks │ CTR │ Cost │ CPA │ Funded │ Rev │ CPF    │
│Campaigns│  ════════════════════════════════════════════════════════   │
│Ad groups│                                                            │
│─────────│  ┌──────────────────────┐  ┌──────────────────────┐       │
│Keywords │  │  Multi-Metric Chart  │  │  Funded Over Time    │       │
│Search   │  │  (select up to 3)    │  │                      │       │
│ terms   │  └──────────────────────┘  └──────────────────────┘       │
│Negative │                                                            │
│ keywords│                                                            │
│─────────│  ┌────────────────────────────────────────────────┐       │
│Channels │  │  Sortable, Filterable Data Table               │       │
│When &   │  │  (click any row to drill down)                 │       │
│ Where   │  │                                                │       │
│         │  └────────────────────────────────────────────────┘       │
└─────────┴────────────────────────────────────────────────────────────┘
```

### KPI Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| Impressions | Google Ads | Ad impressions |
| Clicks | Google Ads | Click count |
| CTR | Derived | Clicks / Impressions |
| Cost | Google Ads | Ad spend |
| CPA | Derived | Cost / Conversions |
| Funded | First-party | Funded loan applications |
| Funded Rev | First-party | Revenue from funded loans |
| CPF | Derived | Cost / Funded |
| Avg Fund Val | Derived | Revenue / Funded |

Funded metrics require [enriched data](#enrichment-pipeline). Without it, those chips show `--`.

### Drill-Down Navigation

```
All Campaigns
  │  click row
  ▼
Campaign: KCU - Personal - Brand       ← KPIs + charts scoped to this campaign
  │  click row
  ▼
Ad Group: Personal Loans                ← KPIs + charts scoped to this ad group
  │
  └── Keywords table (leaf level)
```

Breadcrumbs and sidebar selection chips let you navigate back to any level.

### User Flow: Investigating a Campaign

```
User opens dashboard URL
  │
  ├─► Sees KPI strip with month-level totals
  ├─► Sees campaign table sorted by clicks
  │
  ├─► Clicks "KCU - Personal - Brand" row
  │     ├─► KPIs recalculate for that campaign only
  │     ├─► Charts re-render scoped to campaign
  │     └─► Table switches to ad groups within that campaign
  │
  ├─► Clicks "Personal Loans" ad group row
  │     ├─► KPIs recalculate for that ad group only
  │     └─► Table switches to keywords within that ad group
  │
  └─► Clicks breadcrumb "All campaigns" to reset
```

### User Flow: Comparing Metrics Over Time

```
User clicks KPI chips (up to 3)
  │
  ├─► Left chart overlays selected metrics (e.g., Cost + Clicks + Funded)
  ├─► Uses date range filter (30d / 60d / 90d / This Month)
  ├─► Toggles Daily vs. Day-of-Week view
  │
  └─► Right chart always shows funded applications over time
```

---

## Enrichment Pipeline

There are **two completely separate paths** to attach first-party funded-loan data to
Google Ads metrics. They read different source tables, join at different granularities,
and produce different levels of detail. Understanding which path you are using is
critical: it determines whether you can see funded metrics at the keyword/ad-group
level or only at the campaign level.

```
                          ┌──────────────────────────────────────────────────────┐
                          │           Two Attribution Paths                     │
                          └──────────────────────────────────────────────────────┘

  PATH A: Athena Export (campaign-level only)          PATH B: GCLID Attribution (keyword-level)
  ───────────────────────────────────────────         ──────────────────────────────────────────

  ROI Pipeline                                        Google Ads API (click_view)
       │                                                   │
       ▼                                                   │  GAQL pulls every click with its
  staging.google_ads_campaign_data                         │  gclid, keyword, ad_group, campaign
  ┌──────────────────────────────────┐                     ▼
  │ campaign_id, campaign,           │                S3: clicks/{date}.csv
  │ clicks, cost, apps,              │                ┌──────────────────────────────────┐
  │ funded, value                    │                │ gclid          ◄── direct column │
  │                                  │                │ keyword, match_type              │
  │ ** NO gclid column **            │                │ campaign_id, ad_group_id, ...    │
  └──────────────┬───────────────────┘                └───────────────┬──────────────────┘
                 │                                                    │
                 ▼                                                    │
  export_athena_data.py                               Athena: prod.application_data
                 │                                    ┌──────────────────────────────────────────┐
                 ▼                                    │ click_id   ◄── direct column (GCLID)    │
  data/{client}/enriched/{month}.csv                  │ funded, approved, production_value, ...  │
  (campaign-level ONLY)                               └───────────────┬──────────────────────────┘
                                                                      │
                                                      ┌───────────────┴──────────────────┐
                                                      │                                  │
                                                 clicks_df                          apps_df
                                                 (has gclid                    (has gclid from
                                                  as a column)                  click_id column)
                                                      │                              │
                                                      └───────────┬──────────────────┘
                                                                  │
                                                                  ▼
                                                      clicks_df.merge(apps_df,
                                                        on="gclid", how="inner")
                                                                  │
                                                      Each matched application now
                                                      inherits the click's keyword,
                                                      ad group, campaign, and geo
                                                                  │
                                                                  ▼
                                                      gclid_attribution.py
                                                                  │
                                                        ┌─────────┴──────────┐
                                                        ▼                    ▼
                                                  enriched/            enriched/daily/
                                                  {month}.csv          {month}.csv
                                                  (campaign)           (keyword-level)
```

### Path A: Athena Export (Campaign-Level Only)

**What it reads:** `staging.google_ads_campaign_data`, a table populated by the
upstream ROI pipeline. This table contains pre-aggregated campaign-level metrics.
It has **no `gclid` column**, so there is no way to trace an individual click back to
a keyword or ad group.

**Available columns:** `campaign_id`, `campaign`, `clicks`, `cost`, `apps`, `approved`,
`funded`, `production`, `value`, all aggregated at the campaign level per day.

**Query (from [`scripts/export_athena_data.py`](scripts/export_athena_data.py) lines 62–93):**

```sql
WITH campaign_level AS (
  SELECT
    campaign_id,
    campaign,
    sum(clicks)        AS clicks,
    sum(cost)          AS cost,
    sum(apps)          AS apps,
    sum(approved)      AS approved,
    sum(funded)        AS funded,
    sum(production)    AS production,
    sum("value")       AS "value"
  FROM staging.google_ads_campaign_data
  WHERE day BETWEEN date ? AND date ?
    AND client_id = ?
  GROUP BY campaign_id, campaign
)
SELECT
  campaign_id,
  campaign AS campaign_name,
  COALESCE(clicks, 0) AS clicks,
  ROUND(COALESCE(cost, 0), 2) AS cost,
  COALESCE(apps, 0) AS apps,
  COALESCE(approved, 0) AS approved,
  COALESCE(funded, 0) AS funded,
  ROUND(COALESCE(production, 0), 2) AS production,
  ROUND(COALESCE("value", 0), 2) AS value,
  CASE WHEN cost > 0 THEN ROUND((("value" / cost) - 1), 2) ELSE NULL END AS roas,
  CASE WHEN funded > 0 THEN ROUND(cost / funded, 2) ELSE NULL END AS cpf,
  CASE WHEN funded > 0 THEN ROUND("value" / funded, 2) ELSE NULL END AS avg_funded_value
FROM campaign_level
ORDER BY cost DESC
```

**Output:** `data/{client}/enriched/{month}.csv` (campaign-level only)

```bash
python scripts/export_athena_data.py 2026-01
```

> **Limitation:** Because the source table has no GCLID, you **cannot** break down
> funded loans by ad group, keyword, or match type. You only know "Campaign X had
> Y funded loans." If you need keyword-level attribution, use Path B.

### Path B: GCLID Attribution (Keyword-Level), Recommended

> **Deep dive:** [`docs/gclid-attribution.md`](docs/gclid-attribution.md): full ADR
> covering the GCLID mechanism, design decisions, client ID mapping, input/output
> schemas, and tradeoffs.

This path produces real keyword-level funded-loan attribution by joining two
independent data sources on the GCLID (Google Click Identifier). It is a three-step
process.

#### Step 1: Click data from S3

The daily pipeline (`pipeline/google_ads_to_s3.py`) pulls click-level data from the
Google Ads API via the `click_view` resource. Each row represents a single click and
carries the GCLID plus the keyword, ad group, campaign, and geographic context for
that click.

**GAQL query and column mapping (from [`pipeline/google_ads_to_s3.py`](pipeline/google_ads_to_s3.py) lines 375–412):**

```python
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

# Each row mapped to:
{
    'date':          row.segments.date,
    'gclid':         row.click_view.gclid,
    'keyword':       row.click_view.keyword_info.text,
    'match_type':    row.click_view.keyword_info.match_type.name,
    'campaign_id':   str(row.campaign.id),
    'campaign_name': row.campaign.name,
    'ad_group_id':   str(row.ad_group.id),
    'ad_group_name': row.ad_group.name,
    'network':       row.segments.ad_network_type.name,
    'city':          row.click_view.area_of_interest.city,
    'region':        row.click_view.area_of_interest.region,
    'country':       row.click_view.area_of_interest.country,
}
```

These CSVs are stored at `s3://{bucket}/{client}/clicks/{date}.csv` and locally at
`data/{client}/clicks/{date}.csv`.

#### Step 2: Application data from Athena

`gclid_attribution.py` queries `prod.application_data` (**not**
`staging.google_ads_campaign_data`, that is Path A's table). It reads the GCLID
directly from the `click_id` column.

**Parameterized query (from [`scripts/gclid_attribution.py`](scripts/gclid_attribution.py) lines 44–57):**

```sql
SELECT
    click_id AS gclid,
    1 AS received,
    CASE WHEN approved = true THEN 1 ELSE 0 END AS approved,
    CASE WHEN funded = true THEN 1 ELSE 0 END AS funded,
    COALESCE(production_value, 0) AS production_value,
    COALESCE(lifetime_value, 0) AS lifetime_value,
    COALESCE(product_family, '') AS product_family
FROM prod.application_data
WHERE client_id = ?
    AND report_completion_timestamp >= CAST(? AS TIMESTAMP)
    AND report_completion_timestamp < CAST(? AS TIMESTAMP)
    AND click_id IS NOT NULL AND click_id != ''
```

A **scheduled version** replaces the date parameters with dynamic Athena functions:

```sql
WHERE ...
    AND report_completion_timestamp >= date_trunc('month', current_date - interval '1' month)
    AND report_completion_timestamp < date_trunc('month', current_date + interval '1' month)
```

Each row represents one loan application that had a GCLID in its `click_id` field.

#### Step 3: Inner join on GCLID

The two DataFrames are joined on the `gclid` column
([`scripts/gclid_attribution.py`](scripts/gclid_attribution.py) line 209):

```python
joined = clicks_df.merge(apps_df, on="gclid", how="inner")
```

Every matched application **inherits** the click's keyword, ad group, campaign, and
geographic data. Unmatched clicks (no application) and unmatched applications (no
click in the date range) are dropped.

The joined data is then aggregated at two levels
([lines 236–277](scripts/gclid_attribution.py)):

**Campaign-level aggregation:**

```python
campaign_agg = (
    joined.groupby(["campaign_id", "campaign_name"], as_index=False)
    .agg(
        apps=("received", "sum"),
        approved=("approved", "sum"),
        funded=("funded", "sum"),
        production=("production_value", "sum"),
        value=("lifetime_value", "sum"),
        # ... plus product-family breakdowns
    )
)
```

**Daily keyword-level aggregation:**

```python
group_cols = [
    "date", "campaign_id", "campaign_name",
    "ad_group_id", "ad_group_name", "keyword", "match_type",
]
daily_kw_agg = (
    joined.groupby(group_cols, as_index=False)
    .agg(
        apps=("received", "sum"),
        approved=("approved", "sum"),
        funded=("funded", "sum"),
        production=("production_value", "sum"),
        value=("lifetime_value", "sum"),
        # ... plus product-family breakdowns
    )
)
```

**Output files:**
- `data/{client}/enriched/{month}.csv`, campaign-level (comparable to Path A output)
- `data/{client}/enriched/daily/{month}.csv`, daily keyword-level (Path B exclusive)

```bash
# Single client
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01

# All configured clients
python scripts/gclid_attribution.py --all --month 2026-01

# Scheduled (no --month, uses last month via CURRENT_DATE)
python scripts/gclid_attribution.py --all
```

### Why This Matters

| Question | Path A (Athena Export) | Path B (GCLID Attribution) |
|----------|:-:|:-:|
| Campaign spent $X, funded Y loans | Yes | Yes |
| Which ad group drove the most funded loans? | **No** | Yes |
| Which keyword drove the most funded loans? | **No** | Yes |
| Cost per funded loan by keyword? | **No** | Yes |
| Daily funded trend by keyword? | **No** | Yes |
| Geographic attribution (city/region)? | **No** | Yes (click has city/region) |

Path A is a fallback for clients where click-level data is not yet collected or where
the ROI pipeline is the only available data source. **Path B (GCLID Attribution) is
the recommended approach** for all clients: it is the only way to answer "which
keyword is actually driving funded loans."

### Alternative: S3 Funded Data Import

Reads Digital Performance Ranking (DPR) files from S3. Campaign-level only (similar
granularity to Path A).

```bash
python scripts/import_s3_funded_data.py --client kitsap_cu --month 2026-01
```

**Script:** [`scripts/import_s3_funded_data.py`](scripts/import_s3_funded_data.py)

### Enrichment Status by Client

| Client | Has Enriched Data | Method | Granularity |
|--------|:-:|--------|--------|
| California Coast CU | Yes | Path B: GCLID Attribution | Keyword-level |
| First Commonwealth Bank | Yes | Path B: GCLID Attribution | Keyword-level |
| First Community CU | Yes | Path B: GCLID Attribution | Keyword-level |
| Kitsap CU | Yes | Path B: GCLID Attribution | Keyword-level |
| Public Service CU | Yes | Path B: GCLID Attribution | Keyword-level |
| Altura Ad Account | **No** | Config ready, run GCLID attribution | - |
| CommonWealth One FCU | **No** | Config ready, run GCLID attribution | - |

---

## Adding a New Client

New clients are **automatically onboarded** when added to the MCC. The pipeline:

1. Detects the new account via MCC query
2. Generates a `client_id` slug and SHA-256 dashboard token
3. Starts pulling data on the next daily run
4. Updates dashboard manifest files
5. Sends Slack notification to `#customer-success`

**To enable funded metrics** for a new client:

1. Add the client to [`config/clients.yaml`](config/clients.yaml) with `prod_id`, `s3_path`, and `athena_id`
2. Validate data availability: see [Dry Runs & Validation](#dry-runs--validation)
3. Run enrichment:
   ```bash
   python scripts/gclid_attribution.py --client {client_id} --month {YYYY-MM}
   ```
4. Commit enriched CSVs and push to deploy to the dashboard

---

## Dry Runs & Validation

Before running enrichment, validate that upstream data exists. See **[Dry Runs & Validation](docs/dry-runs/)** for the full list of commands and troubleshooting steps.

---

## Setup

Install dependencies, configure credentials, and generate OAuth tokens. See **[Setup Guide](docs/setup.md)**.

---

## CLI Reference

Pipeline commands, enrichment scripts, and utilities. See **[CLI Reference](docs/cli-reference.md)**.

---

## Project Structure

```
google_ads_to_s3/
├── index.html                              # Dashboard (GitHub Pages)
├── data-manifest.json                      # Available months per client
├── clients/
│   └── README.md                           # Dashboard tokens and URLs
├── config/
│   ├── config.example.yaml                 # Credential template
│   ├── config.yaml                         # Real credentials (gitignored)
│   └── clients.yaml                        # Client config (prod_id, s3_path, etc.)
├── dags/
│   └── google_ads_to_s3_dag.py             # Airflow DAG (daily @ 8 AM UTC)
├── pipeline/
│   ├── google_ads_to_s3.py                 # Main pipeline class + CLI
│   └── slack.py                            # Slack notifications
├── scripts/
│   ├── gclid_attribution.py                # GCLID-based keyword-level attribution
│   ├── import_s3_funded_data.py            # Import funded data from S3 DPR files
│   ├── export_athena_data.py               # Export enriched data from Athena
│   ├── generate_daily_attribution.py       # Proportional daily attribution (fallback)
│   ├── aggregate_monthly.py                # Aggregate daily CSVs → monthly
│   ├── export_insights_data.py             # Pull search terms, channels, devices, geo, negative keywords
│   ├── sync_registry.py                    # Sync S3 registry → local dashboard files
│   ├── export_account_ids.py               # List MCC child accounts
│   └── generate_refresh_token.py           # OAuth setup helper
├── docs/
│   ├── pipeline-walkthrough.md             # Detailed architecture walkthrough
│   ├── gclid-attribution.md                # GCLID attribution ADR
│   ├── setup.md                            # Installation & credential setup
│   ├── cli-reference.md                    # Pipeline, enrichment & utility commands
│   ├── future_state.md                     # Attribution, optimization & retargeting roadmap
│   └── dry-runs/                           # Validation commands & troubleshooting
│       └── README.md
├── data/                                   # Per-client CSV data (served by GitHub Pages)
│   └── {client_id}/
│       ├── campaigns/{month}.csv
│       ├── keywords/{month}.csv
│       ├── daily/{month}.csv
│       ├── enriched/{month}.csv
│       ├── search_terms/{month}.csv
│       ├── channels/{month}.csv
│       ├── devices/{month}.csv
│       ├── locations/{month}.csv
│       └── negative_keywords/{month}.csv    # Account state snapshot; dashboard loads latest
├── output/                                 # Local daily CSVs (gitignored)
├── requirements.txt
└── .gitignore
```

---

## Further Reading

- [Pipeline Architecture Walkthrough](docs/pipeline-walkthrough.md): detailed team-facing breakdown of every component
- [Future State Roadmap](docs/future_state.md): multi-model attribution, ad group optimization, retargeting
- [Dry Runs & Validation](docs/dry-runs/): commands to verify data before running enrichment

---

Proprietary - Alpharank
