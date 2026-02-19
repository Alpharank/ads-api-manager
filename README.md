# Google Ads to S3 Pipeline

Pulls daily Google Ads data, enriches it with first-party attribution, and serves per-client dashboards via GitHub Pages.

**Dashboard:** `https://alpharank.github.io/Google-Ads-Automation/?client=TOKEN` — see [`clients/`](clients/) for tokens

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
│  2. Pull campaigns + keywords + clicks per account  ──► S3      │
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

The [pipeline](pipeline/google_ads_to_s3.py) pulls three datasets per account per day:

| Dataset | Description | S3 Path |
|---------|-------------|---------|
| Campaigns | Daily spend, clicks, conversions per campaign | `ad-spend-reports/{client}/campaigns/{date}.csv` |
| Keywords | Performance by keyword within each ad group | `ad-spend-reports/{client}/keywords/{date}.csv` |
| Clicks | Individual click events with GCLIDs + geo | `ad-spend-reports/{client}/clicks/{date}.csv` |

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
data/{client}/campaigns/{month}.csv     ← campaign metrics (required)
data/{client}/keywords/{month}.csv      ← keyword metrics
data/{client}/daily/{month}.csv         ← daily timeseries
data/{client}/enriched/{month}.csv      ← funded/attribution data
data/{client}/search_terms/{month}.csv  ← search query data
data/{client}/channels/{month}.csv      ← network breakdown
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
│─────────│                                                            │
│Channels │  ┌────────────────────────────────────────────────┐       │
│When &   │  │  Sortable, Filterable Data Table               │       │
│ Where   │  │  (click any row to drill down)                 │       │
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

Three methods to add first-party funded data. All produce `data/{client}/enriched/{month}.csv`.

### Method 1: GCLID Attribution (Recommended)

Real keyword-level attribution — joins click GCLIDs from S3 with application data from Athena.

```
S3: clicks/{date}.csv (gclid, keyword, campaign)
                    │
                    ├──► Inner join on GCLID
                    │
Athena: prod.application_data (gclid, funded, value)
                    │
                    ▼
        data/{client}/enriched/{month}.csv          (campaign-level)
        data/{client}/enriched/daily/{month}.csv    (daily keyword-level)
```

```bash
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01
python scripts/gclid_attribution.py --all --month 2026-01
```

**Script:** [`scripts/gclid_attribution.py`](scripts/gclid_attribution.py)

### Method 2: S3 Funded Data Import

Reads Digital Performance Ranking (DPR) files from S3. Campaign-level only.

```bash
python scripts/import_s3_funded_data.py --client kitsap_cu --month 2026-01
```

**Script:** [`scripts/import_s3_funded_data.py`](scripts/import_s3_funded_data.py)

### Method 3: Athena Export

Queries `staging.google_ads_campaign_data` (populated by the ROI pipeline).

```bash
python scripts/export_athena_data.py 2026-01
```

**Script:** [`scripts/export_athena_data.py`](scripts/export_athena_data.py)

### Enrichment Status by Client

| Client | Has Enriched Data | Method |
|--------|:-:|--------|
| California Coast CU | Yes | GCLID Attribution |
| First Commonwealth Bank | Yes | GCLID Attribution |
| First Community CU | Yes | GCLID Attribution |
| Kitsap CU | Yes | GCLID Attribution |
| Public Service CU | Yes | GCLID Attribution |
| Altura Ad Account | **No** | Config ready — run GCLID attribution |
| CommonWealth One FCU | **No** | Config ready — run GCLID attribution |

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
2. Validate data availability — see [Dry Runs & Validation](#dry-runs--validation)
3. Run enrichment:
   ```bash
   python scripts/gclid_attribution.py --client {client_id} --month {YYYY-MM}
   ```
4. Commit enriched CSVs and push to deploy to the dashboard

---

## Dry Runs & Validation

Before running enrichment, validate that upstream data exists.

```bash
# GCLID attribution dry run — checks S3 clicks + Athena apps, prints stats, writes nothing
python scripts/gclid_attribution.py --client {client_id} --month 2026-01 --dry-run
```

For the full list of validation commands and troubleshooting steps, see **[`docs/dry-runs/`](docs/dry-runs/)**.

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Credentials

```bash
cp config/config.example.yaml config/config.yaml
```

Edit with Google Ads and AWS credentials. See [`config/config.example.yaml`](config/config.example.yaml).

### 3. Generate OAuth Token (if needed)

```bash
python scripts/generate_refresh_token.py
```

---

## CLI Reference

### Pipeline

```bash
python pipeline/google_ads_to_s3.py                               # Pull yesterday's data
python pipeline/google_ads_to_s3.py --date 2026-01-15             # Pull specific date
python pipeline/google_ads_to_s3.py --backfill 90                 # Last 90 days
python pipeline/google_ads_to_s3.py --client kitsap_cu --backfill 7  # Single client
python pipeline/google_ads_to_s3.py --list-accounts               # List MCC accounts
```

### Enrichment

```bash
python scripts/gclid_attribution.py --client {id} --month 2026-01           # GCLID attribution
python scripts/gclid_attribution.py --all --month 2026-01 --dry-run         # Dry run all
python scripts/import_s3_funded_data.py --client {id} --month 2026-01       # S3 funded import
python scripts/export_athena_data.py 2026-01                                # Athena export
python scripts/export_athena_data.py --list-clients                         # List Athena clients
```

### Utilities

```bash
python scripts/aggregate_monthly.py               # Aggregate daily CSVs → monthly
python scripts/export_insights_data.py             # Pull search terms, channels, devices, locations
python scripts/sync_registry.py                    # Preview dashboard file sync
python scripts/sync_registry.py --commit           # Sync + git commit + push
python scripts/export_account_ids.py               # List all MCC child accounts
```

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
│   ├── export_insights_data.py             # Pull search terms, channels, devices, geo
│   ├── sync_registry.py                    # Sync S3 registry → local dashboard files
│   ├── export_account_ids.py               # List MCC child accounts
│   └── generate_refresh_token.py           # OAuth setup helper
├── docs/
│   ├── pipeline-walkthrough.md             # Detailed architecture walkthrough
│   ├── future_state.md                     # Multi-model attribution roadmap
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
│       └── locations/{month}.csv
├── output/                                 # Local daily CSVs (gitignored)
├── requirements.txt
└── .gitignore
```

---

## Further Reading

- [Pipeline Architecture Walkthrough](docs/pipeline-walkthrough.md) — detailed team-facing breakdown of every component
- [Multi-Model Attribution Roadmap](docs/future_state.md) — future state for first-click / linear attribution
- [Dry Runs & Validation](docs/dry-runs/) — commands to verify data before running enrichment

---

Proprietary - Alpharank
