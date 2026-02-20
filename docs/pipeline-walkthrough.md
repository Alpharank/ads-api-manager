# Pipeline Architecture Walkthrough

Detailed breakdown of how the Google Ads to S3 pipeline works, what data it captures, how the dashboard displays it, and how it scales.

For a concise overview, see the [main README](../README.md).

---

## Table of Contents

- [1. End-to-End Daily Process](#1-end-to-end-daily-process)
- [2. What Data Gets Captured](#2-what-data-gets-captured)
- [3. GCLID Attribution Pipeline](#3-gclid-attribution-pipeline)
- [4. How the Dashboard Works](#4-how-the-dashboard-works)
- [5. Data Volume & Scaling](#5-data-volume--scaling)
- [6. Attribution Bridge to ROI Pipeline](#6-attribution-bridge-to-roi-pipeline)
- [7. EC2 Cost Optimization](#7-ec2-cost-optimization)
- [8. Architecture Recommendation](#8-architecture-recommendation)

---

## 1. End-to-End Daily Process

### DAG: `google_ads_to_s3_daily`

Runs daily at **8:00 AM UTC (3 AM EST)** via Airflow. Source: [`dags/google_ads_to_s3_dag.py`](../dags/google_ads_to_s3_dag.py)

```
8:00 AM UTC   Airflow DAG kicks off:

  ┌─────────────────────┐
  │  discover_accounts  │  Query MCC for all child accounts
  └────────┬────────────┘
           │
     ┌─────┴──────┐
     │            │
     ▼            ▼
  ┌──────────┐  ┌──────────────────────────┐
  │ pull ×N  │  │ notify_account_changes   │  Slack #customer-success
  │ (parallel│  └──────────────────────────┘
  │  per     │
  │ account) │
  └────┬─────┘
       │
       ▼
  ┌────────────────────────┐
  │ update_dashboard_files │  Rebuild clients.json + data-manifest.json
  └────────┬───────────────┘
           │
           ▼
  ┌──────────────────────────┐
  │ export_for_attribution   │  Write monthly CSV for ROI pipeline
  └──────────────────────────┘

~8:30 AM UTC   DAG completes
11:30 AM UTC   ROI attribution pipeline reads the exported file
```

### Step Details

**discover_accounts** — Queries the Google Ads MCC for every enabled child account. Compares against an S3 registry ([`_registry/accounts.json`](#s3-registry)). New accounts get an auto-generated slug and dashboard token. Removed accounts get a `removed_at` timestamp and are skipped.

**pull_account (×N in parallel)** — For each active account, the [pipeline](../pipeline/google_ads_to_s3.py) pulls three datasets via the Google Ads API and writes them to S3:

| Dataset | What it Contains | S3 Path |
|---------|-----------------|---------|
| Campaigns | Daily campaign-level metrics | `ad-spend-reports/{client}/campaigns/{date}.csv` |
| Keywords | Keyword-level metrics within each ad group | `ad-spend-reports/{client}/keywords/{date}.csv` |
| Clicks | Individual click events with GCLIDs and geo | `ad-spend-reports/{client}/clicks/{date}.csv` |

**update_dashboard_files** — Rebuilds `clients.json` (token → client mapping) and `data-manifest.json` (available months per client) so the GitHub Pages dashboard picks up new data.

**export_for_attribution** — Reads all daily campaign CSVs for the current month, concatenates them, renames columns to match the ROI pipeline format, and writes to S3. See [Attribution Bridge](#6-attribution-bridge-to-roi-pipeline).

**notify_account_changes** — Sends a Slack alert to `#customer-success` when accounts are added to or removed from the MCC.

---

## 2. What Data Gets Captured

Three layers of Google Ads data, each more granular than the last.

### Layer 1: Campaign (What Attribution Uses Today)

```
date, campaign_id, campaign_name, campaign_status, impressions, clicks, cost, conversions
```

One row per campaign per day. This is what the ROI pipeline reads.

### Layer 2: Keyword (10x More Granular)

```
date, campaign_id, campaign_name, ad_group_id, ad_group_name, keyword, match_type,
impressions, clicks, cost, conversions
```

One row per keyword per day. Shows which keywords within each ad group drive performance.

### Layer 3: Click / GCLID (100-1000x More Granular)

```
date, gclid, keyword, match_type, campaign_id, campaign_name,
ad_group_id, ad_group_name, network, city, region, country
```

One row per individual click. The `gclid` is the key for joining with first-party loan data — when a click turns into a funded loan, the origination system captures the GCLID.

### What Each Layer Unlocks

```
Campaign ──► "Brand campaign spent $80 and got 24 conversions"
    │
    ▼
Keyword  ──► "Within Brand, the keyword 'best personal loan rates' got 12 clicks at $4.50"
    │
    ▼
Click    ──► "This click from Portland, OR searched 'personal loan rates' — GCLID: EAIaIQ..."
    │
    ▼
GCLID Join ► "That click became a funded $25,000 personal loan"
```

---

## 3. GCLID Attribution Pipeline

The [`gclid_attribution.py`](../scripts/gclid_attribution.py) script joins click-level GCLID data from S3 with application data from Athena to produce real keyword-level attribution.

### How It Works

```
┌────────────────────────────────────────────┐
│  S3: ad-spend-reports/{client}/clicks/     │
│  ┌──────────────────────────────────────┐  │
│  │ date, gclid, keyword, campaign_id,  │  │
│  │ ad_group_id, match_type, city, ...  │  │
│  └──────────────────────────────────────┘  │
└──────────────────┬─────────────────────────┘
                   │  Inner join on GCLID
┌──────────────────┴─────────────────────────┐
│  Athena: prod.application_data             │
│  ┌──────────────────────────────────────┐  │
│  │ click_id (GCLID, direct column)     │  │
│  │ funded_status, product_family,       │  │
│  │ funded_amount, lifetime_value        │  │
│  └──────────────────────────────────────┘  │
└──────────────────┬─────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────┐
│  Output:                                    │
│  data/{client}/enriched/{month}.csv        │  Campaign-level totals
│  data/{client}/enriched/daily/{month}.csv  │  Daily keyword-level
└────────────────────────────────────────────┘
```

### Output Schema

**Campaign-level** (`enriched/{month}.csv`):

| Column | Description |
|--------|-------------|
| `campaign_id` | Google Ads campaign ID |
| `campaign_name` | Campaign name |
| `apps` | Total applications received |
| `approved` | Approved applications |
| `funded` | Funded applications |
| `production` | Production value |
| `value` | Lifetime value |
| `cpf` | Cost per funded |
| `avg_funded_value` | Revenue per funded app |
| `cc_appvd` | Credit card approvals |
| `deposit_appvd` | Deposit/checking approvals |
| `personal_appvd` | Personal loan approvals |
| `vehicle_appvd` | Vehicle loan approvals |
| `heloc_appvd` | HELOC/home equity approvals |

**Daily keyword-level** (`enriched/daily/{month}.csv`) adds: `date`, `ad_group_id`, `ad_group_name`, `keyword`, `match_type`.

### Running It

```bash
# Dry run — validate data, write nothing
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01 --dry-run

# Run for one client
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01

# Run for all clients
python scripts/gclid_attribution.py --all --month 2026-01

# Scheduled (no --month, auto-computes last month via CURRENT_DATE)
python scripts/gclid_attribution.py --all
```

For more validation commands, see [`docs/dry-runs/`](dry-runs/).

### Alternative Enrichment Methods

| Method | Script | Source | Granularity |
|--------|--------|--------|-------------|
| **GCLID Attribution** | [`gclid_attribution.py`](../scripts/gclid_attribution.py) | S3 clicks + Athena apps | Daily keyword-level |
| **S3 Funded Import** | [`import_s3_funded_data.py`](../scripts/import_s3_funded_data.py) | S3 DPR files | Campaign-level |
| **Athena Export** | [`export_athena_data.py`](../scripts/export_athena_data.py) | staging.google_ads_campaign_data | Campaign-level |

---

## 4. How the Dashboard Works

The [dashboard](../index.html) is a single-page app deployed to GitHub Pages. Each client gets a unique URL with a SHA-256 token. No login — valid token loads the dashboard, invalid token shows an error.

### Dashboard Layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  [=] AlphaRank  │  Client Name  │                  Month: Jan 2026 ▼│
├─────────┬────────────────────────────────────────────────────────────┤
│         │  Impr │ Clicks │ CTR │ Cost │ CPA │ Funded │ Rev │ CPF    │
│         │  ════════════════════════════════════════════════════════   │
│Campaigns│                                                            │
│Ad groups│  ┌──────────────────────┐  ┌──────────────────────┐       │
│─────────│  │  Multi-Metric Chart  │  │  Funded Over Time    │       │
│Keywords │  │  (select up to 3)    │  │                      │       │
│Search   │  └──────────────────────┘  └──────────────────────┘       │
│ terms   │                                                            │
│Negative │  ┌────────────────────────────────────────────────┐       │
│ keywords│  │  Data Table                                     │       │
│─────────│  │  (sortable, filterable, click rows to drill)   │       │
│Channels │  └────────────────────────────────────────────────┘       │
│When &   │                                                            │
│ Where   │                                                            │
├─────────┤                                                            │
│Selection│                                                            │
└─────────┴────────────────────────────────────────────────────────────┘
```

### KPI Strip

Nine clickable chips. Click up to 3 to overlay on the left chart:

| Metric | Source | Description |
|--------|--------|-------------|
| **Impr.** | Google Ads | Impressions |
| **Clicks** | Google Ads | Click count |
| **CTR** | Derived | Clicks / Impressions |
| **Cost** | Google Ads | Ad spend (selected by default) |
| **CPA** | Derived | Cost / Conversions |
| **Funded** | First-party (enriched) | Funded loan applications |
| **Funded Rev** | First-party (enriched) | Revenue from funded loans |
| **CPF** | Derived | Cost / Funded |
| **Avg Fund Val** | Derived | Revenue / Funded |

The first 5 come from Google Ads data. The last 4 come from first-party enriched data. Without enriched data, those chips show `--`.

### Charts

**Left chart** — Multi-metric. Shows whichever KPI chips are selected (up to 3 overlaid). Supports line/bar mode, daily/day-of-week aggregation, and date range filtering (30/60/90 days or this month). When Cost is selected and funded data exists, a dotted gold line overlays funded revenue.

**Right chart** — Always shows funded applications over time.

### Drill-Down Navigation

Click any table row to drill into it:

```
All Campaigns                    ← sidebar: "Campaigns" active
  │  click row
  ▼
Campaign: KCU - Personal - Brand ← sidebar: "Ad groups", breadcrumb appears
  │  click row                     KPIs + charts scoped to this campaign
  ▼
Ad Group: Personal Loans          ← sidebar: "Keywords", breadcrumb extends
  │                                 KPIs + charts scoped to this ad group
  └── Keywords table (leaf level)
```

Breadcrumbs navigate back. Selection chips in the sidebar show the current drill path with x buttons to clear.

### Six Sidebar Views

| View | What it Shows | Drill-Down? |
|------|--------------|-------------|
| **Campaigns** | Campaign-level metrics | Yes → Ad groups |
| **Ad groups** | Ad group metrics within a campaign | Yes → Keywords |
| **Keywords** | Keyword performance with match type badges | No (leaf level) |
| **Search terms** | Actual search queries that triggered ads | No |
| **Negative keywords** | Ad-group-level negative keyword exclusions | No |
| **Channels** | Network breakdown (Search, Display, YouTube) | No |
| **When & Where** | Device type + geographic performance | No |

### Filtering Controls

| Filter | Location | Effect |
|--------|----------|--------|
| Text search | Toolbar | Filters table rows by name |
| Match type toggles | Toolbar (keywords only) | Show/hide Broad, Phrase, Exact |
| Month selector | Header | Loads different month of data |
| Date range | Toolbar | Charts show 30/60/90 days or current month |
| Chart view | Toolbar | Daily actuals vs. day-of-week averages |
| Compact mode | Toolbar | `1,234,567` → `1.2M` |

### Data Files Loaded

```
data/{client}/campaigns/{month}.csv     ← required
data/{client}/keywords/{month}.csv
data/{client}/daily/{month}.csv
data/{client}/enriched/{month}.csv      ← enables funded metrics
data/{client}/search_terms/{month}.csv
data/{client}/channels/{month}.csv
data/{client}/devices/{month}.csv
data/{client}/locations/{month}.csv
data/{client}/negative_keywords/{month}.csv ← account state snapshot
```

A [`data-manifest.json`](../data-manifest.json) tells the dashboard which months have data. For 90-day chart spans, it preloads up to 3 past months in parallel.

### What's Not Live Yet

**Attribution model selector** — The UI has Last Click / First Click / Linear buttons (archived/commented out). Only last-click is active. The code is ready to load different CSV files per model once multi-model attribution is implemented. See [`docs/future_state.md`](future_state.md).

---

## 5. Data Volume & Scaling

### Per Client Per Day (Real Numbers, 2026-02-11)

| Client | Campaign Rows | Keyword Rows | Click Rows |
|--------|:--:|:--:|:--:|
| Altura Ad Account | 8 | 5 | 48,573 |
| California Coast CU | 10 | 108 | 1,531 |
| CommonWealth One FCU | 0 | 0 | 0 |
| First Commonwealth Bank | 6 | 18 | 57,930 |
| First Community CU | 1 | 61 | 98 |
| Kitsap CU | 14 | 96 | 10,862 |
| Public Service CU | 13 | 25 | 422 |
| **Total** | **52** | **313** | **119,416** |

The ROI attribution pipeline reads **52 rows/day**. We store **119,781 rows/day**. The extra granularity powers GCLID attribution and geographic insights.

### High-Traffic Accounts

Three accounts generate **99% of all click data**:

| Client | Clicks/Day | % of Total |
|--------|:--:|:--:|
| First Commonwealth Bank | 57,930 | 49% |
| Altura Ad Account | 48,573 | 41% |
| Kitsap CU | 10,862 | 9% |
| All others | 2,051 | 1% |

### Scaling to 70 Clients

| Metric | 7 Clients (Today) | 70 Clients (Target) |
|--------|:--:|:--:|
| Campaign rows/day | 52 | ~500 |
| Keyword rows/day | 313 | ~4,000 |
| Click rows/day | 119,416 | ~500K–2M |
| Daily CSV storage | ~30 MB | ~150–500 MB |
| Google Ads API calls | 21 | 210 |
| DAG runtime | ~1–2 min | ~20–40 min |

The bottleneck is **Google Ads API latency** (2–5s per call), not compute. Airflow runs accounts in parallel via dynamic task mapping, so 70 accounts with parallelism of 16 takes ~70 seconds for the API phase.

---

## 6. Attribution Bridge to ROI Pipeline

### Before (Manual Monthly)

1. Someone manually exports campaign data from S3
2. Reformats columns to match the ROI pipeline
3. Uploads to a different S3 path
4. ROI pipeline runs hours later — works or fails silently

### After (Automated Daily)

The `export_for_attribution` task in the [DAG](../dags/google_ads_to_s3_dag.py) runs automatically after every daily pull:

```
ad-spend-reports/{client}/campaigns/{date}.csv  (daily files)
        │
        ▼  Concat all days in month
        │
        ▼  Rename columns: campaign_id → Campaign ID, campaign_name → Ad group
        │
        ▼  Prepend 2 blank rows (ROI pipeline reads with skiprows=2)
        │
adspend_reports/{client}_{month}_daily.csv
        │
        ▼  ROI pipeline (11:30 AM UTC) reads this file
        │
staging.google_ads_campaign_data (Athena)
        │
        ▼  Powers ROI reporting
        │
"Campaign X spent $5,000 and generated $50,000 in funded loans"
```

### Timing

| Pipeline | Schedule | Purpose |
|----------|----------|---------|
| `google_ads_to_s3_daily` | 8:00 AM UTC | Pull data, rebuild dashboards, export attribution file |
| `update_google_ads_roi` | 11:30 AM UTC | Read attribution file, join with app data, write to staging |

The export finishes hours before the ROI pipeline reads it.

---

## 7. EC2 Cost Optimization

The pipeline runs on `auto-attribution-prod` (m5zn.6xlarge — 24 vCPUs, 192 GB RAM) at **~$1.18/hour**.

### Current: Always Running

```
24 hours × $1.18/hr × 30 days = ~$850/month
```

### Ready to Enable: Start/Stop Automation

An AWS Lambda and EventBridge rule exist but are **disabled**. The DAG also has a commented-out `stop_instance` task:

```
EventBridge (7:50 AM UTC) → Lambda → Start EC2
Airflow DAG runs (8:00 AM UTC)
DAG final task → Stop EC2

~0.7 hours × $1.18/hr × 30 days = ~$25/month   (saves ~$825/month)
```

To enable:
1. `aws events enable-rule --name start-airflow-ec2-daily --region us-west-2`
2. Uncomment `stop_instance` task in [`dags/google_ads_to_s3_dag.py`](../dags/google_ads_to_s3_dag.py)

---

## 8. Architecture Recommendation

### Stick with Airflow DAGs

**The workload doesn't need distributed compute.** The pipeline's compute step — concat CSVs and rename columns — processes ~16K rows/day in milliseconds. Even at 70 clients, pandas handles this in under 5 seconds on a single core.

**The bottleneck is I/O, not compute.** 90% of runtime is waiting on Google Ads API responses. Switching to Spark doesn't make API calls faster.

| | Airflow + pandas | Glue + PySpark |
|--|:--|:--|
| Cold start | 0 (already running) | 1–2 min (Spark cluster spin-up) |
| Min cost per run | ~$0.01 | ~$0.15 (2 DPUs × $0.44/hr, 10-min min) |
| Monthly cost | ~$25 (with start/stop) | ~$4.50+ (no orchestration) |
| Orchestration | Built-in | Need Step Functions or Airflow anyway |
| Debugging | SSH, check logs | CloudWatch, less interactive |

### When to Revisit

Move to Glue/PySpark **if and when**:
1. **Click-level GCLID joins at scale** — 1M+ rows/day with multi-touch attribution windows
2. **Backfills across years** — hundreds of millions of rows in one shot
3. **Real-time needs** — hourly attribution instead of daily

**None of these apply today.**

### Higher-ROI Investments

1. **GCLID → funded loan matching** — already implemented via [`gclid_attribution.py`](../scripts/gclid_attribution.py)
2. **Keyword + click data in attribution** — data is in S3, expand ROI pipeline to use it
3. **Monitoring and alerting** — did the export land? Did the ROI pipeline succeed?

---

## Related Documentation

- [Main README](../README.md) — concise project overview
- [Multi-Model Attribution Roadmap](future_state.md) — first-click / linear attribution future state
- [Dry Runs & Validation](dry-runs/) — commands to verify data before running enrichment
- [Client Dashboard Tokens](../clients/README.md) — per-client URLs and tokens
