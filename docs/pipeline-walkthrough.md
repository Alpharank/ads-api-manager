# Google Ads Pipeline: Team Walkthrough

## What this document covers

1. What the pipeline does end-to-end
2. How the dashboard works and drill-down navigation
3. The three data layers (campaign → keyword → click)
4. How much data we store vs. what we actually use today
5. How it scales as we onboard more clients
6. How the attribution bridge works
7. EC2 cost optimization
8. Recommendation: Airflow DAGs vs. PySpark/Glue

---

## 1. End-to-End Process

### The daily cycle

Every day at **7:50 AM UTC**, an EventBridge rule fires a Lambda that starts the EC2 instance (`auto-attribution-prod`). Ten minutes later, Airflow wakes up and runs the DAG:

```
7:50 AM UTC   EventBridge → Lambda → Start EC2 instance
8:00 AM UTC   Airflow DAG kicks off:

  ┌─────────────────────┐
  │  discover_accounts  │  Query MCC for all child accounts
  └────────┬────────────┘
           │
     ┌─────┴──────┐
     │            │
     ▼            ▼
  ┌──────────┐  ┌──────────────────────────┐
  │ pull ×N  │  │ notify_account_changes   │  Slack alert if accounts added/removed
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
  └────────┬─────────────────┘
           │
           ▼
  ┌───────────────┐
  │ stop_instance │  Shut down EC2 to save cost
  └───────────────┘

~8:30 AM UTC   Instance stops
11:30 AM UTC   ROI attribution pipeline reads the exported file
```

The instance only runs for ~30–40 minutes per day.

### What happens at each step

**discover_accounts** — Queries the Google Ads MCC (Manager Account) for every enabled child account. Compares against an S3 registry. If a new account appears under the MCC, it automatically generates a slug, a dashboard token, and starts pulling data for it. No config changes needed. If an account disappears from the MCC, it's marked as removed and skipped.

**pull_account (×N in parallel)** — For each active account, pulls three datasets from the Google Ads API for yesterday's date and writes them to S3 as CSVs:

| Dataset | What it contains | S3 path |
|---------|-----------------|---------|
| Campaigns | Daily campaign-level metrics | `ad-spend-reports/{client}/campaigns/YYYY-MM-DD.csv` |
| Keywords | Keyword-level metrics within each ad group | `ad-spend-reports/{client}/keywords/YYYY-MM-DD.csv` |
| Clicks | Individual click events with GCLIDs and geo | `ad-spend-reports/{client}/clicks/YYYY-MM-DD.csv` |

**update_dashboard_files** — Rebuilds two JSON files that power the GitHub Pages dashboard: `clients.json` (token → client mapping) and `data-manifest.json` (which months have data per client).

**export_for_attribution** — This is the bridge to the ROI pipeline. It reads all the daily campaign CSVs for the current month, concatenates them into one file, renames the columns, and writes it to a different S3 path in the format the ROI pipeline expects. This replaced a manual monthly export that someone had to run by hand.

**stop_instance** — Stops the EC2 instance. Uses `trigger_rule="all_done"` so the instance stops even if upstream tasks fail.

---

## 2. How the Dashboard Works

The dashboard is a single-page app (`index.html`) deployed to GitHub Pages. Each client gets a unique URL with a SHA-256 token:

```
https://alpharank.github.io/Google-Ads-Automation/?client=0f72e02a...
```

No login page — if the token is valid, the dashboard loads. If not, it shows an error. Tokens are generated automatically when accounts are discovered.

### What you see when it loads

```
┌──────────────────────────────────────────────────────────────────────┐
│  AlphaRank  │  California Coast Credit Union  │  Data Month: Jan ▼  │
├─────────┬────────────────────────────────────────────────────────────┤
│         │  Impr. │ Clicks │ CTR │ Cost │ CPA │ Funded │ Rev │ CPF  │
│ CAMPAIGNS│  ══════════════════════════════════════════════════════   │
│ Ad groups│                                                          │
│─────────│  ┌─────────────────────┐  ┌──────────────────────┐       │
│ KEYWORDS │  │  Multi-Metric Chart │  │   Funded Chart       │       │
│ Search   │  │  (up to 3 metrics)  │  │   (apps over time)   │       │
│  terms   │  └─────────────────────┘  └──────────────────────┘       │
│─────────│                                                          │
│ INSIGHTS │  ┌────────────────────────────────────────────────┐      │
│ Channels │  │  Data Table                                     │      │
│ When &   │  │  (sortable, filterable, clickable rows)        │      │
│  Where   │  └────────────────────────────────────────────────┘      │
├─────────┤                                                          │
│Selection:│                                                          │
│ Campaign │                                                          │
│ Ad Group │                                                          │
└─────────┴────────────────────────────────────────────────────────────┘
```

### KPI strip (top row)

Nine clickable metric chips across the top. Click up to 3 at a time to overlay them on the left chart:

| Metric | Source | What it shows |
|--------|--------|--------------|
| **Impr.** | Google Ads | Impressions |
| **Clicks** | Google Ads | Click count |
| **CTR** | Derived | Clicks / Impressions |
| **Cost** | Google Ads | Ad spend (selected by default) |
| **CPA** | Derived | Cost / Conversions |
| **Funded** | First-party (Athena) | Funded loan applications |
| **Funded Rev** | First-party (Athena) | Revenue from funded loans |
| **CPF** | Derived | Cost per funded app |
| **Avg Fund Val** | Derived | Revenue per funded app |

The first 5 metrics come from Google Ads data. The last 4 come from first-party attribution data (joined via the ROI pipeline through Athena). If attribution data isn't available for a client, those chips show "--".

### Charts (two side-by-side)

**Left chart** — Multi-metric. Shows whichever KPI chips are selected (up to 3 overlaid). Supports line or bar mode, daily or day-of-week aggregation, and date range filtering (last 30/60/90 days or this month). When Cost is selected and funded data exists, a dotted gold line overlays funded revenue for comparison.

**Right chart** — Always shows funded applications over time. Dedicated view so you can see funded volume alongside whatever you're exploring on the left.

### Drill-down navigation

This is the key interactive feature. **Click any row in a table to drill into it:**

```
All Campaigns                    ← sidebar shows "Campaigns" active
  │
  │  click "KCU - Personal - Brand" row
  ▼
Campaign: KCU - Personal - Brand ← sidebar shows "Ad groups", breadcrumb appears
  │
  │  click "Personal Loans" ad group row
  ▼
Ad Group: Personal Loans          ← sidebar shows "Keywords", breadcrumb extends
  │
  └── Keywords table shows all keywords in this ad group
```

**What happens when you drill down:**
- The KPI strip recalculates to show metrics for just the selected campaign/ad group
- Both charts re-render scoped to the selection
- The data table switches to the next level (campaigns → ad groups → keywords)
- A breadcrumb trail appears: `All campaigns > KCU - Personal - Brand > Personal Loans`
- The sidebar selection panel shows the current drill path with × buttons to go back

**Breadcrumb navigation** — Click any level in the breadcrumb to jump back. Click × on the sidebar selection chips to clear.

### Six views (left sidebar)

| View | What it shows | Drill-down? |
|------|--------------|-------------|
| **Campaigns** | Campaign-level metrics, cost, conversions | Yes → Ad groups |
| **Ad groups** | Ad group metrics within a campaign | Yes → Keywords |
| **Keywords** | Keyword performance with match type badges | No (leaf level) |
| **Search terms** | Actual search queries that triggered ads | No |
| **Channels** | Network breakdown (Search, Display, YouTube) | No |
| **When & Where** | Device type + geographic location performance | No |

### Filtering

| Filter | Where | What it does |
|--------|-------|-------------|
| **Text search** | Top toolbar | Filters table rows by name (case-insensitive partial match) |
| **Match type toggles** | Top toolbar (keywords/search terms only) | Show/hide Broad, Phrase, Exact keywords |
| **Month selector** | Header dropdown | Loads entirely different month of data |
| **Date range** | Toolbar dropdown | Filters charts to 30/60/90 days or current month |
| **Chart view** | Toolbar dropdown | Daily actuals vs. day-of-week averages |
| **Compact mode** | Toolbar toggle | Switches numbers from `1,234,567` to `1.2M` |

### How data gets into the dashboard

The dashboard loads CSV files from the same GitHub Pages origin:

```
data/{client_id}/campaigns/{YYYY-MM}.csv      ← campaign metrics (required)
data/{client_id}/keywords/{YYYY-MM}.csv       ← keyword metrics
data/{client_id}/daily/{YYYY-MM}.csv          ← daily campaign aggregation
data/{client_id}/enriched/{YYYY-MM}.csv       ← first-party attribution data
data/{client_id}/search_terms/{YYYY-MM}.csv   ← search term data
data/{client_id}/channels/{YYYY-MM}.csv       ← network breakdown
data/{client_id}/devices/{YYYY-MM}.csv        ← device breakdown
data/{client_id}/locations/{YYYY-MM}.csv      ← geographic data
```

A `data-manifest.json` tells the dashboard which months have data per client. For charts spanning 90 days, it preloads up to 3 additional past months in parallel.

### What's not live yet

**Attribution model selector** — The UI has Last Click / First Click / Linear buttons, but they're archived (commented out). Currently only last-click attribution is active. The code is ready to load different CSV files per model (e.g., `enriched/2025-01_first_click.csv`) once the multi-model attribution pipeline is implemented.

---

## 3. The Three Data Layers

The Google Ads API gives us data at three levels of granularity. Think of it as a drill-down:

### Layer 1: Campaign (what attribution uses today)

```
date, campaign_id, campaign_name, campaign_status, impressions, clicks, cost, conversions
2026-02-04, 23228929821, KCU - Personal - Brand, ENABLED, 1093, 350, 80.75, 23.75
```

This tells you: "The Brand campaign spent $80.75 and got 24 conversions yesterday."

**That's it.** That's all the ROI attribution pipeline sees today. It can't tell you which keywords drove those conversions or where the clicks came from.

### Layer 2: Keyword (10x more data, unused by attribution)

```
date, campaign_id, campaign_name, ad_group_id, ad_group_name, keyword, match_type, impressions, clicks, cost, conversions
2026-02-04, 23228929821, KCU - Personal - Brand, 1557362, Personal Loans, best personal loan rates, BROAD, 89, 12, 4.50, 1.5
```

This tells you: "Within the Brand campaign, the 'Personal Loans' ad group's keyword 'best personal loan rates' got 12 clicks at $4.50."

This data is **already in S3**. We pull it every day. The attribution pipeline just doesn't read it.

### Layer 3: Click / GCLID (30–1,700x more data, also unused)

```
date, gclid, keyword, match_type, campaign_id, campaign_name, ad_group_id, ad_group_name, network, city, region, country
2026-02-04, EAIaIQob..., personal loan rates, BROAD, 23228929821, KCU - Personal - Brand, 1557362, Personal Loans, SEARCH, Portland, Oregon, US
```

This tells you: "This specific click came from Portland, Oregon, searched 'personal loan rates', and we captured the GCLID."

The GCLID is the key. When a click turns into a funded loan, the loan origination system captures the GCLID. By joining this click data against funded loans, you get **true ROI per keyword per region** — not just what Google's conversion pixel reports.

---

## 4. Data Volume: What We Store vs. What We Use

### Per client, per day (real numbers from our data)

| Client | Campaign rows | Keyword rows | Click rows | What attribution reads |
|--------|--------------|-------------|-----------|----------------------|
| California Coast CU | 12 | 145 | 1,192 | 12 |
| CommonWealth One FCU | 2 | 19 | 24 | 2 |
| First Community CU | 1 | 29 | 74 | 1 |
| Kitsap CU | 8 | 76 | 14,631 | 8 |
| Public Service CU | 13 | 23 | 352 | 13 |
| **Total** | **36** | **292** | **16,273** | **36** |

The attribution pipeline reads **36 rows/day**. We store **16,601 rows/day**. That's a **460:1 ratio** of data collected to data used.

### Multipliers by data layer

| Layer | Multiplier vs. campaigns | What it unlocks |
|-------|-------------------------|----------------|
| Keywords | **~10x** (range: 2x–29x) | Which keywords convert, match type efficiency, ad group performance |
| Clicks | **~30–100x** typical, **1,700x** for high-traffic accounts | GCLID → funded loan join, geographic ROI, true per-click attribution |

### Monthly storage per client

| Layer | Rows/month (typical client) | CSV size/month |
|-------|----------------------------|----------------|
| Campaigns | ~300 | ~20 KB |
| Keywords | ~3,000 | ~300 KB |
| Clicks | ~30,000 (up to 450K for Kitsap) | ~3 MB (up to ~50 MB) |
| **Total** | ~33,000 | ~3.3 MB |

For 70 clients over 12 months: roughly **28M rows / 2.8 GB** of click data. Campaign and keyword data is negligible in comparison.

---

## 5. How It Scales

### Client onboarding

Adding a new client to the MCC requires **zero pipeline changes**. The discover step automatically:
1. Detects the new account
2. Generates a client_id slug from the account name
3. Generates a dashboard token
4. Starts pulling data on the next run
5. Sends a Slack notification to #customer-success

### Scaling from 5 → 70 clients

| Metric | 5 clients (today) | 70 clients (target) | Notes |
|--------|-------------------|---------------------|-------|
| Campaign rows/day | 36 | ~500 | Trivial |
| Keyword rows/day | 292 | ~4,000 | Still trivial |
| Click rows/day | 16,273 | ~200K–500K | Depends on traffic mix |
| Daily CSV storage | ~5 MB | ~50–150 MB | S3 cost is negligible |
| Monthly attribution file | 5 files, ~1 KB each | 70 files, ~1 KB each | Negligible |
| Google Ads API calls | 15 (3 per account) | 210 (3 per account) | Well within rate limits |
| DAG runtime | ~5–10 min | ~20–40 min | API calls are the bottleneck, not compute |

### What actually takes time

The bottleneck is **Google Ads API latency**, not data processing. Each API call takes 2–5 seconds. With 70 accounts × 3 data types = 210 calls. Airflow runs accounts in parallel (dynamic task mapping), so with a parallelism of 16 workers the API pull phase takes ~70 seconds. The pandas operations (concat, rename, write CSV) take milliseconds.

### The Kitsap factor

Kitsap CU generates ~14,600 click rows/day — roughly 90% of all click data across 5 clients. If the 70-account roster includes several high-traffic accounts like Kitsap, daily click volume could reach 500K–1M rows. This is still fine for pandas (a 1M-row CSV concat takes <2 seconds), but it's the metric to watch.

---

## 6. The Attribution Bridge

### Before (manual monthly process)

1. Someone manually exports campaign data from one S3 path
2. Reformats columns to match the ROI pipeline's expectations
3. Uploads to a different S3 path
4. Hopes the format is right
5. The ROI pipeline runs hours later and either works or fails silently

### After (automated daily)

The `export_for_attribution` task runs automatically after every daily pull. It:

1. Reads all daily campaign CSVs for the current month from `ad-spend-reports/{client}/campaigns/`
2. Concatenates into one DataFrame
3. Renames columns: `campaign_id` → `Campaign ID`, `campaign_name` → `Ad group`, `date` → `Day`
4. Drops unused columns (`campaign_status`, `impressions`)
5. Writes to `adspend_reports/{client}_{YYYY-MM}_daily.csv` with 2 blank rows prepended
6. The ROI pipeline (`update_google_ads_roi` at 11:30 AM UTC) reads this file with `skiprows=2`

The file is overwritten daily with the full month-to-date, so it always has every day pulled so far. The ROI pipeline's `check_if_file_exists` validates exactly one file exists per client-month, and `get_year_month_values` validates all rows belong to a single month.

### What the ROI pipeline does with it

```
adspend_reports/{client}_{month}_daily.csv
        │
        ▼
  Read CSV (skiprows=2)
        │
        ▼
  Join with prod.application_data (funded loans, app counts, values)
        │
        ▼
  Write to staging.google_ads_campaign_data (Athena)
        │
        ▼
  Powers ROI reporting: "Campaign X spent $5,000 and generated $50,000 in funded loans"
```

---

## 7. EC2 Cost Optimization

The pipeline runs on `auto-attribution-prod` (m5zn.6xlarge — 24 vCPUs, 192 GB RAM). This instance costs **~$1.18/hour**.

### Before: always running

```
24 hours × $1.18/hr × 30 days = ~$850/month
```

### After: start/stop automation

```
~0.7 hours × $1.18/hr × 30 days = ~$25/month
```

That's **$825/month saved** just from the start/stop automation. The Lambda + EventBridge rule that manages this costs fractions of a penny.

---

## 8. Recommendation: Stick with DAGs or Move to PySpark/Glue?

### Stick with Airflow DAGs. Here's why.

**The workload doesn't need distributed compute.** PySpark and Glue solve the problem of processing data that's too large for a single machine. Your pipeline's compute step — concat CSVs and rename columns — processes 16K rows/day in milliseconds. Even at 70 clients with 500K click rows/day, pandas handles this in under 5 seconds on a single core.

**The bottleneck is I/O, not compute.** The pipeline spends 90% of its time waiting on Google Ads API responses. Switching to Spark doesn't make API calls faster. The actual data transformation (rename columns, drop fields, write CSV) is trivially small.

**Glue has overhead that hurts small workloads.**

| | Airflow + pandas | Glue + PySpark |
|--|-----------------|----------------|
| Cold start | 0 (already running) | 1–2 minutes (Spark cluster spin-up) |
| Min cost per run | ~$0.01 (EC2 time) | ~$0.15 (2 DPUs × $0.44/hr, 10-min minimum) |
| Monthly cost (daily runs) | ~$25 (with start/stop) | ~$4.50+ (Glue only, no orchestration) |
| Orchestration | Built-in (Airflow IS the orchestrator) | Need Step Functions or Airflow anyway |
| Debugging | SSH into instance, check logs | CloudWatch logs, less interactive |
| Dependencies | pip install in venv | Glue job parameters, wheel packaging |

**You'd still need an orchestrator.** Glue runs compute jobs, but you still need something to decide: discover accounts → fan out per account → update dashboards → export → stop instance. That's Airflow's job. Moving to Glue doesn't eliminate Airflow — it adds a second system.

### When to revisit this decision

Move compute to Glue/PySpark **if and when**:

1. **Click-level GCLID joins at scale** — If you start joining 1M+ click rows/day against a large funded-loans table with multi-touch attribution windows, and that join takes more than a few minutes in pandas, Spark's distributed join becomes worth the overhead.

2. **Backfills across years of data** — If someone needs to reprocess 12 months × 70 clients × click-level data in one shot (hundreds of millions of rows), a Glue job with 10+ DPUs will finish in minutes vs. hours.

3. **Real-time or near-real-time needs** — If the business wants hourly attribution updates instead of daily, Glue Streaming or Spark Structured Streaming would be the right tool.

**None of these apply today.** The pipeline pulls 16K rows, renames 4 columns, and writes a CSV. Pandas does this before you finish reading this sentence.

### What IS worth investing in

Instead of changing the compute engine, the higher-ROI investments are:

1. **Feed keyword + click data into attribution** — The data is already in S3. Expanding the ROI pipeline to read keyword and click CSVs (not just campaigns) would unlock keyword-level ROI and geographic performance without changing any infrastructure.

2. **GCLID → funded loan matching** — Join the click-level GCLIDs against loan origination data to get true first-party attribution instead of relying on Google's conversion pixel. This is the highest-value unlock and doesn't require Spark.

3. **Monitoring and alerting** — Add checks for: did the export file land? Does it have the expected row count? Did the ROI pipeline succeed? A few Airflow sensors or a lightweight data quality check is more valuable than a compute engine migration.

### Summary

| Question | Answer |
|----------|--------|
| Is the current architecture handling the load? | Yes, comfortably |
| Will it handle 70 clients? | Yes, with room to spare |
| Does PySpark solve a problem we have? | No — our bottleneck is API I/O, not compute |
| Would Glue save money? | No — it would cost more for this workload |
| Would it add complexity? | Yes — second system to manage, package, debug |
| When should we switch? | When we're doing click-level joins across 100M+ rows |
| What should we invest in instead? | Keyword/click attribution, GCLID matching, monitoring |
