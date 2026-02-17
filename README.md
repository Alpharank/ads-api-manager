# Google Ads to S3 Pipeline

**Dashboard:** [https://alpharank.github.io/Google-Ads-Automation/](https://alpharank.github.io/Google-Ads-Automation/)

Pulls daily campaign metrics, keyword metrics, and GCLID click data from all Google Ads accounts under our MCC, uploads to S3, and serves per-client dashboards via GitHub Pages.

New accounts added to the MCC are **automatically discovered** — no config changes or code deploys needed.

## How It Works

The Airflow DAG (`google_ads_to_s3_daily`) runs daily at **8:00 AM UTC (3 AM EST)** and does the following:

```
discover_accounts ──> pull_account.expand(N) ──> update_dashboard_files ──> export_for_attribution
                  \──> notify_account_changes (parallel, fires only when accounts change)
```

1. **Discover accounts** — queries the MCC for all enabled child accounts, reconciles against an S3 registry (`_registry/accounts.json`), and auto-generates slugs + dashboard tokens for any new accounts
2. **Pull data** — for each active account (dynamic task mapping), pulls campaign, keyword, and click/GCLID data for yesterday and uploads to S3
3. **Update dashboard files** — rebuilds `clients.json` and `data-manifest.json` so the GitHub Pages dashboard picks up new data and new clients
4. **Export for attribution** — produces a monthly campaign file per client in the format the ROI attribution pipeline (`update_google_ads_roi`) expects, eliminating the previous manual monthly export
5. **Notify account changes** — sends a Slack alert to `#customer-success` when accounts are added to or removed from the MCC

## Auto-Discovery

When a new account appears under the MCC:

- A `client_id` slug is generated from the account name (e.g. "Altura Credit Union" → `altura_credit_union`)
- A SHA-256 dashboard token is generated
- The account is added to the S3 registry and included in all subsequent runs
- A Slack notification is sent to `#customer-success`
- Dashboard files are updated so the new client's dashboard is immediately available

When an account is removed from the MCC, the registry entry gets a `removed_at` timestamp and data pulls stop. If the account reappears later, it's automatically reactivated.

## Attribution Export

After each daily pull, the pipeline writes a monthly attribution file for each client:

```
s3://ai.alpharank.core/adspend_reports/{client_id}_{YYYY-MM}_daily.csv
```

This file has 2 blank rows followed by a header row with columns: `Campaign ID`, `Ad group`, `Day`, `Clicks`, `Cost`, `Conversions`. The ROI attribution pipeline reads this file with `skiprows=2`.

The file is overwritten daily with the full month-to-date data, so it always contains every day pulled so far for that month.

### Timing

| Pipeline | Schedule | What it does |
|----------|----------|-------------|
| `google_ads_to_s3_daily` | 8:00 AM UTC (3 AM EST) | Pull daily data, rebuild dashboards, export attribution file |
| `update_google_ads_roi` | 11:30 AM UTC (6:30 AM EST) | Read attribution file, join with app data, write to staging |

The export finishes hours before the ROI pipeline reads it.

## Dashboard

Each client has a token-gated dashboard deployed to GitHub Pages. See [`clients/`](clients/) for tokens and URLs.

### Dashboard Features

- **Auto-Month Selection**: Loads the most recent available month
- **Month Dropdown**: Switch between available months
- **KPI Cards**: Impressions, Clicks, Spend, Conversions (with CPA)
- **Campaign Performance Chart**: Horizontal bar chart, click to filter
- **Keyword Table**: Sortable columns, match type badges (Broad/Phrase/Exact)
- **Text Filter**: Search by campaign or keyword name
- **Campaign Filter**: Click any campaign bar to drill down

## Setup

### 1. Install Dependencies

```bash
cd google_ads_to_s3
pip install -r requirements.txt
```

### 2. Configure Credentials

Copy the example config and fill in credentials:

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml` with our credentials:

```yaml
google_ads:
  developer_token: "OUR_DEVELOPER_TOKEN"
  client_id: "OUR_OAUTH_CLIENT_ID"
  client_secret: "OUR_OAUTH_CLIENT_SECRET"
  refresh_token: "OUR_REFRESH_TOKEN"
  login_customer_id: "OUR_MCC_ID"  # No dashes, e.g., "1234567890"
```

### 3. Generate OAuth Refresh Token (if needed)

If we don't have a refresh token yet:

```bash
pip install google-auth-oauthlib
python scripts/generate_refresh_token.py
```

## Usage

### List all accessible accounts
```bash
python pipeline/google_ads_to_s3.py --list-accounts
```

### Pull yesterday's data (default)
```bash
python pipeline/google_ads_to_s3.py
```

### Backfill last 90 days
```bash
python pipeline/google_ads_to_s3.py --backfill 90
```

### Pull specific date
```bash
python pipeline/google_ads_to_s3.py --date 2024-01-15
```

### Pull for a single client
```bash
python pipeline/google_ads_to_s3.py --backfill 90 --client californiacoast_cu
```

## Project Structure

```
google_ads_to_s3/
├── index.html               # Dashboard application (GitHub Pages)
├── clients/
│   └── README.md            # Dashboard tokens and URLs per client
├── .github/workflows/
│   └── deploy.yml           # GitHub Pages deployment
├── config/
│   ├── config.example.yaml  # Template with placeholder values
│   └── config.yaml          # Real credentials (gitignored)
├── dags/
│   └── google_ads_to_s3_dag.py  # Airflow DAG (daily @ 8 AM UTC)
├── pipeline/
│   ├── __init__.py
│   ├── google_ads_to_s3.py  # Main pipeline class + CLI entrypoint
│   └── slack.py             # Slack notification helper
├── scripts/
│   ├── gclid_attribution.py       # GCLID-based keyword-level attribution
│   ├── generate_daily_attribution.py  # Synthetic proportional attribution (fallback)
│   ├── export_athena_data.py      # Export enriched data from Athena
│   ├── import_s3_funded_data.py   # Import funded data from S3
│   ├── export_account_ids.py      # List MCC child accounts
│   └── generate_refresh_token.py  # OAuth setup helper
├── output/                  # Local daily CSVs (gitignored)
├── requirements.txt
└── .gitignore
```

## S3 Output Structure

### Daily data (per account, per date)

```
s3://ai.alpharank.core/ad-spend-reports/
├── _registry/
│   └── accounts.json              # Auto-discovery account registry
├── _dashboard/
│   ├── clients.json               # Token -> client config mapping
│   └── data-manifest.json         # Available months per client
└── {client_id}/
    ├── campaigns/YYYY-MM-DD.csv
    ├── keywords/YYYY-MM-DD.csv
    └── clicks/YYYY-MM-DD.csv
```

### Attribution export (per client, per month)

```
s3://ai.alpharank.core/adspend_reports/
└── {client_id}_{YYYY-MM}_daily.csv
```

### Campaign Data Columns
- date, campaign_id, campaign_name, campaign_status
- impressions, clicks, cost, conversions

### Keyword Data Columns
- date, campaign_id, campaign_name, ad_group_id, ad_group_name
- keyword, match_type
- impressions, clicks, cost, conversions

### Click/GCLID Data Columns
- date, gclid, keyword, match_type
- campaign_id, campaign_name, ad_group_id, ad_group_name
- network, city, region, country

## GCLID Attribution

`scripts/gclid_attribution.py` produces real keyword-level attribution by joining GCLID click data (from S3) with application data (from `prod.application_data` in Athena). Each application is mapped to the exact click (date + campaign + keyword) that generated it.

```bash
# Dry run — print match stats without writing files
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01 --dry-run

# Run for one client
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01

# Run for all clients
python scripts/gclid_attribution.py --all --month 2026-01
```

**How it works:**

1. Loads click CSVs from S3 (`ad-spend-reports/{client_id}/clicks/{month}-*.csv`)
2. Queries `prod.application_data` in Athena, extracting GCLIDs from `attributed_tag_event_url`
3. Inner joins on GCLID — each application gets the exact keyword that drove it
4. Writes two outputs that the dashboard already consumes:
   - `data/{client_id}/enriched/{month}.csv` — campaign-level totals
   - `data/{client_id}/enriched/daily/{month}.csv` — daily keyword-level attribution

This replaces the synthetic proportional distribution in `generate_daily_attribution.py` with real per-click attribution.

## Adding New Clients

New clients are **automatically onboarded** when they appear under the MCC. No manual steps needed.

The `client_mapping` in `config/config.yaml` seeds the initial set of accounts on first run. After that, the S3 registry is the source of truth and new accounts are discovered via the MCC query.

## License

Proprietary - Alpharank
