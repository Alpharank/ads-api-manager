# Google Ads to S3 Pipeline

Pulls daily campaign metrics, keyword metrics, and GCLID click data from Google Ads accounts under our MCC, uploads to S3, and serves per-client dashboards via GitHub Pages.

## Dashboard

Each client has a token-gated dashboard deployed to GitHub Pages.

### URL Pattern

```
https://alpharank.github.io/Google-Ads-Automation/?client={CLIENT_TOKEN}
```

### Client Tokens

| Client | Token |
|--------|-------|
| California Coast Credit Union | `0f72e02a2ecb57f724288d7dab4fbbdd2d1640b45ad59e34ff260ed591dad92f` |
| CommonWealth One Federal Credit Union | `27579ec0b32f0584b26197dc8e9fb46a190d4ef97a7931a7d71ddd3356b57a5e` |
| First Community Credit Union | `05979da34c7285484c85deed78e6fd75cefef705b3d9ec87f1c9cd9954475530` |
| Kitsap Credit Union | `d49d866d6e58accdfc6b0a57099c04d349ceafedcefa8f249d3cee7bd8ebb472` |
| Public Service Credit Union | `92ec66122ab956def432f3ba0d4f5f142db0f211270256bf12fc27beba80fd9a` |

### Dashboard URLs

```
# California Coast Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=0f72e02a2ecb57f724288d7dab4fbbdd2d1640b45ad59e34ff260ed591dad92f

# CommonWealth One Federal Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=27579ec0b32f0584b26197dc8e9fb46a190d4ef97a7931a7d71ddd3356b57a5e

# First Community Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=05979da34c7285484c85deed78e6fd75cefef705b3d9ec87f1c9cd9954475530

# Kitsap Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=d49d866d6e58accdfc6b0a57099c04d349ceafedcefa8f249d3cee7bd8ebb472

# Public Service Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=92ec66122ab956def432f3ba0d4f5f142db0f211270256bf12fc27beba80fd9a
```

### Assembly Integration

Embed in Assembly iFrame tabs:

```html
<iframe src="https://alpharank.github.io/Google-Ads-Automation/?client={CLIENT_TOKEN}" />
```

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

### 4. Map Our Client IDs

Add accounts to the `client_mapping` section in `config/config.yaml`:

| Customer ID  | Account Name                        | Client ID |
|-------------|--------------------------------------|-----------|
| 9001645164  | California Coast Credit Union        | californiacoast_cu |
| 7306319113  | CommonWealth One Federal Credit Union | commonwealth_one_fcu |
| 4668771744  | FCC_FirstCommunityCreditUnion        | firstcommunity_cu |
| 7543892333  | Kitsap Credit Union                  | kitsap_cu |
| 7636280979  | Public Service Credit Union          | publicservice_cu |

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
├── clients.json              # Client token -> config mapping
├── data-manifest.json        # Available months per client
├── data/                     # Monthly aggregated dashboard data
│   ├── californiacoast_cu/
│   │   ├── campaigns/YYYY-MM.csv
│   │   └── keywords/YYYY-MM.csv
│   └── ...
├── .github/workflows/
│   └── deploy.yml            # GitHub Pages deployment
├── config/
│   ├── config.example.yaml   # Template with placeholder values
│   └── config.yaml           # Real credentials (gitignored)
├── dags/
│   └── google_ads_to_s3_dag.py  # Airflow DAG (daily @ 6 AM UTC)
├── pipeline/
│   ├── __init__.py
│   └── google_ads_to_s3.py   # Main pipeline class + CLI entrypoint
├── scripts/
│   ├── export_account_ids.py  # List MCC child accounts
│   └── generate_refresh_token.py  # OAuth setup helper
├── output/                   # Local daily CSVs (gitignored)
├── requirements.txt
└── .gitignore
```

## Output Structure

Daily data is uploaded to S3:

```
s3://ai.alpharank.core/ad-spend-reports/
└── {client_id}/
    ├── campaigns/YYYY-MM-DD.csv
    ├── keywords/YYYY-MM-DD.csv
    └── clicks/YYYY-MM-DD.csv
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

## Scheduling

### Airflow DAG

The DAG `google_ads_to_s3_daily` runs at 6 AM UTC daily. Client list is driven by the Airflow Variable `google_ads_clients` (JSON list) — add/remove clients from the Airflow UI without code changes.

### Cron
```bash
0 6 * * * cd /path/to/google_ads_to_s3 && python pipeline/google_ads_to_s3.py >> /var/log/google_ads.log 2>&1
```

## GCLID Mapping

Map funded loan GCLIDs back to campaigns and keywords:

```python
import pandas as pd

# Funded loans with GCLIDs
funded_loans = pd.read_csv('funded_loans.csv')

# Load click data from S3
clicks = pd.read_csv('s3://ai.alpharank.core/ad-spend-reports/californiacoast_cu/clicks/2024-01-15.csv')

# Join to get campaign + keyword info
result = funded_loans.merge(clicks, on='gclid', how='left')
print(result[['loan_id', 'gclid', 'campaign_name', 'keyword', 'match_type', 'ad_group_name']])
```

## Adding New Dashboard Data

1. Aggregate daily CSVs into monthly summaries in `data/{client_id}/campaigns/YYYY-MM.csv` and `data/{client_id}/keywords/YYYY-MM.csv`
2. Update `data-manifest.json` to include the new month
3. Commit and push to main branch — GitHub Pages deploys automatically

## Adding New Clients

1. Add client mapping to `config/config.yaml`
2. Generate a token: `python -c "import hashlib; print(hashlib.sha256(b'google-ads-{client_id}').hexdigest())"`
3. Add entry to `clients.json`
4. Add entry to `data-manifest.json`
5. Create data directory: `data/{client_id}/campaigns/` and `data/{client_id}/keywords/`
6. Run backfill and aggregate data

## License

Proprietary - Alpharank
