# Google Ads to S3 Pipeline

Pulls daily campaign metrics, keyword metrics, and GCLID click data from all Google Ads accounts under our MCC and uploads to S3.

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

Add accounts to the `client_mapping` section in `config/config.yaml`. Current accounts under the MCC:

| Customer ID  | Account Name                        |
|-------------|--------------------------------------|
| 9001645164  | California Coast Credit Union        |
| 7306319113  | CommonWealth One Federal Credit Union |
| 4668771744  | FCC_FirstCommunityCreditUnion        |
| 7543892333  | Kitsap Credit Union                  |
| 7636280979  | Public Service Credit Union          |

```yaml
client_mapping:
  "9001645164": "californiacoast_cu"
  "7306319113": "commonwealth_one_fcu"
  "4668771744": "firstcommunity_cu"
  "7543892333": "kitsap_cu"
  "7636280979": "publicservice_cu"
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
├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   ├── config.example.yaml   # Template with placeholder values
│   └── config.yaml           # Real credentials (gitignored)
├── pipeline/
│   ├── __init__.py
│   └── google_ads_to_s3.py   # Main pipeline class + CLI entrypoint
└── scripts/
    ├── export_account_ids.py  # List MCC child accounts
    └── generate_refresh_token.py  # OAuth setup helper
```

## Output Structure

Data is uploaded to S3 with this structure:

```
s3://ai.alpharank.core/ad-spend-reports/
├── campaigns/
│   └── {client_id}/
│       └── YYYY-MM-DD.csv
├── keywords/
│   └── {client_id}/
│       └── YYYY-MM-DD.csv
└── clicks/
    └── {client_id}/
        └── YYYY-MM-DD.csv
```

### Campaign Data Columns
- date, campaign_id, campaign_name, campaign_status
- impressions, clicks, cost, conversions

### Keyword Data Columns
- date, campaign_id, campaign_name, ad_group_id, ad_group_name
- keyword, match_type
- impressions, clicks, cost, conversions

### Click/GCLID Data Columns
- date, gclid, campaign_id, campaign_name
- ad_group_id, ad_group_name, network
- city, region, country

## Scheduling

### AWS Lambda
Deploy as a Lambda function triggered by CloudWatch Events (daily at 6 AM UTC):

```json
{
  "schedule": "cron(0 6 * * ? *)"
}
```

### Cron
```bash
0 6 * * * cd /path/to/google_ads_to_s3 && python pipeline/google_ads_to_s3.py >> /var/log/google_ads.log 2>&1
```

## GCLID Mapping

To map funded loan GCLIDs back to campaigns:

```python
import pandas as pd

# Funded loans with GCLIDs
funded_loans = pd.read_csv('funded_loans.csv')

# Load click data from S3
clicks = pd.read_csv('s3://ai.alpharank.core/ad-spend-reports/clicks/californiacoast_cu/2024-01-15.csv')

# Join to get campaign info
result = funded_loans.merge(clicks, on='gclid', how='left')
print(result[['loan_id', 'gclid', 'campaign_name', 'ad_group_name']])
```
