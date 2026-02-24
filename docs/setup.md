# Setup

## Prerequisites

- Python 3.10+
- AWS CLI configured with access to the `ai.alpharank.core` S3 bucket and Athena (`us-east-1`)
- Google Ads API developer token with access to the Alpharank MCC (ID `9738614464`)
- Slack webhook URL (optional, for `#customer-success` notifications)

## 1. Clone and Install

```bash
git clone git@github.com:Alpharank/Google-Ads-Automation.git
cd Google-Ads-Automation

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

**Dependencies:** `google-ads`, `boto3`, `pandas`, `pyyaml`, `google-auth-oauthlib`, `slack-sdk`. See [`requirements.txt`](../requirements.txt).

## 2. Configure Credentials

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml` with:

| Field | Where to Get It |
|-------|----------------|
| `google_ads.developer_token` | Google Ads API Center |
| `google_ads.client_id` | Google Cloud Console → OAuth 2.0 Client ID |
| `google_ads.client_secret` | Google Cloud Console → OAuth 2.0 Client Secret |
| `google_ads.refresh_token` | Generated in step 3 below |
| `google_ads.login_customer_id` | MCC account ID (no dashes): `9738614464` |
| `aws.bucket` | S3 bucket: `ai.alpharank.core` |
| `aws.region` | `us-east-1` |

The `client_mapping` section maps Google Ads customer IDs to internal `client_id` slugs. New accounts under the MCC are auto-discovered, but pre-mapping ensures consistent naming.

## 3. Generate OAuth Refresh Token

```bash
python scripts/generate_refresh_token.py
```

This opens a browser for Google OAuth consent. After authorizing, the script prints a refresh token. Paste it into `config/config.yaml` under `google_ads.refresh_token`.

## 4. Client Configuration (for Enrichment)

Enrichment scripts (GCLID attribution, Athena export, S3 funded import) use [`config/clients.yaml`](../config/clients.yaml) to map client IDs to Athena identifiers and S3 paths. Each client entry includes:

| Field | Purpose |
|-------|---------|
| `name` | Human-readable client name |
| `google_ads_id` | Google Ads customer ID (no dashes) |
| `s3_path` | S3 folder for first-party DPR data |
| `athena_id` | `client_id` in `staging.google_ads_campaign_data` |
| `prod_id` | `client_id` in `prod.application_data` |
| `dashboard_token` | SHA-256 token for the GitHub Pages dashboard |

## 5. Verify Setup

```bash
# List all MCC child accounts (verifies Google Ads API access)
python pipeline/google_ads_to_s3.py --list-accounts

# Dry run enrichment (verifies S3 + Athena access)
python scripts/gclid_attribution.py --all --dry-run
```

See [Dry Runs & Validation](dry-runs/) for more verification commands.

## AWS Credentials

The pipeline and enrichment scripts use `boto3`, which reads credentials from the standard AWS credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. `~/.aws/credentials` profile
3. IAM instance role (on EC2)

Required permissions:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `ai.alpharank.core`
- `athena:StartQueryExecution`, `athena:GetQueryResults` in `us-east-1`
- `s3:GetObject`, `s3:PutObject` on the Athena results bucket
