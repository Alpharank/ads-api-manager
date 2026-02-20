# Dry-Run & Validation Commands

Use these commands to verify data availability and pipeline health **before** writing any files.

---

## GCLID Attribution (Dry Run)

Checks that click data exists in S3 and application data exists in Athena, prints match statistics without writing output files.

```bash
# Single client
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01 --dry-run

# All clients
python scripts/gclid_attribution.py --all --month 2026-01 --dry-run

# Scheduled (no --month, auto-computes last month via CURRENT_DATE)
python scripts/gclid_attribution.py --all --dry-run
```

**Example output:**

```
============================================================
  Kitsap Credit Union (kitsap_cu)  —  2026-01
============================================================
  Loading click data from S3...
  Loaded 10,862 clicks
  Querying Athena: client_id=kitsap, range=[2026-01-01, 2026-03-01)
  Loaded 234 applications with GCLIDs
  Clicks: 10,862  |  Apps: 234  |  Matched: 189  |  Unmatched apps: 45
  Matched apps: 189 received, 124 funded, $567,890 value
  [dry-run] No files written
```

**What to look for:**
- `Loaded N clicks` — confirms click data exists in S3 for that month
- `Loaded N applications with GCLIDs` — confirms Athena has application data for the client's `prod_id`
- `Matched: N` — how many applications could be traced back to a specific click
- `Unmatched apps: N` — applications with GCLIDs that don't appear in click data (may have clicked before the month window)

---

## List Accessible Accounts

Verifies Google Ads API access and lists all child accounts under the MCC.

```bash
python pipeline/google_ads_to_s3.py --list-accounts
```

---

## Check S3 Data Availability

Verify that raw data exists in S3 for a client.

```bash
# Click data (needed for GCLID attribution)
aws s3 ls s3://ai.alpharank.core/ad-spend-reports/{client_id}/clicks/ --recursive | grep "2026-01"

# Campaign data
aws s3 ls s3://ai.alpharank.core/ad-spend-reports/{client_id}/campaigns/ --recursive | grep "2026-01"

# DPR file (needed for funded data import)
aws s3 ls s3://ai.alpharank.core/customers/{client_slug}/
```

---

## Validate Local Enriched Data

After running an enrichment script, check the output.

```bash
# Check files exist
ls -la data/{client_id}/enriched/

# Inspect campaign-level output
head -5 data/{client_id}/enriched/2026-01.csv

# Inspect daily keyword-level output
head -5 data/{client_id}/enriched/daily/2026-01.csv
```

**Expected campaign-level columns:**
```
campaign_id,campaign_name,apps,approved,funded,production,value,cpf,avg_funded_value,cc_appvd,deposit_appvd,personal_appvd,vehicle_appvd,heloc_appvd
```

---

## List Available Athena Clients

```bash
python scripts/export_athena_data.py --list-clients
```

---

## Sync Check (Preview Only)

Preview what dashboard file changes would be made without committing.

```bash
python scripts/sync_registry.py
```

Add `--commit` to actually commit and push.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `Loaded 0 clicks` | Click data not in S3 for that month | Run pipeline backfill: `python pipeline/google_ads_to_s3.py --client {id} --backfill 30` |
| `Loaded 0 applications` | No application data in Athena for this `prod_id` | Verify `prod_id` in `config/clients.yaml` matches Athena `prod.application_data.client_id` |
| `Matched: 0` | GCLIDs don't overlap | Applications may have clicked in a different month — widen the query window |
| `No DPR file found` | S3 path in `clients.yaml` doesn't point to a valid file | Run `aws s3 ls s3://ai.alpharank.core/{s3_path}/` to check |
| Dashboard shows `--` for funded metrics | No `enriched/` data for that client/month | Run one of the enrichment scripts below |
