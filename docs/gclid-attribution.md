# ADR: GCLID-Based Keyword-Level Attribution

**Script:** [`scripts/gclid_attribution.py`](../scripts/gclid_attribution.py)

---

## Table of Contents

- [1. Context & Problem](#1-context--problem)
- [2. Decision](#2-decision)
- [3. How the GCLID Link Actually Works](#3-how-the-gclid-link-actually-works)
- [4. Data Flow in Detail](#4-data-flow-in-detail)
- [5. Input Schemas](#5-input-schemas)
- [6. Output Schemas](#6-output-schemas)
- [7. Key Design Decisions](#7-key-design-decisions)
- [8. Client ID Mapping](#8-client-id-mapping)
- [9. Running the Script](#9-running-the-script)
- [10. Consequences & Tradeoffs](#10-consequences--tradeoffs)

---

## 1. Context & Problem

The AlphaRank ROI pipeline has a table called `staging.google_ads_campaign_data` that
contains funded-loan metrics aggregated at the campaign level. The Athena Export script
([`scripts/export_athena_data.py`](../scripts/export_athena_data.py)) queries this
table and produces campaign-level enriched CSVs.

**The problem:** that table has no GCLID column. Everything is pre-aggregated to
`campaign_id` + `campaign`. You can say "Campaign X drove 5 funded loans," but you
**cannot** say which keyword, ad group, or match type drove those loans.

For Google Ads optimization — pausing underperforming keywords, increasing bids on
high-ROAS terms, comparing match types — you need keyword-level attribution. That
requires tracing individual clicks all the way to individual funded applications.

## 2. Decision

Build a separate attribution path that bypasses `staging.google_ads_campaign_data`
entirely. Instead, join two independent data sources on the GCLID (Google Click
Identifier):

1. **Click data** — pulled daily from the Google Ads API `click_view` resource and
   stored as CSVs in S3. Each row is one click with its GCLID, keyword, ad group,
   campaign, and geography.

2. **Application data** — queried on-demand from `prod.application_data` in Athena.
   Each row is one loan application. The GCLID is not a dedicated column — it is
   embedded inside the `attributed_tag_event_url` field and must be extracted via
   regex.

Inner-joining these two DataFrames on `gclid` gives us exact click-to-application
mapping at the keyword level.

## 3. How the GCLID Link Actually Works

This is the core mechanism and the part most likely to be misunderstood.

### Where the GCLID comes from

When a user clicks a Google Ad, Google appends a `gclid` parameter to the landing
page URL:

```
https://www.examplecu.com/apply?gclid=EAIaIQobChMI8f...&utm_source=google
```

This GCLID is unique to that specific click. Google's `click_view` resource records
it alongside the keyword, ad group, campaign, and geographic data for that click.

### Where the GCLID ends up in application data

If that user eventually submits a loan application, the AlphaRank tracking tag
captures the page URL (including the `gclid` query parameter) and stores it in the
`attributed_tag_event_url` column of `prod.application_data`. The raw value looks
like:

```
https://www.examplecu.com/apply?gclid=EAIaIQobChMI8f...&utm_source=google&utm_medium=cpc
```

The GCLID is **not** stored as its own column. It is buried inside this URL string.

### How the script extracts it

The Athena query uses `REGEXP_EXTRACT` to parse the GCLID out of the URL:

```sql
-- scripts/gclid_attribution.py lines 43-57
SELECT
    REGEXP_EXTRACT(attributed_tag_event_url, '(gclid)=([^&#\?]+)', 2) AS gclid,
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
    AND attributed_tag_event_url LIKE '%gclid=%'
```

The regex `'(gclid)=([^&#\?]+)'` captures group 2 — everything after `gclid=` up to
the next `&`, `#`, `?`, or end of string. The `LIKE '%gclid=%'` pre-filter ensures
only rows with a GCLID are scanned.

### The join

Now both sides have a `gclid` column:

```
clicks_df                              apps_df
┌──────────────────────────┐           ┌──────────────────────────┐
│ gclid   ◄── from API    │           │ gclid   ◄── parsed from │
│ keyword                  │           │              URL above  │
│ match_type               │           │ funded                  │
│ campaign_id              │           │ approved                │
│ campaign_name            │           │ production_value        │
│ ad_group_id              │           │ lifetime_value          │
│ ad_group_name            │           │ product_family          │
│ city, region, country    │           │                         │
└────────────┬─────────────┘           └────────────┬────────────┘
             │                                      │
             └──────────────┬───────────────────────┘
                            │
                            ▼
             clicks_df.merge(apps_df, on="gclid", how="inner")
                            │
                            ▼
             ┌──────────────────────────────────────┐
             │ Each matched application inherits    │
             │ the click's keyword, ad group,       │
             │ campaign, match type, and geography  │
             └──────────────────────────────────────┘
```

This is an **inner join** — only clicks that have a matching application (and vice
versa) appear in the output. Clicks without applications and applications without
matching clicks are dropped.

## 4. Data Flow in Detail

```
   Google Ads API                                   AlphaRank Tracking Tag
   (click_view resource)                            (on client's website)
        │                                                │
        │  GAQL query pulls every click                  │  Captures landing page URL
        │  with gclid, keyword, ad_group                 │  (which contains ?gclid=...)
        │                                                │
        ▼                                                ▼
   pipeline/google_ads_to_s3.py                     prod.application_data
   pull_click_data()                                (Athena table)
        │                                                │
        │  Stores one CSV per day                        │
        ▼                                                │
   S3: {bucket}/{client}/clicks/{date}.csv               │
        │                                                │
        │                                                │
        ├────────────────────────────────────────────────┤
        │                                                │
        │  scripts/gclid_attribution.py                  │
        │                                                │
        │  Step 1: load_click_data()                     │
        │    Reads all clicks/{month}-*.csv              │
        │    from S3 into clicks_df                      │
        │                                                │
        │                               Step 2: query_applications()
        │                                 REGEXP_EXTRACT(url) → gclid
        │                                 Returns apps_df
        │                                                │
        │               Step 3: build_attribution()      │
        │                 clicks_df.merge(apps_df,       │
        │                   on="gclid", how="inner")     │
        │                                                │
        ▼                                                ▼
   ┌─────────────────────────────────────────────────────────┐
   │                  joined DataFrame                       │
   │  gclid | keyword | match_type | campaign_id | funded | │
   │  ad_group_id | ad_group_name | production_value | ...  │
   └──────────────────────┬──────────────────────────────────┘
                          │
              ┌───────────┴────────────┐
              ▼                        ▼
   Campaign-level agg           Daily keyword-level agg
   groupby:                     groupby:
     campaign_id,                 date, campaign_id,
     campaign_name                campaign_name, ad_group_id,
                                  ad_group_name, keyword,
              │                   match_type
              ▼                        │
   enriched/{month}.csv                ▼
   (campaign totals)            enriched/daily/{month}.csv
                                (daily keyword breakdown)
```

### Step 1 — Load click data from S3

[`load_click_data()`](../scripts/gclid_attribution.py) (line 131) reads all CSV files
matching `s3://{bucket}/{prefix}/{client_id}/clicks/{month}-*.csv` and concatenates
them into a single DataFrame.

These CSVs were written by the daily pipeline
([`pipeline/google_ads_to_s3.py`](../pipeline/google_ads_to_s3.py) `pull_click_data()`
lines 375–412), which runs this GAQL query:

```sql
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
WHERE segments.date = '{date}'
```

Each row is mapped to:

| Column | Source |
|--------|--------|
| `gclid` | `click_view.gclid` |
| `keyword` | `click_view.keyword_info.text` |
| `match_type` | `click_view.keyword_info.match_type.name` |
| `campaign_id` | `campaign.id` |
| `campaign_name` | `campaign.name` |
| `ad_group_id` | `ad_group.id` |
| `ad_group_name` | `ad_group.name` |
| `date` | `segments.date` |
| `network` | `segments.ad_network_type.name` |
| `city` | `click_view.area_of_interest.city` |
| `region` | `click_view.area_of_interest.region` |
| `country` | `click_view.area_of_interest.country` |

### Step 2 — Query applications from Athena

[`query_applications()`](../scripts/gclid_attribution.py) (line 160) runs the
`APPLICATION_QUERY` against `prod.application_data` with parameters:

- `client_id` = the client's `prod_id` from `config/clients.yaml`
- Date range = first of target month through **+2 months** (see [decision #3](#3-two-month-lookahead-window))

Returns a DataFrame with columns: `gclid`, `received`, `approved`, `funded`,
`production_value`, `lifetime_value`, `product_family`.

### Step 3 — Join and aggregate

[`build_attribution()`](../scripts/gclid_attribution.py) (line 199):

1. Inner join on `gclid` (line 209)
2. Derive product-family breakdown flags (lines 216–220)
3. Aggregate to campaign-level (lines 236–254)
4. Aggregate to daily keyword-level (lines 257–275)

## 5. Input Schemas

### S3 click CSV columns

| Column | Type | Example |
|--------|------|---------|
| `date` | string | `2026-01-15` |
| `gclid` | string | `EAIaIQobChMI8f...` |
| `keyword` | string | `credit union near me` |
| `match_type` | string | `BROAD`, `PHRASE`, `EXACT` |
| `campaign_id` | string | `20145678901` |
| `campaign_name` | string | `Brand - Checking` |
| `ad_group_id` | string | `154321098765` |
| `ad_group_name` | string | `Checking Accounts` |
| `network` | string | `SEARCH`, `SEARCH_PARTNERS` |
| `city` | string | `Bremerton` (nullable) |
| `region` | string | `Washington` (nullable) |
| `country` | string | `US` (nullable) |

### Athena application query result columns

| Column | Type | Derivation |
|--------|------|------------|
| `gclid` | string | `REGEXP_EXTRACT(attributed_tag_event_url, ...)` |
| `received` | int | Always `1` (one row = one application) |
| `approved` | int | `1` if `approved = true`, else `0` |
| `funded` | int | `1` if `funded = true`, else `0` |
| `production_value` | float | `COALESCE(production_value, 0)` |
| `lifetime_value` | float | `COALESCE(lifetime_value, 0)` |
| `product_family` | string | `Credit Card`, `Vehicle Loan`, `Personal Loan`, `Home Equity Loan`, `Xpress App`, or empty |

## 6. Output Schemas

### Campaign-level — `data/{client}/enriched/{month}.csv`

| Column | Description |
|--------|-------------|
| `campaign_id` | Google Ads campaign ID |
| `campaign_name` | Campaign name |
| `apps` | Total applications received (sum of `received`) |
| `approved` | Approved applications |
| `funded` | Funded applications |
| `production` | Total production value |
| `value` | Total lifetime value |
| `cpf` | Set to `0` — the dashboard computes this from cost data |
| `avg_funded_value` | `value / funded` (or `0` if no funded) |
| `cc_appvd` | Credit card approvals |
| `deposit_appvd` | Deposit/checking approvals (product_family = `Xpress App`) |
| `personal_appvd` | Personal loan approvals |
| `vehicle_appvd` | Vehicle loan approvals |
| `heloc_appvd` | Home equity loan approvals |

### Daily keyword-level — `data/{client}/enriched/daily/{month}.csv`

Same columns as campaign-level, plus:

| Column | Description |
|--------|-------------|
| `date` | Click date (`YYYY-MM-DD`) |
| `ad_group_id` | Google Ads ad group ID |
| `ad_group_name` | Ad group name |
| `keyword` | Keyword text |
| `match_type` | `BROAD`, `PHRASE`, or `EXACT` |

Rows are sorted by `date`, `campaign_id`, `keyword`.

## 7. Key Design Decisions

### 1. Inner join (not left/outer)

```python
joined = clicks_df.merge(apps_df, on="gclid", how="inner")
```

Only clicks with a matching application appear in the output. This means:

- Clicks that did not result in an application are dropped (most of them)
- Applications whose GCLID does not appear in the click data are dropped (e.g., the
  click happened outside the target month, or the click CSV was missing)

**Why inner:** We only want rows where we can attribute a specific application to a
specific keyword. Left-joining would include millions of unmatched clicks and inflate
the output.

### 2. Two-month lookahead window

```python
# lines 168-174
end_month = int(mo) + 2
```

The application query scans from the first of the target month through +2 months.
A click on January 15 might not result in a funded loan until March. The lookahead
window catches these delayed conversions.

**Tradeoff:** Running attribution for a recent month may undercount funded loans if
applications are still in progress. Re-running after 60+ days captures late arrivals.

### 3. `prod_id` vs `athena_id`

The `prod.application_data` table uses a different client identifier than
`staging.google_ads_campaign_data`. In `config/clients.yaml`, each client has:

- `athena_id` — used by `export_athena_data.py` to query `staging.google_ads_campaign_data`
- `prod_id` — used by `gclid_attribution.py` to query `prod.application_data`

These often differ:

| client_id | athena_id | prod_id |
|-----------|-----------|---------|
| `kitsap_cu` | `kitsap_cu` | `kitsap` |
| `firstcommunity_cu` | `first_community_cu` | `first_ccu` |
| `commonwealth_one_fcu` | `commonwealth_one_fcu` | `commonwealthone` |

Using the wrong ID returns zero applications. The script reads `prod_id` at line 320:

```python
prod_id = client_cfg["prod_id"]
```

### 4. Product-family breakdown

The join output is enriched with per-product approval flags (lines 216–220):

| Flag column | product_family value |
|-------------|---------------------|
| `cc_appvd` | `Credit Card` |
| `deposit_appvd` | `Xpress App` |
| `personal_appvd` | `Personal Loan` |
| `vehicle_appvd` | `Vehicle Loan` |
| `heloc_appvd` | `Home Equity Loan` |

These are `1` only if the application is **both** the matching product family **and**
`approved = 1`. This lets the dashboard break down approved loans by product type per
keyword.

### 5. CPF is set to zero

```python
campaign_agg["cpf"] = 0  # dashboard computes from cost data
```

Cost data lives in the daily pipeline CSVs, not in the enrichment output. The
dashboard merges enriched data with cost data and computes `cpf = cost / funded`
at render time.

### 6. GCLID regex tolerance

The regex `'(gclid)=([^&#\?]+)'` handles:

- `?gclid=abc123` — GCLID as first parameter
- `&gclid=abc123` — GCLID after other parameters
- `gclid=abc123#section` — GCLID before a fragment
- URLs with or without trailing parameters

The `LIKE '%gclid=%'` pre-filter is a performance optimization — Athena can skip
rows without a GCLID before applying the more expensive regex.

## 8. Client ID Mapping

The script resolves client identifiers through `config/clients.yaml`:

```
CLI argument        clients.yaml key        clients.yaml field
──────────          ────────────────        ──────────────────
--client kitsap_cu  kitsap_cu:
                      google_ads_id ──────► Google Ads API (daily pipeline)
                      prod_id: "kitsap" ──► WHERE client_id = ? (Athena query)
                      s3_path ────────────► S3 bucket prefix for DPR files
                      athena_id ──────────► (NOT used by this script)
```

## 9. Running the Script

```bash
# Single client
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01

# All configured clients
python scripts/gclid_attribution.py --all --month 2026-01

# Dry run — prints match statistics, writes nothing
python scripts/gclid_attribution.py --client kitsap_cu --month 2026-01 --dry-run
```

**Dry-run output example:**

```
============================================================
  Kitsap Credit Union (kitsap_cu)  —  2026-01
============================================================
  Loading click data from S3...
  Loaded 12,483 clicks
  Querying Athena: client_id=kitsap, range=[2026-01-01 00:00:00, 2026-03-01 00:00:00)
  Loaded 847 applications with GCLIDs
  Clicks: 12,483  |  Apps: 847  |  Matched: 312  |  Unmatched apps: 535
  Matched apps: 312 received, 41 funded, $1,245,000 value
  [dry-run] No files written
```

**Prerequisites:**
- `boto3` and `pandas` installed
- AWS credentials configured with access to S3 bucket and Athena
- Click data already collected (daily pipeline must have run for the target month)

## 10. Consequences & Tradeoffs

### Benefits

- **Keyword-level attribution** — the only path that can answer "which keyword drove
  funded loans"
- **Ad-group-level attribution** — enables pausing underperforming ad groups
- **Daily granularity** — funded trends by keyword over time
- **Geographic attribution** — click data includes city/region/country
- **Product-family breakdown** — which loan types each keyword drives
- **Deterministic** — exact click-to-application mapping, not statistical estimation

### Limitations

- **Requires click data in S3** — if the daily pipeline didn't run for a date range,
  those clicks are missing and applications from those clicks will be unmatched
- **GCLID coverage** — only applications with a GCLID in the URL are matchable.
  Applications from direct visits, organic search, or non-Google channels are excluded
- **Delayed attribution** — a January click may not fund until March. The +2 month
  window helps but running too early undercounts. Re-run after 60 days for accuracy
- **Inner join drops unmatched** — applications whose click happened outside the target
  month, or clicks without an application, are silently dropped. The dry-run output
  reports unmatched counts for monitoring
- **No cost data in output** — the dashboard must merge enriched CSVs with cost data
  separately to compute CPF and ROAS
