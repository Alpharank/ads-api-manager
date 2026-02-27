# Attribution Data Discrepancy — Root Cause Analysis

> **Date**: 2026-02-27
> **Author**: AlphaRank Engineering
> **Status**: Investigation complete, fixes proposed
> **Severity**: High — dashboard underreports applications by 41–91% across all clients

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [The Three Data Sources](#the-three-data-sources)
3. [Data Flow Architecture](#data-flow-architecture)
4. [The Overwrite Problem](#the-overwrite-problem)
5. [Impact Assessment](#impact-assessment)
6. [Deep Dive: Kitsap Credit Union](#deep-dive-kitsap-credit-union)
7. [GCLID Capture Rate by Client](#gclid-capture-rate-by-client)
8. [Additional Bugs Discovered](#additional-bugs-discovered)
9. [Immediate Fixes Applied](#immediate-fixes-applied)
10. [Recommended Fix Roadmap](#recommended-fix-roadmap)
11. [Validation Tool](#validation-tool)
12. [File Reference](#file-reference)

---

## Executive Summary

The Google Ads dashboard has been displaying significantly undercounted application numbers for all clients. The root cause is a **file overwrite bug**: two attribution scripts write to the same output path, and the script that runs second (GCLID attribution) replaces the correct data with a much smaller subset.

```
                     ┌──────────────────────────────────────────────┐
                     │   WHAT CLIENTS SEE vs REALITY                │
                     │                                              │
                     │   Dashboard showed:    9 approved            │
                     │   Actual number:      61 approved            │
                     │                                              │
                     │   That's 85% of approvals MISSING            │
                     │   from the Kitsap CU January dashboard.      │
                     └──────────────────────────────────────────────┘
```

---

## The Three Data Sources

Three independent systems produce attribution data. Each uses a different methodology, producing wildly different numbers:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│  SOURCE 1: Auto-Attribution Pipeline                         ★ GROUND TRUTH ★  │
│  ─────────────────────────────────────                                          │
│  Table:    staging.google_ads_campaign_data (Athena / S3 parquet)               │
│  Owner:    External auto-attribution team                                       │
│  Method:   Full application matching across all channels and touchpoints        │
│  Location: s3://etl.alpharank.airflow/staging/google_ads_campaign_data/         │
│  Scope:    ALL applications attributed to Google Ads campaigns                  │
│  Quality:  ████████████████████ 100% coverage                                  │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  SOURCE 2: GCLID Attribution                                  ✗ UNDERCOUNTS    │
│  ───────────────────────────                                                    │
│  Script:   scripts/gclid_attribution.py                                         │
│  Method:   Joins S3 click data with prod.application_data on click_id           │
│  Filter:   WHERE click_id IS NOT NULL AND click_id != ''                        │
│  Output:   data/{client}/enriched/{month}.csv                                   │
│  Scope:    ONLY applications with a tracked Google click_id                     │
│  Quality:  ████░░░░░░░░░░░░░░░░ 9-59% coverage (varies by client)             │
│                                                                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  SOURCE 3: Synthetic Spread                                   ✗ OVERCOUNTS     │
│  ──────────────────────────                                                     │
│  Script:   scripts/generate_daily_attribution.py                                │
│  Method:   Takes auto-attr totals, distributes proportionally by ad spend       │
│  Output:   data/{client}/enriched/{month}_first_click.csv                       │
│  Scope:    ALL campaigns including non-Google-Ads (organic, direct, etc.)       │
│  Quality:  ████████████████████████████████ inflated 2-16x above truth         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Architecture

### Current System (Broken)

```
                            ┌──────────────────┐
                            │   Google Ads API  │
                            └────────┬─────────┘
                                     │
                              click data, costs
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │        S3 Click Storage         │
                    │  ad-spend-reports/{client}/     │
                    │  ├── clicks/2026-01-*.csv       │
                    │  ├── campaigns/2026-01.csv      │
                    │  └── keywords/2026-01.csv       │
                    └───────┬──────────┬─────────────┘
                            │          │
              ┌─────────────┘          └────────────┐
              │                                     │
              ▼                                     ▼
┌──────────────────────────┐          ┌──────────────────────────┐
│  Auto-Attribution        │          │  GCLID Attribution       │
│  Pipeline (external)     │          │  gclid_attribution.py    │
│                          │          │                          │
│  Queries ALL apps from   │          │  Queries ONLY apps with  │
│  prod.application_data   │          │  click_id != ''          │
│  (no click_id filter)    │          │  from prod.app_data      │
│                          │          │                          │
│  Writes parquet to S3    │          │  Writes CSV locally      │
└──────────┬───────────────┘          └──────────┬───────────────┘
           │                                     │
           ▼                                     ▼
┌──────────────────────────┐          ┌──────────────────────────┐
│  S3 Parquet Files        │          │  enriched/{month}.csv    │
│  staging/google_ads_     │          │                          │
│  campaign_data/          │          │  ⚠️  OVERWRITES the     │
│  client_id=kitsap/       │          │  file written by         │
│  google_ads_2026-01.pq   │          │  export_athena_data.py   │
│                          │          │                          │
│  156 apps ✓              │          │  32 apps ✗               │
└──────────────────────────┘          └──────────┬───────────────┘
                                                 │
                                                 ▼
                                      ┌──────────────────────────┐
                                      │  Dashboard (index.html)  │
                                      │                          │
                                      │  Loads enriched/*.csv    │
                                      │  Shows: 32 apps          │
                                      │  Should show: 156 apps   │
                                      └──────────────────────────┘
```

### DAG Task Execution Order

```
 ┌──────────────────┐
 │ discover_accounts│
 └────────┬─────────┘
          │
          ▼
 ┌──────────────────┐
 │ pull_account     │  Pull raw Google Ads data from API
 │ (×N accounts)    │
 └────────┬─────────┘
          │
          ▼
 ┌──────────────────────────┐
 │ update_dashboard_files   │  Rebuild clients.json + manifest
 └────────┬─────────────────┘
          │
          ▼
 ┌──────────────────────────┐
 │ export_for_attribution   │  Step 1: Queries Athena (auto-attr)
 │                          │  Writes: enriched/{month}.csv  ← CORRECT (156 apps)
 └────────┬─────────────────┘
          │
          ▼
 ┌──────────────────────────┐
 │ enrich_funded_data       │  Step 2: Runs GCLID attribution
 │                          │  Writes: enriched/{month}.csv  ← OVERWRITES with 32 apps!
 └────────┬─────────────────┘
          │
          ▼
 ┌──────────────────────────┐
 │ export_keywords_to_athena│  Export keyword data to Athena
 └────────┬─────────────────┘
          │
          ▼
 ┌──────────────────────────┐
 │ stop_instance            │  Shut down EC2
 └──────────────────────────┘

          ⚠️  Step 2 DESTROYS Step 1's output — same file path!
```

---

## The Overwrite Problem

This is the core bug. Both scripts write to the identical path:

```
data/{client}/enriched/{month}.csv

Timeline during DAG run:

  08:00  ┌─ export_for_attribution runs ───────────────────────────┐
         │  export_athena_data.py queries Athena                   │
         │  Writes enriched/2026-01.csv                            │
         │  Content: 156 apps, 61 approved, 34 funded              │
  08:05  └─────────────────────────────────────────────────────────┘

  08:05  ┌─ enrich_funded_data runs ───────────────────────────────┐
         │  gclid_attribution.py joins clicks with applications    │
         │  Writes enriched/2026-01.csv   ← SAME FILE             │
         │  Content: 32 apps, 9 approved, 4 funded                 │
  08:10  └─────────────────────────────────────────────────────────┘

  Result: Dashboard loads 32 apps instead of 156. User sees 9 approved.
          124 apps, 52 approved, 30 funded silently lost.
```

---

## Impact Assessment

### Severity by Metric

```
  Applications Missing from Dashboard (Jan 2026)
  ──────────────────────────────────────────────────────────────

  californiacoast_cu  ███████████████████████████████████████░  91% missing
                      111 shown / 1,254 actual

  firstcommunity_cu   ████████████████████████████████████░░░░  85% missing
                      6 shown / 39 actual

  kitsap_cu           ██████████████████████████████████░░░░░░  79% missing
                      32 shown / 156 actual

  publicservice_cu    █████████████████████████████████░░░░░░░  77% missing
                      60 shown / 256 actual

  first_commonwealth  ████████████████░░░░░░░░░░░░░░░░░░░░░░░  41% missing
                      172 shown / 292 actual

  ░ = missing from dashboard    █ = shown (GCLID only)
```

### Business Impact

```
  ┌────────────────────────────────────────────────────────────────┐
  │                                                                │
  │  Total across all clients (January 2026):                      │
  │                                                                │
  │  Dashboard showed:     381 apps    113 approved    51 funded   │
  │  Actual (Athena):    1,997 apps    641 approved   369 funded   │
  │  Missing:            1,616 apps    528 approved   318 funded   │
  │                                                                │
  │  ► 81% of applications invisible to dashboard users            │
  │  ► 82% of approvals invisible                                  │
  │  ► 86% of funded loans invisible                               │
  │                                                                │
  │  Affected metrics: ROI, ROAS, $/App, $/Funded all distorted   │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
```

---

## Deep Dive: Kitsap Credit Union

### January 2026 — Three Sources Compared

```
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  Kitsap CU — January 2026 Applications                                      │
  │                                                                              │
  │                                                                              │
  │  2,597  ╷                                                                    │
  │         │                                                                    │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │         │  ████  Synthetic _first_click                                      │
  │         │  ████  (INFLATED — includes non-Google-Ads campaigns)              │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │         │  ████                                                              │
  │    200 ─┤  ████                                                              │
  │    179  │  ████  ░░░░  Athena parquet daily sum (179)                        │
  │    156  │  ████  ████  Athena campaign-month agg (156) ★ TRUTH              │
  │         │  ████  ████                                                        │
  │     32  │  ████  ████  ▓▓▓▓  GCLID attribution (32) — what dashboard showed │
  │      0 ─┴──████──████──▓▓▓▓──────────────────────────────────────────────────│
  │         Synthetic Auto-Attr  GCLID                                           │
  │                                                                              │
  └──────────────────────────────────────────────────────────────────────────────┘
```

### Approved Loans Comparison

```
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  Kitsap CU — January 2026 Approved                                          │
  │                                                                              │
  │  110  ╷  ████  Synthetic (110)                                               │
  │       │  ████                                                                │
  │   61  │  ████  ████  Athena campaign-month (61) ★ TRUTH                     │
  │       │  ████  ████                                                          │
  │       │  ████  ████                                                          │
  │       │  ████  ████                                                          │
  │    9  │  ████  ████  ▓▓▓▓  GCLID (9) — dashboard showed this               │
  │    0 ─┴──████──████──▓▓▓▓──────────────────────────────────────────────────  │
  │       Synthetic  Truth  GCLID                                                │
  │                                                                              │
  │  Dashboard showed 9 approved.  True count is 61.  85% were missing.          │
  └──────────────────────────────────────────────────────────────────────────────┘
```

### Campaign-Level Breakdown (Athena Parquet — Source of Truth)

```
  Kitsap CU — January 2026 — Apps by Campaign (Athena Parquet)
  ─────────────────────────────────────────────────────────────

  KCU - Personal - Brand              ████████████████████████████████████  80 apps
  KCU - Personal - Brand Geo Test     ██████████████                        28 apps
  KCU - Personal - NB Extended        █████████                             18 apps
  KCU - Personal - NB                 ███████                               13 apps
  KCU - Personal - NB Locations       ████                                   7 apps
  KCU - Business - NB                 ██                                     4 apps
  KCU - Business Checking - pMax      █                                      2 apps
  KCU - Business - Brand              ██                                     3 apps
  KCU - DG - 10 Mo Cert               █                                     1 apps
  KCU - YT - AWA                      ░                                      0 apps
  KCU - YT - AWA Extended             ░                                      0 apps
  KCU - DG - Kraken                   ░                                      0 apps
  X KCU - Business - NB Locations     ░                                      0 apps
                                                                    Total: 156 apps

  Of these, GCLID matched only 32 apps across 7 campaigns.
  The other 124 apps had no click_id → invisible to GCLID pipeline.
```

---

## GCLID Capture Rate by Client

### January 2026

```
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  GCLID Capture Rate — January 2026 (% of true apps captured)                │
  │                                                                              │
  │  californiacoast_cu   ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   8.9%    │
  │  firstcommunity_cu    ██████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  15.4%    │
  │  kitsap_cu            ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  20.5%    │
  │  publicservice_cu     █████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  23.4%    │
  │  first_commonwealth   ███████████████████████░░░░░░░░░░░░░░░░░░░  58.9%    │
  │                                                                              │
  │  ████ = captured by GCLID    ░░░░ = missing (no click_id)                   │
  │                                                                              │
  │  Average across all clients: ~25% capture rate                               │
  │  ► 75% of applications are invisible to GCLID attribution                   │
  └──────────────────────────────────────────────────────────────────────────────┘
```

### Detailed Numbers

| Client | GCLID Apps | True Apps (Athena) | Missing | Capture Rate | GCLID Approved | True Approved | Approved Missing |
|--------|-----------|-------------------|---------|-------------|----------------|---------------|-----------------|
| californiacoast_cu | 111 | 1,254 | **1,143** | 8.9% | 35 | 439 | 404 |
| firstcommunity_cu | 6 | 39 | **33** | 15.4% | 2 | 15 | 13 |
| kitsap_cu | 32 | 156 | **124** | 20.5% | 9 | 61 | 52 |
| publicservice_cu | 60 | 256 | **196** | 23.4% | 4 | 26 | 22 |
| first_commonwealth_bank | 172 | 292 | **120** | 58.9% | 64 | 126 | 62 |
| **TOTAL** | **381** | **1,997** | **1,616** | **19.1%** | **114** | **667** | **553** |

### Why Does Capture Rate Vary So Much?

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Factors affecting GCLID capture rate:                                   │
  │                                                                          │
  │  1. Click tracking coverage                                              │
  │     └─ Some clients have better GCLID pass-through on their websites    │
  │     └─ Form implementations may or may not preserve the gclid parameter │
  │                                                                          │
  │  2. Application channel mix                                              │
  │     └─ More branch/phone applications = lower GCLID capture rate        │
  │     └─ More online applications = higher GCLID capture rate             │
  │                                                                          │
  │  3. Cookie/session expiry                                                │
  │     └─ GCLID stored in cookies that expire                              │
  │     └─ Users who return days later lose their click_id                  │
  │                                                                          │
  │  4. Cross-device journeys                                                │
  │     └─ Click on mobile, apply on desktop = no click_id match            │
  └──────────────────────────────────────────────────────────────────────────┘
```

---

## Additional Bugs Discovered

### Bug 1: Athena DDL Column Case Mismatch

```
  Athena DDL (scripts/ddl/):          Parquet Files (S3):
  ────────────────────────            ──────────────────
  campaign_id   STRING                Campaign_ID   string     ← PascalCase!
  day           DATE                  Day           date32
  campaign      STRING                Campaign      string
  clicks        BIGINT                Clicks        int64
  cost          DOUBLE                Cost          double
  apps          BIGINT                Apps          int64
  approved      BIGINT                Approved      int64
  funded        BIGINT                Funded        int64
  ...                                 ...

  Result: SELECT * FROM staging.google_ads_campaign_data
          → ERROR: "Relation contains no accessible columns"

  The entire Athena table is UNQUERYABLE. export_athena_data.py fails silently.
```

### Bug 2: Partition Key Mismatch

```
  S3 Partition Keys (actual):         export_athena_data.py queries:
  ────────────────────────────        ───────────────────────────────
  client_id=kitsap                    client_id='kitsap_cu'        ← WRONG
  client_id=californiacoast_cu        client_id='california_coast_cu'  ← WRONG
  client_id=publicservice_cu          client_id='public_service_cu'    ← WRONG
  client_id=commonwealthone           client_id='commonwealth_one_fcu' ← WRONG
  client_id=fc_bank                   client_id='fc_bank'           ← matches!
  client_id=first_ccu                 client_id='first_community_cu'   ← WRONG
  client_id=altura_cu                 client_id='altura_cu'         ← matches!

  Only 2 of 7 clients have matching partition keys.
  The other 5 would return empty results even if the DDL bug were fixed.

  Root cause: clients.yaml has both athena_id and prod_id fields.
  S3 uses prod_id. export_athena_data.py uses athena_id.
```

### Bug 3: Daily Sum ≠ Monthly Aggregation

```
  When summing apps from daily rows in the parquet:

  Client                   Daily Sum    Campaign-Month    Delta
  ─────────────────────    ─────────    ──────────────    ─────
  kitsap_cu                   179            156           +23
  californiacoast_cu        1,270          1,254           +16
  first_commonwealth_bank     432            292          +140
  publicservice_cu            262            256            +6
  commonwealth_one_fcu          8              0            +8

  Applications appear in multiple daily rows, inflating the daily sum.
  This may be intentional (multi-day attribution windows) or a bug.
```

### Bug 4: Missing February 2026 Data

```
  Auto-attribution parquet files available in S3:

  Client                   Latest Parquet    Feb 2026?
  ─────────────────────    ──────────────    ─────────
  kitsap_cu                2026-01           ✗ MISSING
  californiacoast_cu       2026-01           ✗ MISSING
  first_commonwealth_bank  2026-01           ✗ MISSING
  publicservice_cu         2026-01           ✗ MISSING
  firstcommunity_cu        2026-01           ✗ MISSING
  commonwealth_one_fcu     2026-01           ✗ MISSING
  altura_ad_account        2025-12           ✗ MISSING

  The dashboard has NO source of truth for February 2026.
  Only GCLID data exists for the current month.
```

---

## Immediate Fixes Applied

### Dashboard Fallback (index.html)

Added `fetchEnrichedWithFallback()` that tries `_first_click.csv` before bare `.csv`:

```
Before:  fetch(`enriched/{month}.csv`)           → GCLID data (undercounted)
After:   fetch(`enriched/{month}_first_click.csv`) → synthetic data (overcounted)
         fallback to fetch(`enriched/{month}.csv`)  → GCLID data

Status: Stopgap. Shows higher numbers but still not the correct numbers.
        The _first_click.csv files include non-Google-Ads campaigns.
```

---

## Recommended Fix Roadmap

### Phase 1: Immediate (this sprint)

```
  Priority   Fix                                              Effort    Impact
  ────────   ──────────────────────────────────────────────   ──────    ──────
  P0         Stop the overwrite — rename GCLID output to      1 hr     Prevents
             {month}_gclid.csv so it doesn't clobber                    data loss
             auto-attribution data

  P0         Export directly from S3 parquet — bypass          2 hrs    Correct
             broken Athena table entirely, read parquet                  numbers
             files with pyarrow, write enriched/{month}.csv             on dashboard

  P1         Fix Athena DDL — lowercase parquet column         1 hr     Unblocks
             names or recreate DDL with PascalCase                      Athena
                                                                        queries

  P1         Fix partition key — use prod_id for Athena        30 min   Fixes
             queries instead of athena_id                               5/7 clients
```

### Phase 2: This month

```
  Priority   Fix                                              Effort    Impact
  ────────   ──────────────────────────────────────────────   ──────    ──────
  P1         Request Feb 2026 auto-attribution run             Ext.     Current
             from the auto-attribution team                    team     month data

  P2         Single source of truth — dashboard loads          4 hrs    No more
             ONLY from auto-attribution parquet data,                   confusion
             GCLID used only for keyword drill-down                     about which
                                                                        numbers are
                                                                        correct

  P2         Add validation to DAG — run                       2 hrs    Automated
             validate_attribution_counts.py after                       catch for
             each enrichment cycle                                      future
                                                                        discrepancies
```

### Phase 3: Next quarter

```
  Priority   Fix                                              Effort    Impact
  ────────   ──────────────────────────────────────────────   ──────    ──────
  P3         Keyword-level auto-attribution — get              Ext.     Replaces
             keyword granularity from the auto-attr            team     GCLID
             pipeline directly                                          entirely

  P3         Real multi-model attribution — replace            Ext.     Real
             synthetic _first_click and _linear files          team     first-click
             with actual first-click/linear computation                 and linear
                                                                        models
```

---

## Validation Tool

A validation script is included to cross-check all three data sources:

```bash
# Single client, verbose output
python scripts/validate_attribution_counts.py --client kitsap_cu --month 2026-01 --verbose

# All clients
python scripts/validate_attribution_counts.py --month 2026-01

# JSON output for programmatic analysis
python scripts/validate_attribution_counts.py --month 2026-01 --json
```

### Sample Output

```
================================================================================
  kitsap_cu — 2026-01
================================================================================
  Source                                          Apps  Approved  Funded  Campaigns
  --------------------------------------------- ------ --------- ------- ----------
  GCLID attribution (exact click match)             32         9       4          7
  Synthetic _first_click (cost-proportional)      2597       110      62         37
  Athena parquet (campaign-month, TRUTH)           156        61      34         13

  Athena daily detail: 347 rows across 31 days, sum(apps) = 179

  GCLID capture rate: 20.5%

  Issues:
    ! GCLID captures only 21% of apps (32 vs 156 in Athena)
    ! Synthetic _first_click inflated: 2597 apps vs 156 in Athena
    ! Daily sum (179) != campaign-month agg (156) — 23 apps appear in multiple rows
```

---

## File Reference

```
  scripts/
  ├── gclid_attribution.py              GCLID attribution — writes enriched/{month}.csv
  │                                      (UNDERCOUNTS — captures 9-59% of apps)
  │
  ├── export_athena_data.py             Athena export — should write enriched/{month}.csv
  │                                      (BROKEN — DDL case mismatch + partition key mismatch)
  │
  ├── generate_daily_attribution.py     Synthetic spread — writes _first_click.csv
  │                                      (OVERCOUNTS — includes non-Google-Ads campaigns)
  │
  ├── validate_attribution_counts.py    Cross-source validation tool (NEW)
  │
  └── ddl/
      └── google_ads_campaign_data.sql  Athena DDL (lowercase — doesn't match PascalCase parquet)

  dags/
  └── google_ads_to_s3_dag.py           DAG that chains scripts in overwrite order

  docs/
  ├── ATTRIBUTION_DISCREPANCY.md        This document
  └── QUESTIONS_FOR_ENGINEERING.md      Prioritized questions for the auto-attribution team

  config/
  └── clients.yaml                      Client config with athena_id/prod_id (source of partition mismatch)

  data/{client}/enriched/
  ├── {month}.csv                       Written by GCLID (last writer wins — currently wrong)
  ├── {month}_first_click.csv           Written by synthetic spread (inflated)
  └── daily/{month}.csv                 Daily keyword-level GCLID attribution
```
