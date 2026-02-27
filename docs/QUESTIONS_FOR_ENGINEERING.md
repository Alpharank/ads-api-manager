# Questions for Engineering — Attribution Pipeline

These questions need answers to fully resolve the attribution data discrepancy and unblock accurate dashboard reporting.

---

## Critical (Blocking)

### 1. Parquet Column Case: PascalCase or lowercase?

The `staging.google_ads_campaign_data` parquet files use PascalCase columns (`Campaign_ID`, `Day`, `Cost`, `Apps`) but the Athena DDL expects lowercase (`campaign_id`, `day`, `cost`, `apps`). This makes the table **completely unqueryable** via Athena.

- **Who writes these parquet files?** Is it the `update_google_ads_roi` DAG or another pipeline?
- **Can we change the column casing at write time** to lowercase? Or should we recreate the DDL with PascalCase?
- **Is anyone successfully querying this table today?** If so, how?

### 2. Partition Key: `prod_id` vs `athena_id`

S3 partitions use `prod_id` values (e.g., `client_id=kitsap`) but our `export_athena_data.py` queries with `athena_id` (e.g., `client_id=kitsap_cu`). They never match.

- **Which is the canonical client identifier** — `prod_id` or `athena_id`?
- **Should we change our config** to use `prod_id` for Athena queries, or should the upstream pipeline change its partition keys?

### 3. February 2026 Auto-Attribution Data

No parquet files exist for February 2026. Latest available is January 2026.

- **What is the refresh cadence** for `staging.google_ads_campaign_data`? Daily? Monthly?
- **Is the February run scheduled?** If so, when?
- **Is there a way to manually trigger it** for a specific month?

---

## Important (Design Decisions)

### 4. Which Attribution Model Should the Dashboard Use?

Three data sources exist with very different numbers:

| Source | Kitsap Jan 2026 Apps | Method |
|--------|---------------------|--------|
| GCLID attribution | 32 | Exact click_id match |
| Auto-attribution (Athena parquet) | 156 | Full pipeline attribution |
| Synthetic _first_click | 2,597 | Cost-proportional spread of ALL campaigns |

- **Which number should clients see on the dashboard?** The 156 from auto-attribution is the most defensible. The 32 from GCLID is the most conservative. The 2,597 is synthetic and should not be used.
- **Should we show the GCLID data separately** (e.g., as "click-attributed" vs "all attributed")?
- **Do clients currently see auto-attribution numbers elsewhere** (Explo, internal reports)? If so, the dashboard needs to match.

### 5. GCLID Attribution: Keep, Replace, or Supplement?

GCLID attribution (`gclid_attribution.py`) captures 9–59% of apps depending on the client. It provides **keyword-level granularity** that auto-attribution doesn't.

- **Is keyword-level attribution from auto-attribution planned?** If so, we could replace GCLID entirely.
- **Should GCLID data be shown as a separate "click-attributed" view** rather than the primary view?
- **Is there a plan to increase click_id coverage?** (e.g., via enhanced conversions, offline conversion import)

### 6. File Write Conflicts

Both `gclid_attribution.py` and `export_athena_data.py` write to `data/{client}/enriched/{month}.csv`. The DAG runs them in sequence, so GCLID always overwrites auto-attribution.

- **Should we change `gclid_attribution.py` to write to `{month}_gclid.csv`?**
- **Or should the DAG order be reversed** (GCLID first, auto-attribution second)?
- **Is there a reason they share the same output path?** (Historical artifact?)

---

## Nice to Know

### 7. Daily Sum ≠ Campaign-Month Aggregation

When summing `apps` across all daily rows in the parquet, we get a higher number than grouping by campaign and summing (e.g., 179 vs 156 for Kitsap). This suggests some applications are counted in multiple daily rows.

- **Is this expected behavior?** (e.g., multi-day attribution windows)
- **Which aggregation level is correct** — daily sum or campaign-month?

### 8. Non-Google-Ads Campaigns in Auto-Attribution

The auto-attribution parquet files contain only 13 campaigns for Kitsap, but the `_first_click.csv` synthetic files have 37 campaigns. The extra 24 campaigns appear to be non-Google-Ads campaigns (organic, direct, etc.).

- **Where do the 37 campaigns come from?** Are they from a different Athena table or a different data source?
- **Should the dashboard show non-Google-Ads campaigns** or only Google Ads campaigns?

### 9. `staging.google_ads_campaign_data` Table Ownership

- **Who owns and maintains this table?** Which team/DAG writes the parquet files?
- **Is there documentation for the schema and data contract?**
- **Are there alerts for when the pipeline fails** or data is stale?

### 10. Athena Region

The `export_athena_data.py` script uses `us-east-1` but the S3 bucket (`etl.alpharank.airflow`) is in `us-west-2`. Both regions have Athena workgroups.

- **Which region should we be querying?** The staging data appears to be in `us-west-2` based on the S3 bucket location.
- **Is there cross-region replication** or are they independent?

---

## Action Items After Answers

| After answering | We can |
|----------------|--------|
| Q1 + Q2 | Fix `export_athena_data.py` to query Athena successfully |
| Q3 | Determine if Feb 2026 dashboard data is possible |
| Q4 | Set the correct attribution source for the dashboard |
| Q5 + Q6 | Restructure file outputs to prevent overwriting |
| Q7 | Decide on aggregation logic for Athena keyword export |
