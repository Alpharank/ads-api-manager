-- Keyword-level Google Ads data with attribution metrics.
-- Joins keyword performance (clicks, cost) with GCLID-based attribution
-- (apps, approved, funded, value, product breakdowns).
--
-- Partitioned by client_id for efficient per-client queries.
-- Written as Snappy-compressed Parquet by scripts/export_keyword_to_athena.py.

CREATE EXTERNAL TABLE IF NOT EXISTS staging.google_ads_keyword_data (
    campaign_id     STRING,
    day             DATE,
    campaign        STRING,
    ad_group_id     STRING,
    ad_group        STRING,
    keyword         STRING,
    match_type      STRING,
    clicks          BIGINT,
    cost            DOUBLE,
    apps            BIGINT,
    approved        BIGINT,
    funded          BIGINT,
    production      DOUBLE,
    value           DOUBLE,
    cc_appvd        BIGINT,
    deposit_appvd   BIGINT,
    personal_appvd  BIGINT,
    vehicle_appvd   BIGINT,
    heloc_appvd     BIGINT
)
PARTITIONED BY (client_id STRING)
STORED AS PARQUET
LOCATION 's3://etl.alpharank.airflow/staging/google_ads_keyword_data/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
