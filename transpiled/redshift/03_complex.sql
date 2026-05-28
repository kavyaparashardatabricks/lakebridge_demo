-- ============================================================
-- COMPLEX: SUPER/JSON, materialized views, UDFs, advanced
--          Redshift-specific functions, recursive CTE, late
--          binding views, COPY/UNLOAD.
-- ============================================================

-- External schema reference (Redshift Spectrum / Glue)
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_raw
FROM DATA CATALOG
DATABASE 'raw_lake'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS

-- Table using SUPER for semi-structured data
CREATE OR REPLACE TABLE events.user_events (
    event_id STRING NOT NULL,
    user_id         BIGINT,
    event_ts        TIMESTAMP,
    event_type STRING ,
    payload         SUPER,            -- semi-structured JSON
    geo             SUPER,
    ingestion_date  DATE GENERATED ALWAYS AS (event_ts::DATE) STORED
)

ZORDER BY(event_ts);

-- Python UDF (Redshift-only, not in Spark)
CREATE OR REPLACE FUNCTION events.f_clean_email(email STRING)
RETURNS STRING
STABLE
AS $$
    import re
    if email is None:
        return None
    e = email.strip().lower()
    if not re.match(r`^[^@\s]+@[^@\s]+\.[^@\s]+$`, e):
        return None
    return e


-- SQL UDF
CREATE OR REPLACE FUNCTION events.f_bucket_amount(amt DECIMAL(12,2))
RETURNS STRING
IMMUTABLE
AS $$
    SELECT CASE
        WHEN $1 IS NULL              THEN 'unknown'
        WHEN $1 < 25                 THEN 'micro'
        WHEN $1 BETWEEN 25 AND 99.99 THEN 'small'
        WHEN $1 BETWEEN 100 AND 499  THEN 'medium'
        WHEN $1 BETWEEN 500 AND 1999 THEN 'large'
        ELSE 'whale'
    END


-- Materialized view with auto refresh
CREATE MATERIALIZED VIEW events.mv_daily_user_metrics
BACKUP NO

ZORDER BY(event_date)
AUTO REFRESH YES
AS
SELECT  ingestion_date                              AS event_date,
        user_id,
        COUNT(*)                                    AS event_count,
        COUNT(DISTINCT event_type)                  AS distinct_event_types,
        APPROXIMATE COUNT(DISTINCT event_id)        AS approx_unique_events,
        MIN(event_ts)                               AS first_event_ts,
        MAX(event_ts)                               AS last_event_ts
FROM    events.user_events
WHERE   event_ts >= DATEADD(day, -90, CURRENT_TIMESTAMP)
GROUP   BY 1, 2;
-- Sessionisation with gap detection + JSON navigation via SUPER
WITH typed_events AS (
SELECT
    user_id,
    event_id,
    event_ts,
    event_type,
    payload.`device_type`:: STRING AS device_type,
    payload.`utm`.`source`:: STRING AS utm_source,
    payload.`order`.`total`::DECIMAL(12,2) AS order_total,
    geo.`country`::CHAR(2) AS country,
    geo.`lat`::FLOAT AS lat,
    geo.`lon`::FLOAT AS lon
FROM events.user_events
WHERE event_ts >= DATEADD(day, -30, CURRENT_TIMESTAMP)
),
gapped AS (
SELECT
    *,
    LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts,
    CASE
                WHEN UNIX_TIMESTAMP(event_ts) - UNIX_TIMESTAMP(LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts)) > 1800
                  OR LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL
                THEN 1 ELSE 0
            END AS is_session_start
FROM typed_events
),
sessionised AS (
SELECT
    *,
    SUM(is_session_start) OVER (PARTITION BY user_id
                                        ORDER BY event_ts
                                        ROWS BETWEEN UNBOUNDED PRECEDING
                                                 AND CURRENT ROW) AS session_no
FROM gapped
),
session_agg AS (
SELECT
    user_id,
    session_no,
    MIN(event_ts) AS session_start,
    MAX(event_ts) AS session_end,
    UNIX_TIMESTAMP(MAX(event_ts)) - UNIX_TIMESTAMP(MIN(event_ts)) AS duration_s,
    COUNT(*) AS event_count,
    SUM(COALESCE(order_total, 0)) AS gmv,
    events.f_bucket_amount(SUM(COALESCE(order_total, 0))) AS gmv_bucket,
    array_join(array_distinct(split(LISTAGG ( utm_source, '|')  
                WITHIN GROUP (ORDER BY utm_source),'|')),'|')  AS utm_sources,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lat) AS median_lat,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lon) AS median_lon,
    ANY_VALUE(device_type) AS device_type,
    ANY_VALUE(country) AS country
FROM sessionised
GROUP BY 1, 2
)
SELECT
    *,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_s)
            OVER (PARTITION BY country) AS p95_duration_country,
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id
                                   ORDER BY session_start DESC) <= 100  -- last 100
FROM session_agg
WHERE duration_s > 0
;


-- Recursive CTE: org/category hierarchy walk
WITH RECURSIVE category_tree (category_id, parent_id, name, depth, path) AS (
    SELECT  category_id,
            parent_id,
            name,
            0                                         AS depth,
            name:: STRING                       AS path
    FROM    catalog.categories
    WHERE   parent_id IS NULL
    UNION ALL
    SELECT  c.category_id,
            c.parent_id,
            c.name,
            ct.depth + 1,
            (ct.path || ' > ' || c.name):: STRING
    FROM    catalog.categories c
    JOIN    category_tree      ct ON ct.category_id = c.parent_id
    WHERE   ct.depth < 8
)
SELECT * FROM category_tree ORDER BY path;

-- COPY from S3 with manifest, region, IAM role
COPY events.user_events
FROM 's3://my-bucket/events/manifests/2026-05-28.manifest'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftLoader'
FORMAT AS JSON 'auto'
GZIP
MANIFEST
REGION 'us-east-1'
TIMEFORMAT 'auto'
TRUNCATECOLUMNS
ACCEPTINVCHARS
MAXERROR 100;

-- UNLOAD to S3
UNLOAD ('SELECT * FROM events.mv_daily_user_metrics WHERE event_date = DATE_ADD(current_date, -1)')
TO 's3://my-bucket/unloads/daily_user_metrics/dt=2026-05-27/part_'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftUnloader'
FORMAT AS PARQUET
PARTITION BY (event_date)
CLEANPATH
MAXFILESIZE 256 MB;

-- Late binding view referencing external (Spectrum) table
CREATE OR REPLACE VIEW reports.v_enriched_events

AS
SELECT  e.event_id,
        e.user_id,
        e.event_ts,
        e.event_type,
        s.session_id,
        events.f_clean_email(s.user_email)            AS clean_email,
        e.payload.`page`:: STRING                AS page
FROM    events.user_events            e
LEFT JOIN spectrum_raw.session_lookup s
       ON s.user_id = e.user_id
      AND e.event_ts BETWEEN s.session_start AND s.session_end;
