-- ============================================================
-- COMPLEX: Materialized views, multi-layer aggregates, search
-- optimization, tagging, secure views (governance pattern that
-- mimics column masking / row filtering without policy syntax).
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.gov;
USE SCHEMA gov;

-- Tagging for governance
CREATE OR REPLACE TAG gov.pii;

-- Mapping of role -> allowed country (row-level filter)
CREATE OR REPLACE TABLE gov.role_country_map (
    role_name STRING,
    country   CHAR(2)
);

-- Search optimization for point lookups
ALTER TABLE demo_db.sales.orders ADD SEARCH OPTIMIZATION;

-- Layer 1: per-customer lifetime aggregate
CREATE OR REPLACE MATERIALIZED VIEW demo_db.sales.mv_customer_lifetime
    CLUSTER BY (country)
AS
SELECT  o.customer_id,
        COUNT(*)                                              AS lifetime_orders,
        SUM(o.total_amount)                                   AS lifetime_revenue,
        MIN(o.order_ts)                                       AS first_order_ts,
        MAX(o.order_ts)                                       AS last_order_ts,
        ANY_VALUE(c.country)                                  AS country,
        DATEDIFF(day, MIN(o.order_ts), MAX(o.order_ts))       AS active_span_days
FROM    demo_db.sales.orders    o
JOIN    demo_db.sales.customers c
  ON    c.customer_id = o.customer_id
WHERE   o.order_status = 'COMPLETED'
GROUP   BY o.customer_id;

-- Layer 2: per-country rollup
CREATE OR REPLACE MATERIALIZED VIEW demo_db.sales.mv_country_summary
AS
SELECT  country,
        SUM(lifetime_revenue)                                 AS country_revenue,
        COUNT(*)                                              AS customers,
        AVG(lifetime_revenue)                                 AS arpu,
        APPROX_PERCENTILE(lifetime_revenue, 0.95)             AS p95_revenue
FROM    demo_db.sales.mv_customer_lifetime
GROUP   BY country;

-- Top-N country view (regular view sitting on the MV)
CREATE OR REPLACE VIEW demo_db.sales.v_top_countries
AS
SELECT  country,
        country_revenue,
        customers,
        arpu,
        revenue_rank
FROM (
    SELECT  country,
            country_revenue,
            customers,
            arpu,
            RANK() OVER (ORDER BY country_revenue DESC) AS revenue_rank
    FROM    demo_db.sales.mv_country_summary
)
WHERE revenue_rank <= 25;

-- Secure view that masks email + filters by country mapping
-- (equivalent semantics to MASKING POLICY + ROW ACCESS POLICY)
CREATE OR REPLACE SECURE VIEW demo_db.sales.v_customer_safe
AS
SELECT  c.customer_id,
        c.first_name,
        c.last_name,
        CASE
            WHEN CURRENT_ROLE() IN ('SECURITY_ADMIN', 'PII_ADMIN') THEN c.email
            WHEN c.email IS NULL                                   THEN NULL
            ELSE REGEXP_REPLACE(c.email, '(.)(.*)(@.*)', '\\1***\\3')
        END                                              AS email,
        c.country,
        c.signup_ts,
        c.is_active
FROM    demo_db.sales.customers c
WHERE   CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SECURITY_ADMIN')
   OR   c.country IN (
            SELECT m.country
            FROM   gov.role_country_map m
            WHERE  m.role_name = CURRENT_ROLE()
        );

-- Manual refresh + inspection (Snowflake auto-maintains MVs, but
-- you can still query metadata)
SELECT  table_name,
        last_altered,
        bytes,
        row_count
FROM    demo_db.information_schema.tables
WHERE   table_schema = 'SALES'
  AND   table_type   = 'MATERIALIZED VIEW'
ORDER BY last_altered DESC;
