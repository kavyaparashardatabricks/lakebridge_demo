USE CATALOG demo_db;

CREATE SCHEMA IF NOT EXISTS demo_db.gov;

USE SCHEMA gov;

CREATE OR REPLACE /* TAG gov.pii */;
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE TAG command, and it cannot be translated

CREATE OR REPLACE TABLE gov.role_country_map (role_name VARCHAR(16777216), country CHAR(2));

ALTER TABLE demo_db.sales.orders ADD COLUMN SEARCH OPTIMIZATION;
-- FIXME: Unsupported data type OPTIMIZATION

CREATE OR REPLACE
    MATERIALIZED VIEW demo_db.sales.mv_customer_lifetime
    AS
        SELECT
            o.customer_id,
            COUNT(*) AS lifetime_orders,
            SUM(o.total_amount) AS lifetime_revenue,
            MIN(o.order_ts) AS first_order_ts,
            MAX(o.order_ts) AS last_order_ts,
            ANY_VALUE(c.country) AS country,
            DATEDIFF(day, MIN(o.order_ts), MAX(o.order_ts)) AS active_span_days
        FROM
            demo_db.sales.orders AS o JOIN demo_db.sales.customers AS c ON c.customer_id = o.customer_id
            WHERE o.order_status = 'COMPLETED'
            GROUP BY o.customer_id;

CREATE OR REPLACE
    MATERIALIZED VIEW demo_db.sales.mv_country_summary
    AS
        SELECT
            country,
            SUM(lifetime_revenue) AS country_revenue,
            COUNT(*) AS customers,
            AVG(lifetime_revenue) AS arpu,
            APPROX_PERCENTILE(lifetime_revenue, 0.95) AS p95_revenue
        FROM demo_db.sales.mv_customer_lifetime GROUP BY country;

CREATE OR REPLACE
    VIEW demo_db.sales.v_top_countries
    AS
        SELECT country, country_revenue, customers, arpu, revenue_rank
        FROM
(
                SELECT
                    country,
                    country_revenue,
                    customers,
                    arpu,
                    RANK() OVER (ORDER BY country_revenue DESC NULLS FIRST) AS revenue_rank
                FROM demo_db.sales.mv_country_summary
            )
            WHERE revenue_rank <= 25;

CREATE OR REPLACE
    VIEW demo_db.sales.v_customer_safe
    AS
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            CASE
                WHEN /* CURRENT_ROLE() */ IN ('SECURITY_ADMIN', 'PII_ADMIN') THEN c.email
                -- FIXME: Function CURRENT_ROLE is not convertible to Databricks SQL
                WHEN c.email IS NULL THEN NULL
                ELSE REGEXP_REPLACE(c.email, '(.)(.*)(@.*)', '\\1***\\3')
            END AS email,
            c.country,
            c.signup_ts,
            c.is_active
        FROM
            demo_db.sales.customers AS c
            WHERE
                /* CURRENT_ROLE() */ IN ('ACCOUNTADMIN', 'SECURITY_ADMIN') OR
                -- FIXME: Function CURRENT_ROLE is not convertible to Databricks SQL
                c.country IN
                (SELECT m.country FROM gov.role_country_map AS m WHERE m.role_name = /* CURRENT_ROLE() */);
                -- FIXME: Function CURRENT_ROLE is not convertible to Databricks SQL

SELECT table_name, last_altered, bytes, row_count
FROM demo_db.information_schema.tables WHERE table_schema = 'SALES' AND table_type = 'MATERIALIZED VIEW'
ORDER BY last_altered DESC NULLS FIRST;