-- ============================================================
-- SIMPLE: SELECT, filter, group, basic Snowflake date/string
-- functions: CURRENT_DATE, DATEADD, DATEDIFF, IFNULL, IFF,
-- INITCAP, ILIKE.
-- ============================================================

USE DATABASE demo_db;
USE SCHEMA   sales;

-- Filtered list
SELECT customer_id,
       INITCAP(first_name) || ' ' || INITCAP(last_name) AS full_name,
       LOWER(email)                                     AS email,
       country,
       signup_ts::DATE                                  AS signup_date
FROM   customers
WHERE  is_active = TRUE
  AND  signup_ts >= DATEADD(month, -6, CURRENT_TIMESTAMP())
  AND  email ILIKE '%@example.com'
ORDER  BY signup_ts DESC;

-- Simple group-by with Snowflake-specific NULL helpers
SELECT country,
       COUNT(*)                                          AS customer_count,
       COUNT_IF(is_active)                               AS active_count,
       IFNULL(AVG(DATEDIFF(day, signup_ts, CURRENT_DATE())), 0) AS avg_tenure_days,
       IFF(COUNT(*) > 100, 'large', 'small')             AS segment_size
FROM   customers
GROUP  BY country
HAVING COUNT(*) > 0
ORDER  BY customer_count DESC;

-- Simple UPDATE / DELETE
UPDATE customers
SET    is_active = FALSE
WHERE  signup_ts < DATEADD(year, -3, CURRENT_DATE());

DELETE FROM customers
WHERE  email IS NULL
   OR  TRIM(email) = '';

-- Catalog inspection via INFORMATION_SCHEMA (portable to Databricks)
SELECT table_name,
       row_count,
       bytes
FROM   demo_db.information_schema.tables
WHERE  table_schema = 'SALES'
ORDER  BY bytes DESC;
