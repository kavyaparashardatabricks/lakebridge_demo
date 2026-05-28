-- ============================================================
-- SIMPLE: Database / schema / warehouse setup, CREATE TABLE,
-- basic DML. Snowflake-specific keywords: WAREHOUSE,
-- TRANSIENT, AUTOINCREMENT, NUMBER, VARIANT, CLUSTER BY.
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS analytics_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS demo_db;
CREATE SCHEMA   IF NOT EXISTS demo_db.sales;

USE WAREHOUSE analytics_wh;
USE DATABASE  demo_db;
USE SCHEMA    sales;

-- Standard table with Snowflake-specific types & clustering
CREATE OR REPLACE TABLE sales.customers (
    customer_id     NUMBER(18,0) AUTOINCREMENT START 1 INCREMENT 1,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    signup_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    country         CHAR(2),
    is_active       BOOLEAN DEFAULT TRUE,
    attributes      VARIANT
)
CLUSTER BY (country, signup_ts);

-- Transient (no fail-safe) staging table — mirror columns explicitly
CREATE OR REPLACE TRANSIENT TABLE sales.customers_stg (
    customer_id     NUMBER(18,0),
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    signup_ts       TIMESTAMP_NTZ,
    country         CHAR(2),
    is_active       BOOLEAN,
    attributes      VARIANT
);

INSERT INTO sales.customers (first_name, last_name, email, country)
VALUES
    ('Alice',   'Singh',  'alice@example.com',  'IN'),
    ('Bob',     'Khan',   'bob@example.com',    'US'),
    ('Charlie', 'Lopez',  'chuck@example.com',  'MX');

SELECT customer_id,
       first_name || ' ' || last_name AS full_name,
       email,
       country
FROM   sales.customers
WHERE  is_active
ORDER  BY signup_ts DESC
LIMIT  100;
