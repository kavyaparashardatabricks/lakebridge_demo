CREATE
/*
        WAREHOUSE IF NOT EXISTS analytics_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
        -- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE WAREHOUSE command, but may be able to do so in the future
    */;

CREATE /* DATABASE IF NOT EXISTS demo_db */;
-- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE DATABASE command, but may be able to do so in the future

CREATE SCHEMA IF NOT EXISTS demo_db.sales;

/* USE WAREHOUSE analytics_wh; */
-- FIXME: SNOWFLAKE: The transpiler cannot currently convert USE command variant, but may be able to do so in the future

USE CATALOG demo_db;

USE SCHEMA sales;

CREATE OR REPLACE
    TABLE sales.customers
    (
        customer_id DECIMAL(18, 0) GENERATED ALWAYS AS IDENTITY,
        first_name VARCHAR(16777216),
        last_name VARCHAR(16777216),
        email VARCHAR(16777216),
        signup_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        country CHAR(2),
        is_active BOOLEAN DEFAULT true,
        attributes VARIANT
    ) TBLPROPERTIES( 'delta.feature.allowColumnDefaults' = 'supported' )
    CLUSTER BY country, signup_ts;

CREATE OR REPLACE
    TEMPORARY TABLE sales.customers_stg
    (
        customer_id DECIMAL(18, 0),
        first_name VARCHAR(16777216),
        last_name VARCHAR(16777216),
        email VARCHAR(16777216),
        signup_ts TIMESTAMP_NTZ,
        country CHAR(2),
        is_active BOOLEAN,
        attributes VARIANT
    );

INSERT INTO sales.customers (first_name, last_name, email, country)
VALUES
    ('Alice', 'Singh', 'alice@example.com', 'IN'),
    ('Bob', 'Khan', 'bob@example.com', 'US'),
    ('Charlie', 'Lopez', 'chuck@example.com', 'MX');

SELECT customer_id, first_name || ' ' || last_name AS full_name, email, country FROM sales.customers WHERE is_active
ORDER BY signup_ts DESC NULLS FIRST
LIMIT 100;