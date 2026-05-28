USE CATALOG demo_db;

CREATE SCHEMA IF NOT EXISTS demo_db.udf;

USE SCHEMA udf;

CREATE OR REPLACE
    FUNCTION udf.f_bucket_amount(amt DECIMAL(12, 2))
    RETURNS VARCHAR(16777216)
    RETURN
        CASE
            WHEN amt IS NULL THEN 'unknown'
            WHEN amt < 25 THEN 'micro'
            WHEN amt BETWEEN 25 AND 99.99 THEN 'small'
            WHEN amt BETWEEN 100 AND 499 THEN 'medium'
            WHEN amt BETWEEN 500 AND 1999 THEN 'large'
            ELSE 'whale'
        END;

CREATE OR REPLACE
/*
        FUNCTION udf.f_extract_domain(email VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        RETURN /* source body omitted */
    */;

CREATE OR REPLACE
/*
        FUNCTION udf.f_levenshtein(a VARCHAR(16777216), b VARCHAR(16777216))
        RETURNS DECIMAL(38, 0)
        RETURN /* source body omitted */
    */;

CREATE OR REPLACE
    /* FUNCTION udf.f_b64encode(s VARCHAR(16777216)) RETURNS VARCHAR(16777216) RETURN /* source body omitted */ */;
    -- FIXME: CREATE FUNCTION with JAVA runtime is not translatable to Databricks SQL CREATE FUNCTION

CREATE OR REPLACE /* FUNCTION udf.f_split_kv(log VARCHAR(16777216)) RETURNS STRUCT<> RETURN /* source body omitted */ */;
-- FIXME: CREATE FUNCTION with PYTHON runtime is not translatable to Databricks SQL CREATE FUNCTION

SELECT e.event_id, kv.k, kv.v
FROM events.raw_events AS e, LATERAL TABLE(udf.F_SPLIT_KV(CAST(e.payload:raw_log AS VARCHAR(16777216)))) AS kv;

CREATE OR REPLACE
    FUNCTION udf.f_mask_pan(pan VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    RETURN CASE WHEN pan IS NULL OR LENGTH(pan) < 4 THEN NULL ELSE REPEAT('*', LENGTH(pan) - 4) || RIGHT(pan, 4) END;

CREATE OR REPLACE
    FUNCTION udf.f_score_fraud(features VARIANT)
    RETURNS DECIMAL(6, 4)
    RETURN
        LEAST(
            1.0,
            GREATEST(
                0.0,
                0.001 * CAST(features:amount AS DECIMAL(38, 0)) +
                CASE CAST(features:channel AS VARCHAR(16777216))
                    WHEN 'WEB' THEN 0.20 WHEN 'CALLCENTER' THEN 0.10 ELSE 0.05
                END +
                CASE CAST(features:country AS VARCHAR(16777216))
                    WHEN 'US' THEN 0.00 WHEN 'IN' THEN 0.05 ELSE 0.15
                END
            )
        );

SELECT
    o.order_id,
    udf.F_EXTRACT_DOMAIN(c.email) AS email_domain,
    udf.F_BUCKET_AMOUNT(o.total_amount) AS amt_bucket,
    udf.F_MASK_PAN(o.card_pan) AS masked_pan,
    udf.F_SCORE_FRAUD(STRUCT(o.total_amount AS amount, o.channel AS channel, c.country AS country)) AS fraud_score
FROM
    sales.orders AS o JOIN sales.customers AS c USING (customer_id)
    WHERE o.order_ts >= DATE_ADD(day, -1, CURRENT_TIMESTAMP());