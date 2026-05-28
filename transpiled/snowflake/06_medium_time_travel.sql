USE CATALOG demo_db;

USE SCHEMA sales;

ALTER TABLE sales.orders /* ??? */;
-- FIXME: Unexpected table alteration UnresolvedTableAlteration(SET DATA_RETENTION_TIME_IN_DAYS = 14,SNOWFLAKE: An error occurred while traversing the parse tree: The COLUMN action was parsed, but is unknown to the SQL builders!,tableColumnAction,Some(SET),Error)

CREATE /* TAG IF NOT EXISTS demo_db.governance.data_sensitivity */;
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE TAG command, and it cannot be translated

ALTER TABLE sales.orders /* ??? */;
-- FIXME: Unexpected table alteration UnresolvedTableAlteration(SET TAG demo_db.governance.data_sensitivity = 'CONFIDENTIAL',SNOWFLAKE: An error occurred while traversing the parse tree: The COLUMN action was parsed, but is unknown to the SQL builders!,tableColumnAction,Some(SET),Error)

SELECT order_id, total_amount, order_status
FROM /* sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ) */ WHERE customer_id = 42;
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to AT/BEFORE, and it cannot be translated

SELECT * FROM /* sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ) */ LIMIT 100;
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to AT/BEFORE, and it cannot be translated

CREATE OR REPLACE SCHEMA dev_db.sales /* SCHEMA dev_db.sales CLONE sales */;
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CLONE clause in CREATE SCHEMA, and it cannot be translated

CREATE OR REPLACE /* DATABASE demo_db_dev CLONE demo_db */;
-- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE DATABASE command, but may be able to do so in the future

DROP TABLE sales.order_items;

/* UNDROP TABLE sales.order_items; */
-- FIXME: SNOWFLAKE: The transpiler cannot currently convert UNDROP commands are, but may be able to do so in the future

CREATE OR REPLACE
    TABLE sales.orders_v2
    (
        order_id DECIMAL(18, 0),
        customer_id DECIMAL(18, 0),
        order_ts TIMESTAMP_NTZ,
        order_status VARCHAR(16777216),
        total_amount DECIMAL(12, 2),
        currency VARCHAR(16777216),
        channel VARCHAR(16777216)
    );

ALTER TABLE sales.orders /* ??? */;
-- FIXME: Unexpected table alteration UnresolvedTableAlteration(SWAP WITH sales.orders_v2,SNOWFLAKE: An error occurred while traversing the parse tree: The COLUMN action was parsed, but is unknown to the SQL builders!,tableColumnAction,Some(SWAP),Error)

MERGE INTO sales.orders AS tgt
USING
(
        SELECT *
        FROM /* sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ) */ WHERE order_id IN (101, 102, 103)
        -- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to AT/BEFORE, and it cannot be translated
    ) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET total_amount = src.total_amount, order_status = src.order_status
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_ts, order_status, total_amount, channel) VALUES
    (src.order_id, src.customer_id, src.order_ts, src.order_status, src.total_amount, src.channel);

/* COMMENT ON TABLE  sales.orders      IS 'Authoritative orders fact table' */
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the COMMENT command, and it cannot be translated

/* COMMENT ON COLUMN sales.orders.email_attempted IS 'PII candidate' */
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the COMMENT command, and it cannot be translated

SELECT table_name, row_count, bytes, retention_time FROM demo_db.information_schema.tables WHERE table_schema = 'SALES'
ORDER BY bytes DESC NULLS FIRST;