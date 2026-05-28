-- ============================================================
-- MEDIUM: Time travel (AT/BEFORE), zero-copy CLONE, UNDROP,
-- DATA_RETENTION_TIME, table SWAP, tag/comment management.
-- ============================================================

USE DATABASE demo_db;
USE SCHEMA   sales;

-- Set retention so time travel is meaningful
ALTER TABLE sales.orders SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Tag the table for cataloging
CREATE TAG IF NOT EXISTS demo_db.governance.data_sensitivity;
ALTER TABLE sales.orders SET TAG demo_db.governance.data_sensitivity = 'CONFIDENTIAL';

-- Query at a specific timestamp (time travel)
SELECT order_id, total_amount, order_status
FROM   sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ)
WHERE  customer_id = 42;

-- Query at a specific point in the past
SELECT *
FROM   sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ)
LIMIT 100;

-- Zero-copy CLONE for dev environments (schema/database level only —
-- table-level CLONE is a Snowflake feature that maps to Databricks
-- DEEP/SHALLOW CLONE which Lakebridge generates separately).
CREATE OR REPLACE SCHEMA dev_db.sales CLONE sales;

CREATE OR REPLACE DATABASE demo_db_dev CLONE demo_db;

-- UNDROP after an accidental DROP
DROP TABLE sales.order_items;
UNDROP TABLE sales.order_items;

-- Atomic SWAP for blue/green table releases
CREATE OR REPLACE TABLE sales.orders_v2 (
    order_id        NUMBER(18,0),
    customer_id     NUMBER(18,0),
    order_ts        TIMESTAMP_NTZ,
    order_status    STRING,
    total_amount    NUMBER(12,2),
    currency        STRING,
    channel         STRING
);
-- ... ETL populates orders_v2 ...
ALTER TABLE sales.orders SWAP WITH sales.orders_v2;

-- Restore a row using time travel + MERGE
MERGE INTO sales.orders tgt
USING (
    SELECT *
    FROM   sales.orders AT(TIMESTAMP => '2026-05-27 00:00:00'::TIMESTAMP_NTZ)
    WHERE  order_id IN (101, 102, 103)
) src
ON  tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET
    total_amount = src.total_amount,
    order_status = src.order_status
WHEN NOT MATCHED THEN INSERT (
    order_id, customer_id, order_ts, order_status, total_amount, channel
) VALUES (
    src.order_id, src.customer_id, src.order_ts,
    src.order_status, src.total_amount, src.channel
);

-- Comments + masking-readiness metadata
COMMENT ON TABLE  sales.orders      IS 'Authoritative orders fact table';
COMMENT ON COLUMN sales.orders.email_attempted IS 'PII candidate';

-- INFORMATION_SCHEMA inspection
SELECT  table_name,
        row_count,
        bytes,
        retention_time
FROM    demo_db.information_schema.tables
WHERE   table_schema = 'SALES'
ORDER   BY bytes DESC;
