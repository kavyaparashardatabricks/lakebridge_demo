-- internal error
-- Multiple errors: 
-- ============================================================
-- COMPLEX: Streams (CDC), Tasks (cron + DAG), MERGE-based
-- incrementals, append-only vs full streams, SYSTEM$ funcs.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.cdc;
USE SCHEMA cdc;

-- Bronze landing
CREATE OR REPLACE TABLE cdc.bronze_orders (
    order_id        NUMBER(18,0),
    customer_id     NUMBER(18,0),
    order_ts        TIMESTAMP_NTZ,
    order_status    STRING,
    total_amount    NUMBER(12,2),
    src_load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver curated
CREATE OR REPLACE TABLE cdc.silver_orders (
    order_id        NUMBER(18,0) NOT NULL,
    customer_id     NUMBER(18,0),
    order_ts        TIMESTAMP_NTZ,
    order_status    STRING,
    total_amount    NUMBER(12,2),
    is_high_value   BOOLEAN,
    inserted_at     TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    is_deleted      BOOLEAN DEFAULT FALSE
);

-- Streams capture INSERT/UPDATE/DELETE on bronze
CREATE OR REPLACE STREAM cdc.bronze_orders_stream
    ON TABLE cdc.bronze_orders
    SHOW_INITIAL_ROWS = TRUE;

-- Append-only stream variant
CREATE OR REPLACE STREAM cdc.bronze_orders_append_stream
    ON TABLE cdc.bronze_orders
    APPEND_ONLY = TRUE;

-- Gold aggregate
CREATE OR REPLACE TABLE cdc.gold_customer_daily (
    customer_id     NUMBER(18,0),
    order_date      DATE,
    order_count     NUMBER,
    revenue         NUMBER(14,2),
    high_value_cnt  NUMBER,
    updated_at      TIMESTAMP_NTZ
);

-- Root task: merge bronze -> silver every 5 min
CREATE OR REPLACE TASK cdc.t_silver_merge
    WAREHOUSE = analytics_wh
    SCHEDULE  = 'USING CRON */5 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('cdc.bronze_orders_stream')
AS
MERGE INTO cdc.silver_orders tgt
USING (
    SELECT  order_id,
            customer_id,
            order_ts,
            order_status,
            total_amount,
            total_amount > 1000           AS is_high_value,
            METADATA$ACTION               AS action,
            METADATA$ISUPDATE             AS is_update,
            METADATA$ROW_ID               AS row_id
    FROM    cdc.bronze_orders_stream
) src
ON  tgt.order_id = src.order_id
WHEN MATCHED AND src.action = 'DELETE' THEN UPDATE SET
    is_deleted = TRUE,
    updated_at = CURRENT_TIMESTAMP()
WHEN MATCHED AND src.action = 'INSERT' AND src.is_update THEN UPDATE SET
    customer_id   = src.customer_id,
    order_ts      = src.order_ts,
    order_status  = src.order_status,
    total_amount  = src.total_amount,
    is_high_value = src.is_high_value,
    updated_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND src.action = 'INSERT' THEN INSERT (
    order_id, customer_id, order_ts, order_status,
    total_amount, is_high_value, inserted_at, updated_at, is_deleted
) VALUES (
    src.order_id, src.customer_id, src.order_ts, src.order_status,
    src.total_amount, src.is_high_value, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), FALSE
);

-- Child task: rebuild gold aggregate after silver merge
CREATE OR REPLACE TASK cdc.t_gold_agg
    WAREHOUSE = analytics_wh
    AFTER cdc.t_silver_merge
AS
MERGE INTO cdc.gold_customer_daily tgt
USING (
    SELECT  customer_id,
            order_ts::DATE                  AS order_date,
            COUNT(*)                        AS order_count,
            SUM(total_amount)               AS revenue,
            COUNT_IF(is_high_value)         AS high_value_cnt
    FROM    cdc.silver_orders
    WHERE   NOT is_deleted
      AND   order_ts >= DATEADD(day, -7, CURRENT_DATE())
    GROUP   BY 1, 2
) src
ON  tgt.customer_id = src.customer_id
AND tgt.order_date  = src.order_date
WHEN MATCHED THEN UPDATE SET
    order_count    = src.order_count,
    revenue        = src.revenue,
    high_value_cnt = src.high_value_cnt,
    updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, order_date, order_count, revenue, high_value_cnt, updated_at
) VALUES (
    src.customer_id, src.order_date, src.order_count, src.revenue,
    src.high_value_cnt, CURRENT_TIMESTAMP()
);

-- Optional alerting task running on a serverless warehouse
CREATE OR REPLACE TASK cdc.t_alert_late_orders
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = 'USING CRON 0 9 * * * UTC'
AS
INSERT INTO ops.alerts (alert_ts, level, message)
SELECT  CURRENT_TIMESTAMP(),
        'WARN',
        'Late orders: ' || COUNT(*) || ' rows past SLA'
FROM    cdc.silver_orders
WHERE   order_status = 'PENDING'
  AND   DATEDIFF(hour, order_ts, CURRENT_TIMESTAMP()) > 24
HAVING  COUNT(*) > 0;

-- Resume the DAG
ALTER TASK cdc.t_alert_late_orders RESUME;
ALTER TASK cdc.t_gold_agg          RESUME;
ALTER TASK cdc.t_silver_merge      RESUME;

-- Health checks
SELECT SYSTEM$STREAM_HAS_DATA('cdc.bronze_orders_stream') AS has_pending;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(hour, -24, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;
