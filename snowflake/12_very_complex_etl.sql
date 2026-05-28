-- ============================================================
-- VERY COMPLEX: End-to-end Snowflake-Scripting ETL pipeline
-- combining streams + tasks + dynamic SQL + multi-table MERGE
-- + cursors + RESULTSET + recursive CTE + window functions +
-- semi-structured + error capture.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.dwh;
USE SCHEMA dwh;

-- ----- target schema -------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_sk     NUMBER AUTOINCREMENT,
    customer_id     NUMBER PRIMARY KEY,
    full_name       STRING,
    email_hash      STRING,                    -- never raw email
    country         CHAR(2),
    segment         STRING,
    attrs           VARIANT,
    valid_from      TIMESTAMP_NTZ,
    valid_to        TIMESTAMP_NTZ,
    is_current      BOOLEAN
);

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_sk        NUMBER AUTOINCREMENT,
    order_id        NUMBER,
    customer_sk     NUMBER,
    order_date      DATE,
    order_status    STRING,
    gross_amount    NUMBER(14,2),
    tax_amount      NUMBER(14,2),
    net_amount      NUMBER(14,2),
    line_count      NUMBER,
    payload         VARIANT,
    loaded_at       TIMESTAMP_NTZ
)
CLUSTER BY (order_date);

CREATE TABLE IF NOT EXISTS dwh.bridge_order_category (
    order_sk        NUMBER,
    category        STRING,
    revenue_share   NUMBER(14,4)
);

CREATE TABLE IF NOT EXISTS dwh.etl_audit (
    run_id          STRING,
    proc_name       STRING,
    started_at      TIMESTAMP_NTZ,
    finished_at     TIMESTAMP_NTZ,
    status          STRING,
    rows_processed  NUMBER,
    rows_inserted   NUMBER,
    rows_updated    NUMBER,
    rows_rejected   NUMBER,
    payload         VARIANT
);

-- Stream over bronze (already created in 07_…)
CREATE STREAM IF NOT EXISTS dwh.bronze_orders_stream ON TABLE cdc.bronze_orders;

-- ----- the procedure ------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.sp_refresh_dwh(
    p_run_date      DATE,
    p_batch_size    NUMBER     DEFAULT 100000,
    p_force_full    BOOLEAN    DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id        STRING        DEFAULT UUID_STRING();
    v_started_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_finished_at   TIMESTAMP_NTZ;
    v_status        STRING        DEFAULT 'OK';
    v_rows_proc     NUMBER        DEFAULT 0;
    v_rows_ins      NUMBER        DEFAULT 0;
    v_rows_upd      NUMBER        DEFAULT 0;
    v_rows_rej      NUMBER        DEFAULT 0;
    v_payload       VARIANT       DEFAULT NULL;
    v_sql           STRING;
    v_partition     STRING;
    res             RESULTSET;
    err_msg         STRING;
BEGIN
    -- 1. Audit start
    INSERT INTO dwh.etl_audit (run_id, proc_name, started_at, status)
    VALUES (:v_run_id, 'sp_refresh_dwh', :v_started_at, 'STARTED');

    -- 2. SCD-2 merge for dim_customer using window-based change detection
    LET dim_sql STRING :=
        'MERGE INTO dwh.dim_customer tgt
         USING (
            WITH changed AS (
              SELECT  c.customer_id,
                      INITCAP(c.first_name) || '' '' || INITCAP(c.last_name) AS full_name,
                      SHA2(LOWER(c.email), 256)                              AS email_hash,
                      c.country,
                      IFF(c.attributes:"vip"::BOOLEAN, ''VIP'', ''STANDARD'') AS segment,
                      c.attributes                                            AS attrs
              FROM    demo_db.sales.customers c
              WHERE   ' || IFF(:p_force_full, '1=1',
                              'c.signup_ts >= DATEADD(day, -1, :2)::TIMESTAMP_NTZ
                                OR c.customer_id IN (SELECT customer_id
                                                     FROM dwh.bronze_orders_stream)') || '
            )
            SELECT  cur.customer_id,
                    cur.full_name, cur.email_hash, cur.country,
                    cur.segment, cur.attrs,
                    prev.email_hash AS prev_email_hash,
                    prev.country    AS prev_country,
                    prev.segment    AS prev_segment
            FROM    changed cur
            LEFT JOIN dwh.dim_customer prev
                   ON prev.customer_id = cur.customer_id
                  AND prev.is_current
         ) src
         ON  tgt.customer_id = src.customer_id AND tgt.is_current
         WHEN MATCHED AND (
                tgt.email_hash <> src.email_hash
             OR tgt.country    <> src.country
             OR tgt.segment    <> src.segment
         ) THEN UPDATE SET
                valid_to   = :1,
                is_current = FALSE
         WHEN NOT MATCHED THEN INSERT (
                customer_id, full_name, email_hash, country, segment,
                attrs, valid_from, valid_to, is_current
         ) VALUES (
                src.customer_id, src.full_name, src.email_hash, src.country,
                src.segment, src.attrs, :1, NULL, TRUE
         )';

    EXECUTE IMMEDIATE :dim_sql USING (v_started_at, p_run_date);
    v_rows_upd := SQLROWCOUNT;

    -- Insert the *new* current rows that the previous step expired
    INSERT INTO dwh.dim_customer (
        customer_id, full_name, email_hash, country, segment,
        attrs, valid_from, valid_to, is_current
    )
    SELECT  c.customer_id,
            INITCAP(c.first_name) || ' ' || INITCAP(c.last_name),
            SHA2(LOWER(c.email), 256),
            c.country,
            IFF(c.attributes:"vip"::BOOLEAN, 'VIP', 'STANDARD'),
            c.attributes,
            :v_started_at, NULL, TRUE
    FROM    demo_db.sales.customers c
    JOIN    dwh.dim_customer        d
      ON    d.customer_id = c.customer_id
     AND    d.valid_to    = :v_started_at;

    v_rows_ins := v_rows_ins + SQLROWCOUNT;

    -- 3. Fact loading from the stream
    MERGE INTO dwh.fact_orders tgt
    USING (
        WITH stream_data AS (
            SELECT  s.order_id,
                    s.customer_id,
                    s.order_ts::DATE                          AS order_date,
                    s.order_status,
                    s.total_amount                            AS gross_amount,
                    s.total_amount * 0.08                     AS tax_amount,
                    s.total_amount * 0.92                     AS net_amount,
                    METADATA$ACTION                           AS action
            FROM    dwh.bronze_orders_stream s
        ),
        items AS (
            SELECT  oi.order_id,
                    COUNT(*)                                   AS line_count,
                    ARRAY_AGG(OBJECT_CONSTRUCT(
                        'sku', oi.sku, 'qty', oi.quantity, 'price', oi.unit_price
                    ))                                          AS payload
            FROM    demo_db.sales.order_items oi
            GROUP   BY oi.order_id
        )
        SELECT  sd.*,
                d.customer_sk,
                NVL(i.line_count, 0)                          AS line_count,
                i.payload
        FROM    stream_data sd
        JOIN    dwh.dim_customer d
          ON    d.customer_id = sd.customer_id
         AND    d.is_current
        LEFT JOIN items i ON i.order_id = sd.order_id
    ) src
    ON tgt.order_id = src.order_id
    WHEN MATCHED AND src.action = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET
        customer_sk  = src.customer_sk,
        order_date   = src.order_date,
        order_status = src.order_status,
        gross_amount = src.gross_amount,
        tax_amount   = src.tax_amount,
        net_amount   = src.net_amount,
        line_count   = src.line_count,
        payload      = src.payload,
        loaded_at    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.action = 'INSERT' THEN INSERT (
        order_id, customer_sk, order_date, order_status,
        gross_amount, tax_amount, net_amount, line_count, payload, loaded_at
    ) VALUES (
        src.order_id, src.customer_sk, src.order_date, src.order_status,
        src.gross_amount, src.tax_amount, src.net_amount,
        src.line_count, src.payload, CURRENT_TIMESTAMP()
    );

    v_rows_proc := SQLROWCOUNT;

    -- 4. Bridge table rebuild for category attribution
    DELETE FROM dwh.bridge_order_category
    WHERE  order_sk IN (
        SELECT order_sk FROM dwh.fact_orders WHERE order_date = :p_run_date
    );

    INSERT INTO dwh.bridge_order_category
    SELECT  fo.order_sk,
            p.category,
            DIV0(SUM(oi.quantity * oi.unit_price),
                 fo.gross_amount)
    FROM    dwh.fact_orders          fo
    JOIN    demo_db.sales.order_items oi USING (order_id)
    JOIN    catalog.products         p  USING (sku)
    WHERE   fo.order_date = :p_run_date
    GROUP   BY fo.order_sk, p.category, fo.gross_amount;

    -- 5. Rejected-row capture
    INSERT INTO dwh.etl_rejected (run_id, reason, raw)
    SELECT  :v_run_id,
            'ORPHAN_CUSTOMER',
            OBJECT_CONSTRUCT('order_id', s.order_id, 'customer_id', s.customer_id)
    FROM    dwh.bronze_orders_stream s
    LEFT JOIN dwh.dim_customer d
           ON d.customer_id = s.customer_id AND d.is_current
    WHERE   d.customer_sk IS NULL
      AND   s.METADATA$ACTION = 'INSERT';

    v_rows_rej := SQLROWCOUNT;

    -- 6. Recursive CTE: walk the SCD2 chain for VIP customers (audit
    --    table populated from a recursive CTE)
    CREATE OR REPLACE TEMP TABLE tmp_vip_history AS
    WITH RECURSIVE chain AS (
        SELECT  customer_id, customer_sk, segment, valid_from, valid_to,
                1                              AS depth
        FROM    dwh.dim_customer
        WHERE   is_current AND segment = 'VIP'
        UNION ALL
        SELECT  d.customer_id, d.customer_sk, d.segment, d.valid_from, d.valid_to,
                c.depth + 1
        FROM    dwh.dim_customer d
        JOIN    chain            c
          ON    d.customer_id = c.customer_id
         AND    d.valid_to    = c.valid_from
        WHERE   c.depth < 20
    )
    SELECT * FROM chain;

    -- 7. Finalize audit row
    v_finished_at := CURRENT_TIMESTAMP();
    v_payload     := OBJECT_CONSTRUCT(
        'run_id',         v_run_id,
        'rows_processed', v_rows_proc,
        'rows_inserted',  v_rows_ins,
        'rows_updated',   v_rows_upd,
        'rows_rejected',  v_rows_rej,
        'force_full',     p_force_full
    );

    UPDATE dwh.etl_audit
    SET    finished_at    = :v_finished_at,
           status         = :v_status,
           rows_processed = :v_rows_proc,
           rows_inserted  = :v_rows_ins,
           rows_updated   = :v_rows_upd,
           rows_rejected  = :v_rows_rej,
           payload        = :v_payload
    WHERE  run_id = :v_run_id;

    RETURN v_payload;

EXCEPTION
    WHEN OTHER THEN
        err_msg := SQLERRM;
        UPDATE dwh.etl_audit
        SET    finished_at = CURRENT_TIMESTAMP(),
               status      = 'ERROR',
               payload     = OBJECT_CONSTRUCT('error', :err_msg,
                                              'code',  SQLCODE,
                                              'state', SQLSTATE)
        WHERE  run_id = :v_run_id;
        RAISE;
END;
$$;

-- Orchestrator task DAG
CREATE OR REPLACE TASK dwh.t_refresh_dwh
    WAREHOUSE = analytics_wh
    SCHEDULE  = 'USING CRON 30 * * * * UTC'
AS
    CALL dwh.sp_refresh_dwh(CURRENT_DATE(), 100000, FALSE);

ALTER TASK dwh.t_refresh_dwh RESUME;

-- One-time backfill driver
CALL dwh.sp_refresh_dwh(
    p_run_date   => DATEADD(day, -7, CURRENT_DATE())::DATE,
    p_batch_size => 200000,
    p_force_full => TRUE
);
