-- FIXME databricks.migration.unsupported.feature 'BB_CLOSE_CURSOR'
-- VERY COMPLEX: Stored procedure with cursors, dynamic SQL,
-- temp tables, MERGE, error handling, transaction control,
-- HyperLogLog sketches, geo/ML functions, data sharing,
-- ROLLUP/CUBE/GROUPING SETS, advanced SUPER navigation.
-- ============================================================

-- Stored procedure: incremental upsert of customer 360 with
-- dynamic SQL, cursor, exception handling, logging.
CREATE OR REPLACE PROCEDURE analytics.sp_refresh_customer_360(
IN in_run_date DATE,
IN in_batch_size INTEGER DEFAULT 50000,
OUT out_rows_merged BIGINT,
OUT out_status STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN


     SET v_rowcount = 0;
     SET v_total = 0;
     SET v_start = CURRENT_TIMESTAMP;

    
DECLARE VARIABLE v_sql STRING;
DECLARE VARIABLE v_partition STRING;
DECLARE VARIABLE v_lower         TIMESTAMP;
DECLARE VARIABLE v_upper         TIMESTAMP;
DECLARE VARIABLE v_part_cur      REFCURSOR;
DECLARE VARIABLE v_rowcount      BIGINT ;

DECLARE VARIABLE v_total         BIGINT ;

DECLARE VARIABLE v_start         TIMESTAMP ;
-- Audit start
    INSERT INTO meta.proc_audit (proc_name, run_date, started_at, status)
    VALUES ('sp_refresh_customer_360', in_run_date, v_start, 'STARTED');

    SET v_lower = in_run_date::TIMESTAMP;
    SET v_upper = DATEADD(day, 1, in_run_date)::TIMESTAMP;

    -- Stage table (temp, session-scoped)
    DROP TABLE IF EXISTS tmp_c360_stage;
    CREATE TEMP TABLE tmp_c360_stage (
        customer_id     BIGINT,
        snapshot_date   DATE,
        total_spend_30d DECIMAL(18,2),
        order_count_30d INTEGER,
        last_order_ts   TIMESTAMP,
        top_category STRING,
        churn_score     DECIMAL(5,4),
        traits          SUPER,
        unique_sessions BIGINT,
        hll_sessions    HLLSKETCH
    )
    
    ZORDER BY(snapshot_date);

    -- Cursor over partitions (one per country) using dynamic SQL
-- FIXME databricks.migration.unsupported.feature 'BB_CLOSE_CURSOR'
 OPEN v_part_cur FOR
        SELECT DISTINCT country FROM sales.customers WHERE is_active; 


    LOOP
-- FIXME databricks.migration.unsupported.feature 'BB_CLOSE_CURSOR'
 FETCH v_part_cur INTO v_partition; 

        EXIT WHEN NOT FOUND;

        SET v_sql = 'INSERT INTO tmp_c360_stage '
              || 'WITH win AS ( '
              || '  SELECT o.customer_id, '
              || '         SUM(o.total_amount)                              AS total_spend_30d, '
              || '         COUNT(*)                                          AS order_count_30d, '
              || '         MAX(o.order_ts)                                   AS last_order_ts, '
              || '         HLL_CREATE_SKETCH(o.order_id::STRING)             AS hll_sessions '
              || '  FROM   sales.orders o '
              || '  JOIN   sales.customers c USING (customer_id) '
              || '  WHERE  c.country = $1 '
              || '    AND  o.order_ts >= DATEADD(day, -30, $2) '
              || '    AND  o.order_ts <  $3 '
              || '  GROUP  BY o.customer_id), '
              || 'top_cat AS ( '
              || '  SELECT customer_id, category, qty, '
              || '         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY qty DESC) rn '
              || '  FROM ( '
              || '    SELECT o.customer_id, p.category, SUM(oi.quantity) qty '
              || '    FROM   sales.orders      o '
              || '    JOIN   sales.order_items oi USING (order_id) '
              || '    JOIN   catalog.products  p  USING (sku) '
              || '    JOIN   sales.customers   c  USING (customer_id) '
              || '    WHERE  c.country = $1 '
              || '      AND  o.order_ts >= DATEADD(day, -30, $2) '
              || '    GROUP  BY 1,2)) '
              || 'SELECT w.customer_id, $2::DATE, '
              || '       w.total_spend_30d, w.order_count_30d, w.last_order_ts, '
              || '       tc.category, '
              || '       ml.predict_churn(w.total_spend_30d, w.order_count_30d, '
              || '                        DATEDIFF(DAY, w.last_order_ts, $2)), '
              || '       FROM_JSON("{`country`:`" || country || "`, `tier`:`gold`}"), '
              || '       HLL_CARDINALITY(w.hll_sessions), '
              || '       w.hll_sessions '
              || 'FROM   win w '
              || 'LEFT JOIN top_cat tc ON tc.customer_id = w.customer_id AND tc.rn = 1';

        EXECUTE v_sql USING v_partition, v_upper, v_upper;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        SET v_total = v_total + v_rowcount;

        RAISE INFO 'Partition % -> % rows', v_partition, v_rowcount;
END_LOOP;

-- FIXME databricks.migration.unsupported.feature 'BB_CLOSE_CURSOR'
 CLOSE v_part_cur; 

-- MERGE into target (Redshift MERGE syntax)
MERGE INTO analytics.customer_360 tgt
USING tmp_c360_stage             src
ON   tgt.customer_id   = src.customer_id
AND  tgt.snapshot_date = src.snapshot_date
WHEN MATCHED THEN UPDATE SET
total_spend_30d = src.total_spend_30d,
order_count_30d = src.order_count_30d,
last_order_ts   = src.last_order_ts,
top_category    = src.top_category,
churn_score     = src.churn_score,
traits          = src.traits,
unique_sessions = src.unique_sessions,
hll_sessions    = src.hll_sessions,
updated_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
customer_id, snapshot_date, total_spend_30d, order_count_30d,
last_order_ts, top_category, churn_score, traits,
unique_sessions, hll_sessions, created_at
) VALUES (
src.customer_id, src.snapshot_date, src.total_spend_30d, src.order_count_30d,
src.last_order_ts, src.top_category, src.churn_score, src.traits,
src.unique_sessions, src.hll_sessions, CURRENT_TIMESTAMP
);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    -- Vacuum + analyze (best practice on large merges)
    EXECUTE 'ANALYZE analytics.customer_360';

    SET out_rows_merged = v_total;
    SET out_status      = 'OK';

    UPDATE meta.proc_audit
    SET    finished_at = CURRENT_TIMESTAMP,
           status      = 'SUCCESS',
           rows_processed = v_total
    WHERE  proc_name = 'sp_refresh_customer_360'
      AND  started_at = v_start;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        SET out_status = 'ERROR: ' || SQLERRM;
        INSERT INTO meta.proc_errors (proc_name, run_date, error_msg, errored_at)
        VALUES ('sp_refresh_customer_360', in_run_date, SQLERRM, CURRENT_TIMESTAMP);
        RAISE;
END;
$$;

-- Cross-database / data-sharing query
GRANT USAGE ON DATABASE shared_marketing TO ROLE analytics_ro;

SELECT  c.customer_id,
        c.email,
        m.campaign_id,
        m.last_touch_ts,
        m.attribution_pct
FROM    analytics.customer_360                              c
JOIN    shared_marketing.public.campaign_attribution        m
  USING (customer_id)
WHERE   c.snapshot_date = DATE_ADD(current_date, -1)
  AND   m.last_touch_ts >= DATEADD(day, -7, CURRENT_TIMESTAMP);

-- HLL union across partitions
SELECT  snapshot_date,
        HLL_CARDINALITY(HLL_COMBINE(hll_sessions))     AS distinct_sessions_total
FROM    analytics.customer_360
WHERE   snapshot_date BETWEEN DATEADD(day, -30, CURRENT_DATE) AND CURRENT_DATE
GROUP   BY 1
ORDER   BY 1;
-- Advanced grouping: ROLLUP / CUBE / GROUPING SETS
SELECT
    COALESCE(c.country, '∑') AS country,
    COALESCE(p.category, '∑') AS category,
    DATE_TRUNC('quarter', o.order_ts)::DATE AS quarter_start,
    GROUPING(c.country, p.category) AS grp_id,
    SUM(oi.quantity * oi.unit_price) AS gross_revenue,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    SUM(oi.quantity * oi.unit_price) /
            NULLIF(COUNT(DISTINCT o.order_id), 0) AS avg_order_value,
    RATIO_TO_REPORT(SUM(oi.quantity * oi.unit_price))
            OVER (PARTITION BY DATE_TRUNC('quarter', o.order_ts)) AS pct_of_quarter
FROM sales.orders        o
JOIN    sales.order_items   oi USING (order_id)
JOIN    sales.customers     c  USING (customer_id)
JOIN    catalog.products    p  USING (sku)
WHERE o.order_ts >= DATEADD(year, -1, CURRENT_TIMESTAMP)
GROUP BY GROUPING SETS (
            (c.country, p.category, DATE_TRUNC('quarter', o.order_ts)),
            (c.country,             DATE_TRUNC('quarter', o.order_ts)),
            (            p.category, DATE_TRUNC('quarter', o.order_ts)),
            (                        DATE_TRUNC('quarter', o.order_ts)),
            ()
          )
ORDER BY quarter_start, country, category;

-- Geo + ML inference via Redshift ML
CREATE MODEL ml.churn_v3
FROM (
    SELECT  c360.total_spend_30d,
            c360.order_count_30d,
            DATEDIFF(DAY, c360.last_order_ts, c360.snapshot_date) AS recency,
            c.country,
            c.signup_date,
            c360.churn_score AS target
    FROM    analytics.customer_360 c360
    JOIN    sales.customers        c USING (customer_id)
    WHERE   c360.snapshot_date BETWEEN '2025-01-01' AND '2025-12-31'
)
TARGET target
FUNCTION ml.f_predict_churn
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftMLRole'
SETTINGS (
    S3_BUCKET 'rs-ml-artifacts',
    MAX_RUNTIME 5400,
    PROBLEM_TYPE REGRESSION,
    OBJECTIVE 'MSE'
);

-- Deeply nested SUPER navigation + PartiQL UNNEST
SELECT  e.user_id,
        e.event_ts,
        item.product_id:: STRING                    AS product_id,
        item.quantity::INTEGER                          AS quantity,
        item.price.value::DECIMAL(12,2)                 AS price_value,
        item.price.currency::CHAR(3)                    AS price_ccy,
        e.payload.`context`.`campaign`.`id`::STRING    AS campaign_id
FROM    events.user_events e,
        e.payload.`cart`.`items` AS item AT idx
WHERE   e.event_type = 'CART_VIEWED'
  AND   e.event_ts >= DATEADD(day, -7, CURRENT_TIMESTAMP)
  AND   item.price.value::DECIMAL(12,2) > 50.00
ORDER   BY e.event_ts DESC
LIMIT 5000;

-- Driver to invoke the stored proc with named-arg call
CALL analytics.sp_refresh_customer_360(
    in_run_date   => DATE_ADD(current_date, -1),
    in_batch_size => 100000
);
