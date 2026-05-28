-- internal error
-- Multiple errors: 
-- ============================================================
-- MEDIUM: PIVOT / UNPIVOT, QUALIFY, MATCH_RECOGNIZE,
-- approximate aggregates, sampling, sequences.
-- ============================================================

USE DATABASE demo_db;
USE SCHEMA   sales;

-- Sequence for surrogate keys
CREATE OR REPLACE SEQUENCE sales.order_seq START = 1 INCREMENT = 1;

-- Daily channel revenue, pivoted into wide columns
SELECT *
FROM (
    SELECT  order_ts::DATE                                AS order_date,
            channel,
            total_amount
    FROM    sales.orders
    WHERE   order_ts >= DATEADD(day, -30, CURRENT_DATE())
)
PIVOT (
    SUM(total_amount) FOR channel IN ('WEB','MOBILE','RETAIL','PARTNER','CALLCENTER')
)
ORDER BY order_date;

-- UNPIVOT: flatten wide marketing metrics
CREATE OR REPLACE TABLE marketing.daily_channel_metrics (
    day             DATE,
    web_spend       NUMBER(12,2),
    mobile_spend    NUMBER(12,2),
    retail_spend    NUMBER(12,2),
    partner_spend   NUMBER(12,2)
);

SELECT day, channel, spend
FROM   marketing.daily_channel_metrics
       UNPIVOT (spend FOR channel IN (web_spend, mobile_spend, retail_spend, partner_spend))
WHERE  day >= DATEADD(day, -7, CURRENT_DATE());

-- QUALIFY: top-3 most recent orders per customer
SELECT  o.customer_id,
        o.order_id,
        o.order_ts,
        o.total_amount,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id
                           ORDER BY o.order_ts DESC) AS recency_rank
FROM    sales.orders o
WHERE   o.order_ts >= DATEADD(year, -1, CURRENT_DATE())
QUALIFY recency_rank <= 3;

-- Approximate distinct + percentiles
SELECT  channel,
        HLL(customer_id)                                         AS approx_customers,
        APPROX_PERCENTILE(total_amount, 0.5)                     AS approx_median,
        APPROX_PERCENTILE(total_amount, 0.95)                    AS approx_p95
FROM    sales.orders
GROUP   BY channel;

-- TABLESAMPLE for cheap exploration
SELECT  customer_id, total_amount
FROM    sales.orders SAMPLE (5)        -- 5% sample
WHERE   order_ts >= DATEADD(month, -3, CURRENT_DATE());

-- Detect customers who went FREE -> TRIAL -> PAID using LAG window
-- functions (portable alternative to MATCH_RECOGNIZE)
WITH lagged AS (
    SELECT  customer_id,
            event_ts,
            new_state,
            LAG(new_state, 2) OVER (PARTITION BY customer_id ORDER BY event_ts) AS state_2_back,
            LAG(new_state, 1) OVER (PARTITION BY customer_id ORDER BY event_ts) AS state_1_back,
            LAG(event_ts,   2) OVER (PARTITION BY customer_id ORDER BY event_ts) AS ts_2_back,
            LAG(event_ts,   1) OVER (PARTITION BY customer_id ORDER BY event_ts) AS ts_1_back
    FROM    events.user_state_changes
)
SELECT  customer_id,
        ts_2_back AS free_ts,
        ts_1_back AS trial_ts,
        event_ts  AS paid_ts
FROM    lagged
WHERE   state_2_back = 'FREE'
  AND   state_1_back = 'TRIAL'
  AND   new_state    = 'PAID'
  AND   DATEDIFF('day', ts_2_back, ts_1_back) <= 30
  AND   DATEDIFF('day', ts_1_back, event_ts)  <= 14;

-- GENERATOR + SEQ8: synthesize a date dimension
CREATE OR REPLACE TABLE dim.date_dim AS
SELECT  DATEADD(day, SEQ4(), '2000-01-01'::DATE)             AS d_date,
        YEAR(DATEADD(day, SEQ4(), '2000-01-01'::DATE))       AS d_year,
        QUARTER(DATEADD(day, SEQ4(), '2000-01-01'::DATE))    AS d_quarter,
        MONTH(DATEADD(day, SEQ4(), '2000-01-01'::DATE))      AS d_month,
        DAYOFWEEK(DATEADD(day, SEQ4(), '2000-01-01'::DATE))  AS d_dow
FROM    TABLE(GENERATOR(ROWCOUNT => 365 * 60));
