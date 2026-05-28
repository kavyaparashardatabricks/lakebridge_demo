-- ============================================================
-- MEDIUM: Multi-table joins, CTEs, window functions, common
-- Snowflake-specific scalar funcs (NVL, NVL2, ZEROIFNULL,
-- NULLIFZERO, DIV0, BOOLAND_AGG, etc.).
-- ============================================================

USE DATABASE demo_db;
USE SCHEMA   sales;

CREATE OR REPLACE TABLE sales.orders (
    order_id        NUMBER(18,0) AUTOINCREMENT,
    customer_id     NUMBER(18,0) NOT NULL,
    order_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    order_status    STRING,
    total_amount    NUMBER(12,2),
    currency        STRING DEFAULT 'USD',
    channel         STRING
)
CLUSTER BY (order_ts);

CREATE OR REPLACE TABLE sales.order_items (
    order_id        NUMBER(18,0) NOT NULL,
    line_no         NUMBER(5,0)  NOT NULL,
    sku             STRING,
    quantity        NUMBER(8,0),
    unit_price      NUMBER(10,2),
    discount_pct    NUMBER(5,2) DEFAULT 0
);

CREATE OR REPLACE TABLE catalog.products (
    sku             STRING,
    name            STRING,
    category        STRING,
    list_price      NUMBER(10,2),
    launched_on     DATE
);

-- Customer LTV with running totals and gap analysis
WITH order_facts AS (
    SELECT  o.customer_id,
            o.order_id,
            o.order_ts::DATE                                 AS order_date,
            o.channel,
            SUM(oi.quantity * oi.unit_price *
                (1 - oi.discount_pct/100.0))                 AS net_amount,
            SUM(oi.quantity)                                 AS total_qty
    FROM    orders        o
    JOIN    order_items   oi USING (order_id)
    WHERE   o.order_status NOT IN ('CANCELLED','FAILED','RETURNED')
      AND   o.order_ts >= DATEADD(year, -2, CURRENT_DATE())
    GROUP   BY 1,2,3,4
),
ranked AS (
    SELECT  *,
            ROW_NUMBER()  OVER (PARTITION BY customer_id ORDER BY order_date)  AS order_seq,
            SUM(net_amount) OVER (PARTITION BY customer_id
                                  ORDER BY order_date
                                  ROWS BETWEEN UNBOUNDED PRECEDING
                                           AND CURRENT ROW)                    AS running_ltv,
            LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
            FIRST_VALUE(channel) OVER (PARTITION BY customer_id
                                       ORDER BY order_date)                    AS acquisition_channel
    FROM    order_facts
)
SELECT  c.customer_id,
        c.first_name || ' ' || c.last_name                          AS customer_name,
        r.order_seq,
        r.order_date,
        r.net_amount,
        r.running_ltv,
        DATEDIFF(day, r.prev_order_date, r.order_date)              AS days_since_prev,
        DIV0(r.net_amount, NULLIFZERO(r.total_qty))                 AS avg_unit_price,
        ZEROIFNULL(r.total_qty)                                     AS qty_safe,
        r.acquisition_channel
FROM    ranked r
JOIN    customers c ON c.customer_id = r.customer_id
WHERE   r.order_seq <= 100
ORDER   BY c.customer_id, r.order_seq;

-- Monthly revenue funnel with CASE
SELECT  DATE_TRUNC('month', order_ts)::DATE                          AS month_start,
        SUM(IFF(order_status = 'COMPLETED', total_amount, 0))        AS rev_completed,
        SUM(IFF(order_status = 'REFUNDED',  total_amount, 0))        AS rev_refunded,
        COUNT_IF(order_status = 'COMPLETED')                         AS orders_completed,
        COUNT(DISTINCT customer_id)                                  AS active_customers,
        DIV0(SUM(IFF(order_status = 'COMPLETED', total_amount, 0)),
             COUNT_IF(order_status = 'COMPLETED'))                   AS avg_order_value
FROM    orders
WHERE   order_ts >= DATEADD(month, -12, DATE_TRUNC('month', CURRENT_DATE()))
GROUP   BY 1
ORDER   BY 1;

-- LISTAGG + QUALIFY for top-N per customer
SELECT  customer_id,
        LISTAGG(sku, ', ') WITHIN GROUP (ORDER BY total_qty DESC) AS top_skus
FROM (
    SELECT  o.customer_id,
            oi.sku,
            SUM(oi.quantity) AS total_qty
    FROM    orders        o
    JOIN    order_items   oi USING (order_id)
    GROUP   BY 1, 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.customer_id
                               ORDER BY SUM(oi.quantity) DESC) <= 5
)
GROUP BY customer_id;

-- Anti-join: customers with no orders in 90 days
SELECT  c.customer_id,
        c.first_name,
        c.last_name,
        c.country
FROM    customers c
WHERE   NOT EXISTS (
    SELECT 1 FROM orders o
    WHERE  o.customer_id = c.customer_id
      AND  o.order_ts >= DATEADD(day, -90, CURRENT_TIMESTAMP())
);
