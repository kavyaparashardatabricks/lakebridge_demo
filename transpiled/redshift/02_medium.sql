-- ============================================================
-- MEDIUM: Joins, CTEs, window functions, Redshift-specific funcs
-- ============================================================

CREATE OR REPLACE TABLE sales.orders (
    order_id        BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    customer_id     INTEGER NOT NULL,
    order_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status STRING ,
    total_amount    DECIMAL(12,2),
    currency        CHAR(3) DEFAULT 'USD'
)

ZORDER BY(order_ts);

CREATE OR REPLACE TABLE sales.order_items (
    order_id        BIGINT NOT NULL,
    line_no         SMALLINT NOT NULL,
    sku STRING ,
    quantity        INTEGER,
    unit_price      DECIMAL(10,2),
    discount_pct    DECIMAL(5,2) DEFAULT 0.00
)

ZORDER BY(order_id);
-- Customer lifetime value with running totals
WITH order_facts AS (
SELECT
    o.customer_id,
    o.order_id,
    o.order_ts::DATE AS order_date,
    SUM(oi.quantity * oi.unit_price *
                (1 - oi.discount_pct/100.0)) AS order_total,
    COUNT(DISTINCT oi.sku) AS distinct_skus
FROM sales.orders        o
    JOIN    sales.order_items   oi USING (order_id)
WHERE o.order_status NOT IN ('CANCELLED', 'FAILED')
      AND   o.order_ts >= DATEADD(year, -2, CURRENT_TIMESTAMP)
GROUP BY 1, 2, 3
),
ranked AS (
SELECT
    customer_id,
    order_id,
    order_date,
    order_total,
    distinct_skus,
    ROW_NUMBER() OVER (PARTITION BY customer_id
                               ORDER BY order_date) AS order_seq,
    SUM(order_total) OVER (PARTITION BY customer_id
                                   ORDER BY order_date
                                   ROWS BETWEEN UNBOUNDED PRECEDING
                                            AND CURRENT ROW) AS running_ltv,
    LAG(order_date) OVER (PARTITION BY customer_id
                                  ORDER BY order_date) AS prev_order_date
FROM order_facts
)
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    r.order_seq,
    r.order_date,
    r.order_total,
    r.running_ltv,
    DATEDIFF(DAY, r.prev_order_date, r.order_date) AS days_since_prev_order,
    COALESCE(r.distinct_skus, 0) AS distinct_skus,
    TRUNC('DD', MONTHS_BETWEEN(CURRENT_TIMESTAMP, c.signup_date)) AS tenure_months
FROM ranked r
JOIN    sales.customers c
  ON    c.customer_id = r.customer_id
WHERE r.order_seq <= 50
ORDER BY c.customer_id, r.order_seq;
-- Pivot-style report using CASE + Redshift date_trunc
SELECT
    DATE_TRUNC('month', order_ts)::DATE AS month_start,
    SUM(CASE WHEN order_status = 'COMPLETED'
                 THEN total_amount ELSE 0 END) AS revenue_completed,
    SUM(CASE WHEN order_status = 'REFUNDED'
                 THEN total_amount ELSE 0 END) AS revenue_refunded,
    COUNT(DISTINCT customer_id) AS active_customers
FROM sales.orders
WHERE order_ts >= ADD_MONTHS(DATE_TRUNC('month', CURRENT_TIMESTAMP), -12)
GROUP BY 1
ORDER BY 1;
-- LISTAGG: top SKUs per customer
SELECT
    customer_id,
    LISTAGG(sku, ', ')
            WITHIN GROUP (ORDER BY total_qty DESC) AS top_skus
FROM (
    SELECT  o.customer_id,
            oi.sku,
            SUM(oi.quantity) AS total_qty,
            ROW_NUMBER() OVER (PARTITION BY o.customer_id
                               ORDER BY SUM(oi.quantity) DESC) AS rn
    FROM    sales.orders      o
    JOIN    sales.order_items oi USING (order_id)
    GROUP   BY 1, 2
) t
WHERE rn <= 5
GROUP BY customer_id;
