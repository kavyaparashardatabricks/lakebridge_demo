USE CATALOG demo_db;

USE SCHEMA sales;

CREATE OR REPLACE
    TABLE sales.orders
    (
        order_id DECIMAL(18, 0) GENERATED ALWAYS AS IDENTITY,
        customer_id DECIMAL(18, 0) NOT NULL,
        order_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        order_status VARCHAR(16777216),
        total_amount DECIMAL(12, 2),
        currency VARCHAR(16777216) DEFAULT 'USD',
        channel VARCHAR(16777216)
    ) TBLPROPERTIES( 'delta.feature.allowColumnDefaults' = 'supported' )
    CLUSTER BY order_ts;

CREATE OR REPLACE
    TABLE sales.order_items
    (
        order_id DECIMAL(18, 0) NOT NULL,
        line_no DECIMAL(5, 0) NOT NULL,
        sku VARCHAR(16777216),
        quantity DECIMAL(8, 0),
        unit_price DECIMAL(10, 2),
        discount_pct DECIMAL(5, 2) DEFAULT 0
    ) TBLPROPERTIES( 'delta.feature.allowColumnDefaults' = 'supported' );

CREATE OR REPLACE
    TABLE catalog.products
    (
        sku VARCHAR(16777216),
        name VARCHAR(16777216),
        category VARCHAR(16777216),
        list_price DECIMAL(10, 2),
        launched_on DATE
    );

WITH
    order_facts AS
    (
        SELECT
            o.customer_id,
            o.order_id,
            CAST(o.order_ts AS DATE) AS order_date,
            o.channel,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100.0)) AS net_amount,
            SUM(oi.quantity) AS total_qty
        FROM
            orders AS o JOIN order_items AS oi USING (order_id)
            WHERE
                NOT o.order_status IN ('CANCELLED', 'FAILED', 'RETURNED') AND
                o.order_ts >= DATE_ADD(year, -2, CURRENT_DATE())
            GROUP BY 1, 2, 3, 4
    ),
    ranked AS
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date NULLS LAST) AS order_seq,
            SUM(net_amount) OVER
            (
                PARTITION BY customer_id ORDER BY order_date NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_ltv,
            LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date NULLS LAST) AS prev_order_date,
            FIRST(channel) OVER
            (
                PARTITION BY customer_id ORDER BY order_date NULLS LAST
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS acquisition_channel
        FROM order_facts
    )
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    r.order_seq,
    r.order_date,
    r.net_amount,
    r.running_ltv,
    DATEDIFF(day, r.prev_order_date, r.order_date) AS days_since_prev,
    IF(IF(r.total_qty = 0, NULL, r.total_qty) = 0, 0, r.net_amount / IF(r.total_qty = 0, NULL, r.total_qty)) AS avg_unit_price,
    IF(r.total_qty IS NULL, 0, r.total_qty) AS qty_safe,
    r.acquisition_channel
FROM ranked AS r JOIN customers AS c ON c.customer_id = r.customer_id WHERE r.order_seq <= 100
ORDER BY c.customer_id NULLS LAST, r.order_seq NULLS LAST;

SELECT
    CAST(DATE_TRUNC('MONTH', order_ts) AS DATE) AS month_start,
    SUM(IF(order_status = 'COMPLETED', total_amount, 0)) AS rev_completed,
    SUM(IF(order_status = 'REFUNDED', total_amount, 0)) AS rev_refunded,
    COUNT_IF(order_status = 'COMPLETED') AS orders_completed,
    COUNT(DISTINCT customer_id) AS active_customers,
    IF(
        COUNT_IF(order_status = 'COMPLETED') = 0,
        0,
        SUM(IF(order_status = 'COMPLETED', total_amount, 0)) / COUNT_IF(order_status = 'COMPLETED')
    ) AS avg_order_value
FROM orders WHERE order_ts >= DATE_ADD(month, -12, DATE_TRUNC('MONTH', CURRENT_DATE())) GROUP BY 1
ORDER BY 1 NULLS LAST;

SELECT
    customer_id,
    ARRAY_JOIN(
        TRANSFORM(
            ARRAY_SORT(
                ARRAY_AGG(NAMED_STRUCT('value', sku, 'sort_by_0', total_qty)),
                (left, right) ->
                CASE
                    WHEN left.sort_by_0 < right.sort_by_0 THEN 1 WHEN left.sort_by_0 > right.sort_by_0 THEN -1 ELSE 0
                END
            ),
            s -> s.value
        ),
        ', '
    ) AS top_skus
FROM
(
        SELECT o.customer_id, oi.sku, SUM(oi.quantity) AS total_qty
        FROM
            orders AS o JOIN order_items AS oi USING (order_id) GROUP BY 1, 2
            QUALIFY ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY SUM(oi.quantity) DESC NULLS FIRST) <= 5
    )
    GROUP BY customer_id;

SELECT c.customer_id, c.first_name, c.last_name, c.country
FROM
    customers AS c
    WHERE
        NOT
        EXISTS(
            SELECT 1
            FROM
                orders AS o
                WHERE o.customer_id = c.customer_id AND o.order_ts >= DATE_ADD(day, -90, CURRENT_TIMESTAMP())
        );