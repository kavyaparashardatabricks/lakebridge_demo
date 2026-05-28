USE CATALOG demo_db;

USE SCHEMA sales;

SELECT
    customer_id,
    INITCAP(first_name) || ' ' || INITCAP(last_name) AS full_name,
    LOWER(email) AS email,
    country,
    CAST(signup_ts AS DATE) AS signup_date
FROM
    customers
    WHERE
        is_active = true AND signup_ts >= DATE_ADD(month, -6, CURRENT_TIMESTAMP()) AND
        LOWER(email) ILIKE '%@example.com'
ORDER BY signup_ts DESC NULLS FIRST;

SELECT
    country,
    COUNT(*) AS customer_count,
    COUNT_IF(is_active) AS active_count,
    COALESCE(AVG(DATEDIFF(day, signup_ts, CURRENT_DATE())), 0) AS avg_tenure_days,
    IF(COUNT(*) > 100, 'large', 'small') AS segment_size
FROM customers GROUP BY country HAVING COUNT(*) > 0
ORDER BY customer_count DESC NULLS FIRST;

UPDATE customers SET is_active = false WHERE signup_ts < DATE_ADD(year, -3, CURRENT_DATE());

DELETE FROM customers WHERE email IS NULL OR TRIM(email) = '';

SELECT table_name, row_count, bytes FROM demo_db.information_schema.tables WHERE table_schema = 'SALES'
ORDER BY bytes DESC NULLS FIRST;