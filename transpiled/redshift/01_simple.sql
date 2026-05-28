-- ============================================================
-- SIMPLE: Basic Redshift DDL/DML operations
-- ============================================================

-- Create a simple customers table with Redshift-specific keywords
CREATE OR REPLACE TABLE sales.customers (
    customer_id     INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
    first_name STRING ,
    last_name STRING ,
    email STRING ,
    signup_date     DATE,
    country STRING ,
    is_active       BOOLEAN DEFAULT TRUE
)


ZORDER BY(signup_date);

-- Simple INSERT
INSERT INTO sales.customers (first_name, last_name, email, signup_date, country)
VALUES
    ('Alice',   'Singh',  'alice@example.com',  '2024-01-15', 'IN'),
    ('Bob',     'Khan',   'bob@example.com',    '2024-02-20', 'US'),
    ('Charlie', 'Lopez',  'chuck@example.com',  '2024-03-05', 'MX');

-- Simple SELECT with WHERE / ORDER BY
SELECT customer_id,
       first_name || ' ' || last_name AS full_name,
       email,
       signup_date
FROM   sales.customers
WHERE  is_active = TRUE
  AND  country IN ('IN', 'US')
ORDER  BY signup_date DESC
LIMIT  100;
-- Simple aggregation
SELECT
    country,
    COUNT(*) AS customer_count,
    MIN(signup_date) AS first_signup,
    MAX(signup_date) AS last_signup
FROM sales.customers
GROUP BY country
ORDER BY customer_count DESC;

-- Simple UPDATE
UPDATE sales.customers
SET    is_active = FALSE
WHERE  signup_date < '2024-01-01';

-- Simple DELETE
DELETE FROM sales.customers
WHERE  email IS NULL;
