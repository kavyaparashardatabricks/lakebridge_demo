-- ============================================================
-- COMPLEX: Storage integrations, stages, file formats,
-- COPY INTO (load), Snowpipe (auto-ingest), external tables.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.ingest;
USE SCHEMA ingest;

-- Storage integration for S3 (one-time setup by ACCOUNTADMIN)
CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3Role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/raw/', 's3://my-bucket/exports/');

-- File formats
CREATE OR REPLACE FILE FORMAT ingest.ff_csv_orders
    TYPE = CSV
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    ENCODING = 'UTF8';

CREATE OR REPLACE FILE FORMAT ingest.ff_json
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    DATE_FORMAT = 'AUTO';

CREATE OR REPLACE FILE FORMAT ingest.ff_parquet
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

-- External stage
CREATE OR REPLACE STAGE ingest.s3_raw_orders
    URL = 's3://my-bucket/raw/orders/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = ingest.ff_csv_orders;

-- Bulk load with options
COPY INTO cdc.bronze_orders
FROM @ingest.s3_raw_orders
FILE_FORMAT = (FORMAT_NAME = ingest.ff_csv_orders)
PATTERN = '.*orders_.*[.]csv[.]gz'
ON_ERROR = 'CONTINUE';

-- Snowpipe for auto-ingest
CREATE OR REPLACE PIPE ingest.p_orders_autoingest
AUTO_INGEST = TRUE
AS
COPY INTO cdc.bronze_orders
FROM @ingest.s3_raw_orders
FILE_FORMAT = (FORMAT_NAME = ingest.ff_csv_orders)
ON_ERROR = 'CONTINUE';

-- External (Parquet) table with virtual column derived from filename
CREATE OR REPLACE EXTERNAL TABLE ingest.ext_clickstream (
    event_date DATE AS (TO_DATE(SUBSTR(metadata$filename, 17, 10), 'YYYY-MM-DD'))
)
LOCATION = @ingest.s3_raw_orders
PATTERN = '.*[.]parquet'
AUTO_REFRESH = TRUE
FILE_FORMAT = (FORMAT_NAME = ingest.ff_parquet);

ALTER EXTERNAL TABLE ingest.ext_clickstream REFRESH;

-- Audit COPY history via INFORMATION_SCHEMA
SELECT  file_name,
        status,
        row_count,
        row_parsed,
        first_error_message
FROM    demo_db.information_schema.load_history
WHERE   table_name = 'BRONZE_ORDERS'
  AND   last_load_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY file_name DESC;
