USE CATALOG demo_db;

CREATE SCHEMA IF NOT EXISTS demo_db.ingest;

USE SCHEMA ingest;

CREATE OR REPLACE
/*
        STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3Role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/raw/', 's3://my-bucket/exports/')
        -- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE STORAGE INTEGRATION command, and it cannot be translated
    */;

/*
    CREATE OR REPLACE FILE FORMAT ingest.ff_csv_orders
    TYPE = CSV
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    ENCODING = 'UTF8';
*/
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE FILE FORMAT command (not required in DBSQL), and it cannot be translated

/* CREATE OR REPLACE FILE FORMAT ingest.ff_json
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    DATE_FORMAT = 'AUTO'; */
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE FILE FORMAT command (not required in DBSQL), and it cannot be translated

/* CREATE OR REPLACE FILE FORMAT ingest.ff_parquet
    TYPE = PARQUET
    COMPRESSION = SNAPPY; */
-- FIXME: SNOWFLAKE: Databricks SQL has no equivalent to the CREATE FILE FORMAT command (not required in DBSQL), and it cannot be translated

CREATE OR REPLACE
/*
        STAGE ingest.s3_raw_orders
    URL = 's3://my-bucket/raw/orders/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = ingest.ff_csv_orders
        -- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE STAGE command, but may be able to do so in the future
    */;

/*
    COPY INTO cdc.bronze_orders
FROM @ingest.s3_raw_orders
FILE_FORMAT = (FORMAT_NAME = ingest.ff_csv_orders)
PATTERN = '.*orders_.*[.]csv[.]gz'
ON_ERROR = 'CONTINUE'
    -- FIXME: SNOWFLAKE: The transpiler cannot currently convert the COPY INTO command, but may be able to do so in the future
*/

CREATE OR REPLACE
/*
        PIPE ingest.p_orders_autoingest
AUTO_INGEST = TRUE
AS
COPY INTO cdc.bronze_orders
FROM @ingest.s3_raw_orders
FILE_FORMAT = (FORMAT_NAME = ingest.ff_csv_orders)
ON_ERROR = 'CONTINUE'
        -- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE PIPE command, but may be able to do so in the future
    */;

CREATE OR REPLACE
/*
        EXTERNAL TABLE ingest.ext_clickstream (
    event_date DATE AS (TO_DATE(SUBSTR(metadata$filename, 17, 10), 'YYYY-MM-DD'))
)
LOCATION = @ingest.s3_raw_orders
PATTERN = '.*[.]parquet'
AUTO_REFRESH = TRUE
FILE_FORMAT = (FORMAT_NAME = ingest.ff_parquet)
        -- FIXME: SNOWFLAKE: The transpiler cannot currently convert the CREATE EXTERNAL TABLE command, but may be able to do so in the future
    */;

ALTER /* EXTERNAL TABLE ingest.ext_clickstream REFRESH */;
-- FIXME: SNOWFLAKE: The transpiler cannot currently convert the ALTER EXTERNAL TABLE command, but may be able to do so in the future

SELECT file_name, status, row_count, row_parsed, first_error_message
FROM
    demo_db.information_schema.load_history
    WHERE table_name = 'BRONZE_ORDERS' AND last_load_time >= DATE_ADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY file_name DESC NULLS FIRST;