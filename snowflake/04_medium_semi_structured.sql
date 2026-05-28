-- ============================================================
-- MEDIUM: VARIANT / OBJECT / ARRAY, PARSE_JSON,
-- OBJECT_CONSTRUCT, LATERAL FLATTEN, path navigation,
-- TRY_PARSE_JSON, TRY_CAST.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.events;
USE SCHEMA events;

-- Landing table for raw JSON events
CREATE OR REPLACE TABLE events.raw_events (
    event_id        STRING,
    received_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    payload         VARIANT
)
CLUSTER BY (received_ts);

-- Insert some test events using PARSE_JSON / OBJECT_CONSTRUCT
INSERT INTO events.raw_events (event_id, payload)
SELECT  UUID_STRING(),
        PARSE_JSON(json_str)
FROM    (VALUES
    ('{"type":"page_view","user":{"id":42,"country":"IN"},"page":"/home","ts":"2026-05-01T10:00:00Z"}'),
    ('{"type":"purchase","user":{"id":42,"country":"IN"},"order":{"id":"o-1","total":129.5,"items":[{"sku":"A","qty":2,"price":40.0},{"sku":"B","qty":1,"price":49.5}]},"ts":"2026-05-01T10:12:00Z"}'),
    ('{"type":"click","user":{"id":99,"country":"US"},"element":"cta_signup","ts":"2026-05-02T08:01:11Z"}')
) AS t(json_str);

-- Typed projection from VARIANT
SELECT  event_id,
        payload:"type"::STRING                              AS event_type,
        payload:"user":"id"::NUMBER                         AS user_id,
        payload:"user":"country"::STRING                    AS country,
        TRY_TO_TIMESTAMP_NTZ(payload:"ts"::STRING)          AS event_ts,
        payload:"order":"total"::NUMBER(12,2)               AS order_total,
        ARRAY_SIZE(payload:"order":"items")                 AS item_count
FROM    events.raw_events
WHERE   payload:"type"::STRING IN ('purchase','page_view');

-- LATERAL FLATTEN: explode items array
SELECT  r.event_id,
        r.payload:"user":"id"::NUMBER                       AS user_id,
        f.index                                             AS item_idx,
        f.value:"sku"::STRING                               AS sku,
        f.value:"qty"::NUMBER                               AS qty,
        f.value:"price"::NUMBER(10,2)                       AS unit_price,
        f.value:"qty"::NUMBER * f.value:"price"::NUMBER(10,2) AS line_total
FROM    events.raw_events r,
LATERAL FLATTEN(input => r.payload:"order":"items") f
WHERE   r.payload:"type"::STRING = 'purchase';

-- Build a VARIANT result for downstream apps
SELECT  payload:"user":"id"::NUMBER                         AS user_id,
        OBJECT_CONSTRUCT(
            'user_id',     payload:"user":"id"::NUMBER,
            'country',     payload:"user":"country"::STRING,
            'first_event', MIN(payload:"ts"::STRING),
            'last_event',  MAX(payload:"ts"::STRING),
            'types',       ARRAY_AGG(DISTINCT payload:"type"::STRING)
        )                                                   AS profile
FROM    events.raw_events
GROUP   BY 1;

-- Schema-on-read view exposed as a typed view
CREATE OR REPLACE VIEW events.v_purchase_items AS
SELECT  r.event_id,
        r.received_ts,
        r.payload:"user":"id"::NUMBER             AS user_id,
        r.payload:"user":"country"::STRING        AS country,
        r.payload:"order":"id"::STRING            AS order_id,
        TRY_CAST(r.payload:"order":"total" AS NUMBER(12,2)) AS order_total,
        f.value:"sku"::STRING                     AS sku,
        f.value:"qty"::NUMBER                     AS qty,
        f.value:"price"::NUMBER(10,2)             AS unit_price
FROM    events.raw_events r,
LATERAL FLATTEN(input => r.payload:"order":"items") f
WHERE   r.payload:"type"::STRING = 'purchase';

-- Defensive cast: corrupt payload that doesn't parse
SELECT  event_id,
        TRY_PARSE_JSON(payload::STRING) IS NULL AS is_corrupt
FROM    events.raw_events;

-- Pivot a VARIANT into wide columns with OBJECT_KEYS
SELECT  r.event_id,
        k.value::STRING                                     AS key_name,
        GET(r.payload, k.value::STRING)::STRING             AS key_value
FROM    events.raw_events r,
LATERAL FLATTEN(input => OBJECT_KEYS(r.payload)) k;
