USE CATALOG demo_db;

CREATE SCHEMA IF NOT EXISTS demo_db.events;

USE SCHEMA events;

CREATE OR REPLACE
    TABLE events.raw_events
    (event_id VARCHAR(16777216), received_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), payload VARIANT)
    TBLPROPERTIES( 'delta.feature.allowColumnDefaults' = 'supported' )
    CLUSTER BY received_ts;

INSERT INTO events.raw_events (event_id, payload)
SELECT UUID(), PARSE_JSON(json_str)
FROM
(
        VALUES
            ('{"type":"page_view","user":{"id":42,"country":"IN"},"page":"/home","ts":"2026-05-01T10:00:00Z"}'),
(
                '{"type":"purchase","user":{"id":42,"country":"IN"},"order":{"id":"o-1","total":129.5,"items":[{"sku":"A","qty":2,"price":40.0},{"sku":"B","qty":1,"price":49.5}]},"ts":"2026-05-01T10:12:00Z"}'
            ),
            ('{"type":"click","user":{"id":99,"country":"US"},"element":"cta_signup","ts":"2026-05-02T08:01:11Z"}')
    ) AS t (json_str);

SELECT
    event_id,
    CAST(payload:type AS VARCHAR(16777216)) AS event_type,
    CAST(payload:user.id AS DECIMAL(38, 0)) AS user_id,
    CAST(payload:user.country AS VARCHAR(16777216)) AS country,
    TRY_TO_TIMESTAMP(CAST(payload:ts AS VARCHAR(16777216))) AS event_ts,
    CAST(payload:order.total AS DECIMAL(12, 2)) AS order_total,
    SIZE(payload:order.items) AS item_count
FROM events.raw_events WHERE CAST(payload:type AS VARCHAR(16777216)) IN ('purchase', 'page_view');

SELECT
    r.event_id,
    CAST(r.payload:user.id AS DECIMAL(38, 0)) AS user_id,
    f.index AS item_idx,
    CAST(f.value:sku AS VARCHAR(16777216)) AS sku,
    CAST(f.value:qty AS DECIMAL(38, 0)) AS qty,
    CAST(f.value:price AS DECIMAL(10, 2)) AS unit_price,
    CAST(f.value:qty AS DECIMAL(38, 0)) * CAST(f.value:price AS DECIMAL(10, 2)) AS line_total
FROM
    events.raw_events AS r LATERAL VIEW POSEXPLODE(r.payload:order.items) f AS index, value
    WHERE CAST(r.payload:type AS VARCHAR(16777216)) = 'purchase';

SELECT
    CAST(payload:user.id AS DECIMAL(38, 0)) AS user_id,
    STRUCT(
        CAST(payload:user.id AS DECIMAL(38, 0)) AS CAST(payload:user.id AS DECIMAL(38, 0)),
        CAST(payload:user.country AS VARCHAR(16777216)) AS country,
        MIN(CAST(payload:ts AS VARCHAR(16777216))) AS first_event,
        MAX(CAST(payload:ts AS VARCHAR(16777216))) AS last_event,
        ARRAY_AGG(DISTINCT CAST(payload:type AS VARCHAR(16777216))) AS types
    ) AS profile
FROM events.raw_events GROUP BY 1;

CREATE OR REPLACE
    VIEW events.v_purchase_items
    AS
        SELECT
            r.event_id,
            r.received_ts,
            CAST(r.payload:user.id AS DECIMAL(38, 0)) AS user_id,
            CAST(r.payload:user.country AS VARCHAR(16777216)) AS country,
            CAST(r.payload:order.id AS VARCHAR(16777216)) AS order_id,
            TRY_CAST(r.payload:order.total AS DECIMAL(12, 2)) AS order_total,
            CAST(f.value:sku AS VARCHAR(16777216)) AS sku,
            CAST(f.value:qty AS DECIMAL(38, 0)) AS qty,
            CAST(f.value:price AS DECIMAL(10, 2)) AS unit_price
        FROM
            events.raw_events AS r LATERAL VIEW POSEXPLODE(r.payload:order.items) f AS index, value
            WHERE CAST(r.payload:type AS VARCHAR(16777216)) = 'purchase';

SELECT event_id, TRY_PARSE_JSON(CAST(payload AS VARCHAR(16777216))) IS NULL AS is_corrupt FROM events.raw_events;

SELECT
    r.event_id,
    CAST(k.value AS VARCHAR(16777216)) AS key_name,
    CAST(GET(r.payload, CAST(k.value AS VARCHAR(16777216))) AS VARCHAR(16777216)) AS key_value
FROM events.raw_events AS r, LATERAL EXPLODE(JSON_OBJECT_KEYS(r.payload)) AS k;