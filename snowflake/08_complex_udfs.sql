-- ============================================================
-- COMPLEX: SQL UDF, JavaScript UDF, Python UDF, Java UDF,
-- table UDF (UDTF), secure UDF, external function (API
-- integration), masking via UDF.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.udf;
USE SCHEMA udf;

-- SQL scalar UDF
CREATE OR REPLACE FUNCTION udf.f_bucket_amount(amt NUMBER(12,2))
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS $$
    CASE
        WHEN amt IS NULL              THEN 'unknown'
        WHEN amt < 25                 THEN 'micro'
        WHEN amt BETWEEN 25 AND 99.99 THEN 'small'
        WHEN amt BETWEEN 100 AND 499  THEN 'medium'
        WHEN amt BETWEEN 500 AND 1999 THEN 'large'
        ELSE 'whale'
    END
$$;

-- JavaScript scalar UDF (no Spark equivalent — Lakebridge will flag)
CREATE OR REPLACE FUNCTION udf.f_extract_domain(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (EMAIL === null || EMAIL === undefined) return null;
    var at = EMAIL.indexOf('@');
    if (at < 0) return null;
    return EMAIL.substring(at + 1).toLowerCase();
$$;

-- Python UDF using Snowpark
CREATE OR REPLACE FUNCTION udf.f_levenshtein(a STRING, b STRING)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'lev'
PACKAGES = ('python-Levenshtein==0.21.1')
AS
$$
import Levenshtein
def lev(a, b):
    if a is None or b is None:
        return None
    return Levenshtein.distance(a, b)
$$;

-- Java UDF
CREATE OR REPLACE FUNCTION udf.f_b64encode(s STRING)
RETURNS STRING
LANGUAGE JAVA
HANDLER = 'B64.encode'
TARGET_PATH = '@~/b64.jar'
AS $$
    import java.util.Base64;
    public class B64 {
        public static String encode(String s) {
            if (s == null) return null;
            return Base64.getEncoder().encodeToString(s.getBytes());
        }
    }
$$;

-- Table UDF (UDTF) in Python: parse a log line into rows
CREATE OR REPLACE FUNCTION udf.f_split_kv(log STRING)
RETURNS TABLE (k STRING, v STRING)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'SplitKV'
AS
$$
class SplitKV:
    def process(self, log):
        if not log: return
        for tok in log.split(','):
            if '=' in tok:
                k, v = tok.split('=', 1)
                yield (k.strip(), v.strip())
$$;

-- Use the UDTF
SELECT  e.event_id,
        kv.k,
        kv.v
FROM    events.raw_events e,
LATERAL TABLE(udf.f_split_kv(e.payload:"raw_log"::STRING)) kv;

-- Secure UDF: hide implementation from data consumers
CREATE OR REPLACE SECURE FUNCTION udf.f_mask_pan(pan STRING)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS $$
    CASE
        WHEN pan IS NULL OR LENGTH(pan) < 4 THEN NULL
        ELSE REPEAT('*', LENGTH(pan) - 4) || RIGHT(pan, 4)
    END
$$;

-- Pure-SQL "fraud score" replacement (kept local; no external function)
CREATE OR REPLACE FUNCTION udf.f_score_fraud(features VARIANT)
RETURNS NUMBER(6,4)
LANGUAGE SQL
IMMUTABLE
AS $$
    LEAST(1.0,
          GREATEST(0.0,
                   0.001 * features:"amount"::NUMBER
                 + CASE features:"channel"::STRING
                       WHEN 'WEB'        THEN 0.20
                       WHEN 'CALLCENTER' THEN 0.10
                       ELSE 0.05
                   END
                 + CASE features:"country"::STRING
                       WHEN 'US' THEN 0.00
                       WHEN 'IN' THEN 0.05
                       ELSE 0.15
                   END))
$$;

-- Use everything together
SELECT  o.order_id,
        udf.f_extract_domain(c.email)            AS email_domain,
        udf.f_bucket_amount(o.total_amount)      AS amt_bucket,
        udf.f_mask_pan(o.card_pan)               AS masked_pan,
        udf.f_score_fraud(OBJECT_CONSTRUCT(
            'amount',   o.total_amount,
            'channel',  o.channel,
            'country',  c.country
        ))                                       AS fraud_score
FROM    sales.orders    o
JOIN    sales.customers c USING (customer_id)
WHERE   o.order_ts >= DATEADD(day, -1, CURRENT_TIMESTAMP());
