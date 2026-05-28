-- ============================================================
-- VERY COMPLEX: Snowflake Scripting stored procedure with
-- dynamic SQL, cursors, transactions, structured error handling,
-- nested IF, FOR loop, and VARIANT return value.
-- ============================================================

USE DATABASE demo_db;
CREATE SCHEMA IF NOT EXISTS demo_db.ops;
USE SCHEMA ops;

CREATE TABLE IF NOT EXISTS ops.proc_audit (
    proc_name       STRING,
    schema_name     STRING,
    table_name      STRING,
    started_at      TIMESTAMP_NTZ,
    ended_at        TIMESTAMP_NTZ,
    status          STRING,
    payload         VARIANT
);

CREATE OR REPLACE PROCEDURE ops.sp_rebuild_partitions(
    schema_name      STRING,
    table_name       STRING,
    partition_col    STRING,
    start_date       DATE,
    end_date         DATE,
    target_schema    STRING,
    dry_run          BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_started_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_ended_at      TIMESTAMP_NTZ;
    v_status        STRING        DEFAULT 'OK';
    v_partitions    NUMBER        DEFAULT 0;
    v_errors        NUMBER        DEFAULT 0;
    v_target_sch    STRING;
    v_src_fqn       STRING;
    v_tgt_fqn       STRING;
    v_part_date     DATE;
    v_part_sql      STRING;
    v_payload       VARIANT;
    err_msg         STRING;
BEGIN
    v_target_sch := COALESCE(target_schema, schema_name);
    v_src_fqn    := schema_name || '.' || table_name;
    v_tgt_fqn    := v_target_sch || '.' || table_name || '_rebuild';

    -- 1. Materialize a fresh target table from a CTAS shell
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || v_tgt_fqn ||
        ' AS SELECT * FROM '       || v_src_fqn || ' WHERE 1=0';

    -- 2. Iterate over the date range and rebuild one partition at a time
    LET cur CURSOR FOR
        SELECT DATEADD(day, SEQ4(), :start_date)::DATE AS d
        FROM   TABLE(GENERATOR(ROWCOUNT => 365))
        WHERE  DATEADD(day, SEQ4(), :start_date)::DATE <= :end_date;

    FOR row_var IN cur DO
        v_part_date := row_var.d;
        v_partitions := v_partitions + 1;

        IF (dry_run) THEN
            CONTINUE;
        END IF;

        v_part_sql :=
            'INSERT INTO ' || v_tgt_fqn ||
            ' SELECT * FROM ' || v_src_fqn ||
            ' WHERE ' || partition_col || '::DATE = ''' || v_part_date || '''';

        BEGIN
            EXECUTE IMMEDIATE :v_part_sql;
        EXCEPTION
            WHEN OTHER THEN
                v_errors := v_errors + 1;
        END;
    END FOR;

    -- 3. Atomic swap into place when no errors and not dry run
    IF (NOT dry_run AND v_errors = 0) THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE ' || v_src_fqn || ' SWAP WITH ' || v_tgt_fqn;
        EXECUTE IMMEDIATE
            'DROP TABLE IF EXISTS ' || v_tgt_fqn;
    ELSEIF (dry_run) THEN
        EXECUTE IMMEDIATE
            'DROP TABLE IF EXISTS ' || v_tgt_fqn;
    ELSE
        v_status := 'PARTIAL_FAILURE';
    END IF;

    v_ended_at := CURRENT_TIMESTAMP();
    v_payload  := OBJECT_CONSTRUCT(
        'status',          v_status,
        'dry_run',         dry_run,
        'partitions',      v_partitions,
        'errors',          v_errors,
        'started_at',      v_started_at,
        'ended_at',        v_ended_at
    );

    INSERT INTO ops.proc_audit
        (proc_name, schema_name, table_name, started_at, ended_at, status, payload)
    SELECT 'sp_rebuild_partitions',
           :schema_name,
           :table_name,
           :v_started_at,
           :v_ended_at,
           :v_status,
           :v_payload;

    RETURN v_payload;

EXCEPTION
    WHEN OTHER THEN
        err_msg := SQLERRM;
        INSERT INTO ops.proc_audit
            (proc_name, schema_name, table_name, started_at, ended_at, status, payload)
        SELECT 'sp_rebuild_partitions',
               :schema_name,
               :table_name,
               :v_started_at,
               CURRENT_TIMESTAMP(),
               'ERROR',
               OBJECT_CONSTRUCT('error', :err_msg);
        RAISE;
END;
$$;

-- Driver: drive a 60-day rebuild
CALL ops.sp_rebuild_partitions(
    'SALES',
    'ORDERS',
    'ORDER_TS',
    DATEADD(day, -60, CURRENT_DATE())::DATE,
    CURRENT_DATE()::DATE,
    'SALES',
    FALSE
);

-- Anonymous Snowflake Scripting block
EXECUTE IMMEDIATE $$
DECLARE
    v_rows  NUMBER DEFAULT 0;
    v_msg   STRING;
BEGIN
    LET cur CURSOR FOR
        SELECT customer_id FROM demo_db.sales.customers WHERE is_active LIMIT 1000;

    FOR row_var IN cur DO
        INSERT INTO demo_db.sales.audit_log (customer_id, audited_at)
        VALUES (:row_var.customer_id, CURRENT_TIMESTAMP());
        v_rows := v_rows + 1;
    END FOR;

    v_msg := 'Audited ' || v_rows || ' customers';
    RETURN v_msg;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERR: ' || SQLERRM;
END;
$$;
