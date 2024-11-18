-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS test;
GRANT USAGE ON SCHEMA test TO PUBLIC;

-- Utility functions to show relation information in tests. These
-- functions generate output which is the same across PostgreSQL
-- versions. Their usage is preferred over psql's '\d <relation>',
-- since that output typically changes across PostgreSQL versions.

-- this function is duplicated in test/isolation/specs/multi_transaction_indexing.spec
-- if it changes, that copy may need to change as well
CREATE OR REPLACE FUNCTION test.show_columns(rel regclass)
RETURNS TABLE("Column" name,
              "Type" text,
              "NotNull" boolean) LANGUAGE SQL STABLE AS
$BODY$
    SELECT a.attname,
    format_type(t.oid, t.typtypmod),
    a.attnotnull
    FROM pg_attribute a, pg_type t
    WHERE a.attrelid = rel
    AND a.atttypid = t.oid
    AND a.attnum >= 0
    ORDER BY a.attnum;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_columnsp(pattern text)
RETURNS TABLE("Relation" regclass,
              "Kind" "char",
              "Column" name,
              "Column type" text,
              "NotNull" boolean) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT c.oid::regclass,
    c.relkind,
    a.attname,
    format_type(t.oid, t.typtypmod),
    a.attnotnull
    FROM pg_class c, pg_attribute a, pg_type t
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND a.attrelid = c.oid
    AND a.atttypid = t.oid
    AND a.attnum >= 0
    ORDER BY c.relname, a.attnum;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_indexes(rel regclass)
RETURNS TABLE("Index" regclass,
              "Columns" name[],
              "Expr" text,
              "Unique" boolean,
              "Primary" boolean,
              "Exclusion" boolean,
              "Tablespace" name) LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.oid::regclass,
    array(SELECT "Column" FROM test.show_columns(i.indexrelid)),
    pg_get_expr(i.indexprs, c.oid, true),
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    (SELECT t.spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)
    FROM pg_class c, pg_index i
    WHERE c.oid = i.indexrelid AND i.indrelid = rel
    ORDER BY c.relname;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_indexespred(rel regclass)
RETURNS TABLE("Index" regclass,
              "Columns" name[],
              "Expr" text,
              "Pred" text,
              "Unique" boolean,
              "Primary" boolean,
              "Exclusion" boolean,
              "Tablespace" name) LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.oid::regclass,
    array(SELECT "Column" FROM test.show_columns(i.indexrelid)),
    pg_get_expr(i.indexprs, i.indrelid, true),
    pg_get_expr(i.indpred, i.indrelid, true),
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    (SELECT t.spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)
    FROM pg_class c, pg_index i
    WHERE c.oid = i.indexrelid AND i.indrelid = rel
    ORDER BY c.relname;
$BODY$;

-- this function is duplicated in test/isolation/specs/multi_transaction_indexing.spec
-- if it changes, that copy may need to change as well
CREATE OR REPLACE FUNCTION test.show_indexesp(pattern text)
RETURNS TABLE("Table" regclass,
              "Index" regclass,
              "Columns" name[],
              "Expr" text,
              "Unique" boolean,
              "Primary" boolean,
              "Exclusion" boolean,
              "Tablespace" name) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT c.oid::regclass,
    i.indexrelid::regclass,
    array(SELECT "Column" FROM test.show_columns(i.indexrelid)),
    pg_get_expr(i.indexprs, c.oid, true),
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    (SELECT t.spcname FROM pg_class cc, pg_tablespace t WHERE cc.oid = i.indexrelid AND t.oid = cc.reltablespace)
    FROM pg_class c, pg_index i
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND c.oid = i.indrelid
    ORDER BY c.oid, i.indexrelid;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_constraints(rel regclass)
RETURNS TABLE("Constraint" name,
              "Type" "char",
              "Columns" name[],
              "Index" regclass,
              "Expr" text,
              "Deferrable" bool,
              "Deferred" bool,
              "Validated" bool) LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.conname,
    c.contype,
    array(SELECT attname FROM pg_attribute a, unnest(conkey) k WHERE a.attrelid = rel AND k = a.attnum),
    c.conindid::regclass,
    pg_get_expr(c.conbin, c.conrelid),
    c.condeferrable,
    c.condeferred,
    c.convalidated
    FROM pg_constraint c
    WHERE c.conrelid = rel
    ORDER BY c.conname;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_constraintsp(pattern text)
RETURNS TABLE("Table" regclass,
              "Constraint" name,
              "Type" "char",
              "Columns" name[],
              "Index" regclass,
              "Expr" text,
              "Deferrable" bool,
              "Deferred" bool,
              "Validated" bool) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT cl.oid::regclass,
    c.conname,
    c.contype,
    array(SELECT attname FROM pg_attribute a, unnest(conkey) k WHERE a.attrelid = cl.oid AND k = a.attnum),
    c.conindid::regclass,
    pg_get_expr(c.conbin, c.conrelid),
    c.condeferrable,
    c.condeferred,
    c.convalidated
    FROM pg_class cl, pg_constraint c
    WHERE format('%I.%I', cl.relnamespace::regnamespace::name, cl.relname) LIKE format('%I.%s', schema_name, table_name)
    AND c.conrelid = cl.oid
    ORDER BY cl.relname, c.conname;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_triggers(rel regclass, show_internal boolean = false)
RETURNS TABLE("Trigger" name,
              "Type" smallint,
              "Function" regproc) LANGUAGE SQL STABLE AS
$BODY$
    SELECT t.tgname,
    t.tgtype,
    t.tgfoid::regproc
    FROM pg_trigger t
    WHERE t.tgrelid = rel
    AND t.tgisinternal = show_internal
    ORDER BY t.tgname;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_triggersp(pattern text, show_internal boolean = false)
RETURNS TABLE("Table" regclass,
              "Trigger" name,
              "Type" smallint,
              "Function" regproc) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT t.tgrelid::regclass,
    t.tgname,
    t.tgtype,
    t.tgfoid::regproc
    FROM pg_class cl, pg_trigger t
    WHERE format('%I.%I', cl.relnamespace::regnamespace::name, cl.relname) LIKE format('%I.%s', schema_name, table_name)
    AND t.tgrelid = cl.oid
    AND t.tgisinternal = show_internal
    ORDER BY t.tgrelid, t.tgname;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_subtables(rel regclass)
RETURNS TABLE("Child" regclass,
              "Tablespace" name) LANGUAGE SQL STABLE AS
$BODY$
    SELECT objid::regclass, (SELECT t.spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)
    FROM pg_depend d, pg_class c
    WHERE d.refobjid = rel
    AND d.deptype = 'n'
    AND d.classid = 'pg_class'::regclass
    AND d.objid = c.oid
    ORDER BY d.refobjid, d.objid;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_subtablesp(pattern text)
RETURNS TABLE("Parent" regclass,
              "Child" regclass,
              "Tablespace" name) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT refobjid::regclass,
    objid::regclass,
    (SELECT t.spcname FROM pg_class cc, pg_tablespace t WHERE cc.oid = d.objid AND t.oid = cc.reltablespace)
    FROM pg_class c, pg_depend d
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND d.refobjid = c.oid
    AND d.deptype = 'n'
    AND d.classid = 'pg_class'::regclass
    ORDER BY d.refobjid, d.objid;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.execute_sql(cmd TEXT)
RETURNS TEXT LANGUAGE PLPGSQL AS $BODY$
BEGIN
  EXECUTE cmd;
  RETURN cmd;
END
$BODY$;

-- Used to set a deterministic memory setting during tests
CREATE OR REPLACE FUNCTION test.set_memory_cache_size(memory_amount text)
RETURNS BIGINT AS :MODULE_PATHNAME, 'ts_set_memory_cache_size' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION test.make_tablespace_path(prefix TEXT, test_name TEXT)
       RETURNS TEXT LANGUAGE plpgsql AS
$BODY$
DECLARE
    mkdirFlag TEXT := CASE WHEN sysname = 'Windows' THEN '' ELSE '-p ' END
        FROM _timescaledb_functions.get_os_info();
    dirPath TEXT := format('%s%s', prefix, test_name);
    createDir TEXT := format('mkdir %s%s', mkdirFlag, dirPath);
BEGIN
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %s', quote_literal(createDir));
    RETURN dirPath;
END;
$BODY$;

-- Wait for job to execute with success or failure
CREATE OR REPLACE FUNCTION test.wait_for_job_to_run(job_param_id INTEGER, expected_runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS)
RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    r RECORD;
BEGIN
    FOR i in 1..spins
    LOOP
        SELECT total_successes, total_failures FROM _timescaledb_internal.bgw_job_stat WHERE job_id=job_param_id INTO r;
        IF (r.total_failures > 0) THEN
            RAISE INFO 'wait_for_job_to_run: job execution failed';
            RETURN false;
        ELSEIF (r.total_successes = expected_runs) THEN
            RETURN true;
        ELSEIF (r.total_successes > expected_runs) THEN
            RAISE 'num_runs > expected';
        ELSE
            PERFORM pg_sleep(0.1);
        END IF;
    END LOOP;
    RAISE INFO 'wait_for_job_to_run: timeout after % tries', spins;
    RETURN false;
END
$BODY$;

-- Wait for job to run or fail
CREATE OR REPLACE FUNCTION test.wait_for_job_to_run_or_fail(job_param_id INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS)
RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    r RECORD;
BEGIN
    FOR i in 1..spins
    LOOP
        SELECT total_runs FROM _timescaledb_internal.bgw_job_stat WHERE job_id=job_param_id INTO r;
        IF (r.total_runs > 0) THEN
            RETURN true;
        ELSE
            PERFORM pg_sleep(0.1);
        END IF;
    END LOOP;
    RAISE INFO 'wait_for_job_to_run_or_fail: timeout after % tries', spins;
    RETURN false;
END
$BODY$;
