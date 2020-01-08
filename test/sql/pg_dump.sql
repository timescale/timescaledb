-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA test_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME
ALTER TABLE PUBLIC."two_Partitions" SET SCHEMA "test_schema";

-- Test that we can restore constraints
ALTER TABLE "test_schema"."two_Partitions"
ADD CONSTRAINT timeCustom_device_id_series_2_key
UNIQUE ("timeCustom", device_id, series_2);

-- Test that we can restore triggers
CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN NEW;
END
$BODY$;

-- Test that a custom chunk sizing function is restored
CREATE OR REPLACE FUNCTION custom_calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN -1;
END
$BODY$;

SELECT * FROM set_adaptive_chunking('"test_schema"."two_Partitions"', '1 MB', 'custom_calculate_chunk_interval');

-- Chunk sizing func set
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT proname, pronamespace, pronargs
FROM pg_proc WHERE proname = 'custom_calculate_chunk_interval';

CREATE TRIGGER restore_trigger BEFORE INSERT ON "test_schema"."two_Partitions"

FOR EACH ROW EXECUTE PROCEDURE test_trigger();

-- Save the number of dependent objects so we can make sure we have the same number later
SELECT count(*) as num_dependent_objects
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb')
\gset

SELECT * FROM test.show_columns('"test_schema"."two_Partitions"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_indexes('"test_schema"."two_Partitions"');
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_constraints('"test_schema"."two_Partitions"');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_triggers('"test_schema"."two_Partitions"');
SELECT * FROM test.show_triggers('_timescaledb_internal._hyper_1_1_chunk');

SELECT * FROM "test_schema"."two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
SELECT * FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY "timeCustom", device_id, series_0, series_1;
SELECT * FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY "timeCustom", device_id, series_0, series_1;

-- Show all index mappings
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM _timescaledb_catalog.chunk_constraint;

--force a value to exist for exported_uuid
INSERT INTO _timescaledb_catalog.metadata VALUES ('exported_uuid', 'original_uuid', true);

\c postgres :ROLE_SUPERUSER

-- We shell out to a script in order to grab the correct hostname from the
-- environmental variables that originally called this psql command. Sadly
-- vars passed to psql do not work in \! commands so we can't do it that way.
\! utils/pg_dump_aux_dump.sh dump/pg_dump.sql

\c :TEST_DBNAME
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
--create a exported uuid before restoring (mocks telemetry running before restore)
INSERT INTO _timescaledb_catalog.metadata VALUES ('exported_uuid', 'new_db_uuid', true);
RESET client_min_messages;
SELECT timescaledb_pre_restore();
SHOW timescaledb.restoring;

\! utils/pg_dump_aux_restore.sh dump/pg_dump.sql


-- Inserting with restoring ON in current session causes tuples to be
-- inserted on main table, but this should be protected by the insert
-- blocking trigger.
\set ON_ERROR_STOP 0
INSERT INTO "test_schema"."two_Partitions"("timeCustom", device_id, series_0, series_1)
VALUES (1357894000000000000, 'dev5', 1.5, 2);
\set ON_ERROR_STOP 1

-- Now run our post-restore function.
SELECT timescaledb_post_restore();
SHOW timescaledb.restoring;

--should be same as count above
SELECT count(*) = :num_dependent_objects as dependent_objects_match
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

--we should have the original uuid from the backed up db set as the exported_uuid
SELECT value = 'original_uuid' FROM _timescaledb_catalog.metadata  WHERE key='exported_uuid';
SELECT count(*) = 1 FROM _timescaledb_catalog.metadata WHERE key LIKE 'exported%';

--main table and chunk schemas should be the same
SELECT * FROM test.show_columns('"test_schema"."two_Partitions"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_indexes('"test_schema"."two_Partitions"');
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_constraints('"test_schema"."two_Partitions"');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_triggers('"test_schema"."two_Partitions"');
SELECT * FROM test.show_triggers('_timescaledb_internal._hyper_1_1_chunk');

--data should be the same
SELECT * FROM "test_schema"."two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1;
SELECT * FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY "timeCustom", device_id, series_0, series_1;
SELECT * FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY "timeCustom", device_id, series_0, series_1;

SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM _timescaledb_catalog.chunk_constraint;

--Chunk sizing function should have been restored
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT proname, pronamespace, pronargs
FROM pg_proc WHERE proname = 'custom_calculate_chunk_interval';

--check simple ddl still works
ALTER TABLE "test_schema"."two_Partitions" ADD COLUMN series_3 integer;
INSERT INTO "test_schema"."two_Partitions"("timeCustom", device_id, series_0, series_1, series_3) VALUES
(1357894000000000000, 'dev5', 1.5, 2, 4);

SELECT * FROM ONLY "test_schema"."two_Partitions";

--query for the extension tables/sequences that will not be dumped by pg_dump (should be empty except for views)
SELECT objid::regclass
FROM pg_catalog.pg_depend
WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
        refobjid = (select oid from pg_extension where extname='timescaledb') AND
        deptype = 'e' AND
        classid='pg_catalog.pg_class'::pg_catalog.regclass
        AND objid NOT IN (select unnest(extconfig) from pg_extension where extname='timescaledb')
ORDER BY objid::text DESC;

-- Make sure we can't run our restoring functions as a normal perm user as that would disable functionality for the whole db
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
-- Hides error messages in cases where error messages differ between Postgres versions
create or replace function get_sqlstate(in_text TEXT) RETURNS TEXT AS
$$
BEGIN
    BEGIN
        EXECUTE in_text;
    EXCEPTION WHEN others THEN GET STACKED DIAGNOSTICS in_text = RETURNED_SQLSTATE;
    END;
    RETURN in_text;
END;
$$
LANGUAGE PLPGSQL;

SELECT get_sqlstate('SELECT timescaledb_pre_restore()');
SELECT get_sqlstate('SELECT timescaledb_post_restore()');

drop function get_sqlstate(TEXT);

--use a standard dbname because :TEST_DBNAME is different on 9.6 vs 10 & 11
--and dbname is displayed in error
\c :TEST_DBNAME :ROLE_SUPERUSER
--need to shutdown workers to use db as template
SELECT _timescaledb_internal.stop_background_workers();
CREATE DATABASE db_dump_error WITH TEMPLATE :TEST_DBNAME;

--now test functions for permission errors
\c  db_dump_error :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT timescaledb_pre_restore();
SELECT timescaledb_post_restore();
\set ON_ERROR_STOP 1

--drop db
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE db_dump_error;
