-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_DBNAME_EXTRA :TEST_DBNAME _extra

\o /dev/null
\ir include/insert_two_partitions.sql
\o
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION bgw_wait(database TEXT, timeout INT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_bgw_wait'
LANGUAGE C VOLATILE;

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

FOR EACH ROW EXECUTE FUNCTION test_trigger();

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
-- disable background jobs
UPDATE _timescaledb_config.bgw_job SET scheduled = false;
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
-- timescaledb_post_restore restarts background worker so we have to stop them
-- to make sure they dont interfere with this database being used as template below
SELECT _timescaledb_internal.stop_background_workers();

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

-- Check that the extension can be copied from an existing database
-- without explicitly installing it. Stop background workers since we
-- cannot have any backends connected to the database when cloning it.
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT timescaledb_pre_restore();
SELECT bgw_wait(:'TEST_DBNAME', 60);

CREATE DATABASE :TEST_DBNAME_EXTRA WITH TEMPLATE :TEST_DBNAME;

-- Connect to the database and do some basic stuff to check that the
-- extension works.
\c :TEST_DBNAME_EXTRA :ROLE_DEFAULT_PERM_USER
CREATE TABLE test_tz(time timestamptz not null, temp float8, device text);

SELECT create_hypertable('test_tz', 'time', 'device', 2);

SELECT id, schema_name, table_name FROM _timescaledb_catalog.hypertable;

INSERT INTO test_tz VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
INSERT INTO test_tz VALUES('Mon Mar 20 09:27:00.936242 2017', 22, 'dev2');
INSERT INTO test_tz VALUES('Mon Mar 20 09:28:00.936242 2017', 21.2, 'dev1');
INSERT INTO test_tz VALUES('Mon Mar 20 09:37:00.936242 2017', 30, 'dev3');
SELECT * FROM test_tz ORDER BY time;

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE :TEST_DBNAME_EXTRA;
