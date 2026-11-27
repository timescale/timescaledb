-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE check_ht(time timestamptz NOT NULL, value int);
SELECT FROM create_hypertable('check_ht', 'time');
INSERT INTO check_ht VALUES ('2020-01-01 00:00:00+00', 1);
CREATE TABLE check_plain(a int);
CREATE VIEW check_view AS SELECT 1 AS a;

SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.chunk'::regclass AND contype = 'c';

\set ON_ERROR_STOP 0
-- invalid chunk relations
UPDATE _timescaledb_catalog.chunk SET relid = 'pg_class';
UPDATE _timescaledb_catalog.chunk SET relid = 'check_view';
UPDATE _timescaledb_catalog.chunk SET relid = 4000000000::oid::regclass;
INSERT INTO _timescaledb_catalog.chunk (relid, hypertable_id, creation_time)
SELECT 'pg_class', id, now() FROM _timescaledb_catalog.hypertable;
-- chunk relation that is not a child of the hypertable
INSERT INTO _timescaledb_catalog.chunk (relid, hypertable_id, creation_time)
SELECT 'check_plain', id, now() FROM _timescaledb_catalog.hypertable;
-- chunk row for a hypertable that does not exist
INSERT INTO _timescaledb_catalog.chunk (relid, hypertable_id, creation_time)
VALUES ('check_plain', 1000, now());
-- violations use the TS601 error code
\set VERBOSITY sqlstate
UPDATE _timescaledb_catalog.chunk SET relid = 'check_plain';
\set VERBOSITY terse
-- COPY in restoring mode is validated too
SET timescaledb.restoring = on;
COPY _timescaledb_catalog.chunk (id, relid, hypertable_id, status, osm_chunk, creation_time) FROM stdin;
1000	pg_class	1	0	f	2020-01-01 00:00:00+00
\.
RESET timescaledb.restoring;
\set ON_ERROR_STOP 1

-- valid values are accepted
BEGIN;
UPDATE _timescaledb_catalog.chunk SET status = 4;
SELECT relid, status FROM _timescaledb_catalog.chunk ORDER BY id;
ROLLBACK;

-- compression settings
CREATE TABLE check_compressed(a int);
CREATE TABLE check_other(a int);
ALTER TABLE check_other OWNER TO :ROLE_DEFAULT_PERM_USER;

SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.compression_settings'::regclass AND contype = 'c' ORDER BY 1;

-- valid values are accepted
INSERT INTO _timescaledb_catalog.compression_settings (relid) VALUES ('check_ht');
INSERT INTO _timescaledb_catalog.compression_settings (relid, compress_relid) VALUES ('check_plain', 'check_compressed');

\set ON_ERROR_STOP 0
-- invalid relations
INSERT INTO _timescaledb_catalog.compression_settings (relid) VALUES ('pg_class');
INSERT INTO _timescaledb_catalog.compression_settings (relid) VALUES ('check_view');
INSERT INTO _timescaledb_catalog.compression_settings (relid) VALUES (4000000000::oid::regclass);
-- invalid compressed relations
UPDATE _timescaledb_catalog.compression_settings SET compress_relid = 'pg_class' WHERE relid = 'check_plain'::regclass;
UPDATE _timescaledb_catalog.compression_settings SET compress_relid = 'check_view' WHERE relid = 'check_plain'::regclass;
UPDATE _timescaledb_catalog.compression_settings SET compress_relid = 4000000000::oid::regclass WHERE relid = 'check_plain'::regclass;
UPDATE _timescaledb_catalog.compression_settings SET compress_relid = relid WHERE relid = 'check_plain'::regclass;
-- compressed relation owned by a different role
UPDATE _timescaledb_catalog.compression_settings SET compress_relid = 'check_other' WHERE relid = 'check_plain'::regclass;
\set ON_ERROR_STOP 1

DELETE FROM _timescaledb_catalog.compression_settings;

-- chunk rewrite
\set CHUNK _timescaledb_internal._hyper_1_1_chunk
SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.chunk_rewrite'::regclass AND contype = 'c';

-- valid values are accepted
INSERT INTO _timescaledb_catalog.chunk_rewrite VALUES (:'CHUNK', 'check_plain');

\set ON_ERROR_STOP 0
-- invalid chunk relations
INSERT INTO _timescaledb_catalog.chunk_rewrite VALUES (4000000000::oid::regclass, 'check_compressed');
INSERT INTO _timescaledb_catalog.chunk_rewrite VALUES ('check_plain', 'check_compressed');
INSERT INTO _timescaledb_catalog.chunk_rewrite VALUES ('check_ht', 'check_compressed');
-- invalid rewrite relations
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 4000000000::oid::regclass;
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 'pg_class';
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 'check_view';
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 'check_other';
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 'check_ht';
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = chunk_relid;
\set ON_ERROR_STOP 1

DELETE FROM _timescaledb_catalog.chunk_rewrite;

-- hypertable
SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.hypertable'::regclass AND contype = 'c' ORDER BY 1;

\set ON_ERROR_STOP 0
-- invalid hypertable relations
UPDATE _timescaledb_catalog.hypertable SET table_name = 'check_missing';
UPDATE _timescaledb_catalog.hypertable SET schema_name = 'pg_catalog', table_name = 'pg_class';
UPDATE _timescaledb_catalog.hypertable SET table_name = 'check_view';
-- hypertable relation that is not the parent of the chunks
UPDATE _timescaledb_catalog.hypertable SET table_name = 'check_plain';
\set ON_ERROR_STOP 1

-- valid values are accepted
BEGIN;
INSERT INTO _timescaledb_catalog.hypertable (schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size)
SELECT schema_name, 'check_plain', associated_schema_name, 'check_plain', num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size
FROM _timescaledb_catalog.hypertable;
ROLLBACK;

-- a role can only write rows of tables whose owner it has the privileges of
GRANT INSERT, UPDATE ON _timescaledb_catalog.hypertable, _timescaledb_catalog.chunk, _timescaledb_catalog.compression_settings, _timescaledb_catalog.chunk_rewrite TO :ROLE_DEFAULT_PERM_USER;
INSERT INTO _timescaledb_catalog.compression_settings (relid) VALUES ('check_ht');
INSERT INTO _timescaledb_catalog.chunk_rewrite VALUES (:'CHUNK', 'check_plain');
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
UPDATE _timescaledb_catalog.hypertable SET status = 0;
UPDATE _timescaledb_catalog.chunk SET status = 0;
UPDATE _timescaledb_catalog.compression_settings SET segmentby = NULL;
UPDATE _timescaledb_catalog.chunk_rewrite SET new_relid = 'check_compressed';
\set ON_ERROR_STOP 1
RESET ROLE;
DELETE FROM _timescaledb_catalog.compression_settings;
DELETE FROM _timescaledb_catalog.chunk_rewrite;
REVOKE INSERT, UPDATE ON _timescaledb_catalog.hypertable, _timescaledb_catalog.chunk, _timescaledb_catalog.compression_settings, _timescaledb_catalog.chunk_rewrite FROM :ROLE_DEFAULT_PERM_USER;

-- associated schema
CREATE TABLE check_assoc(time timestamptz NOT NULL);
SELECT FROM create_hypertable('check_assoc', 'time');
ALTER TABLE check_assoc OWNER TO :ROLE_DEFAULT_PERM_USER;
SELECT id AS assoc_ht FROM _timescaledb_catalog.hypertable WHERE table_name = 'check_assoc' \gset
CREATE SCHEMA check_schema;
CREATE SCHEMA check_schema_owned AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
GRANT UPDATE ON _timescaledb_catalog.hypertable TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;
UPDATE _timescaledb_catalog.hypertable SET associated_schema_name = 'check_schema_owned' WHERE id = :assoc_ht;
\set ON_ERROR_STOP 0
UPDATE _timescaledb_catalog.hypertable SET associated_schema_name = 'check_missing' WHERE id = :assoc_ht;
UPDATE _timescaledb_catalog.hypertable SET associated_schema_name = 'check_schema' WHERE id = :assoc_ht;
UPDATE _timescaledb_catalog.hypertable SET associated_schema_name = 'pg_catalog' WHERE id = :assoc_ht;
\set ON_ERROR_STOP 1
UPDATE _timescaledb_catalog.hypertable SET associated_schema_name = '_timescaledb_internal' WHERE id = :assoc_ht;
RESET ROLE;
REVOKE UPDATE ON _timescaledb_catalog.hypertable FROM :ROLE_DEFAULT_PERM_USER;

-- a role that can only SET ROLE to the owner is accepted
BEGIN;
CREATE ROLE check_set_owner;
CREATE TABLE check_set(a int);
ALTER TABLE check_set OWNER TO check_set_owner;
GRANT check_set_owner TO :ROLE_DEFAULT_PERM_USER WITH INHERIT FALSE, SET TRUE;
SET LOCAL ROLE :ROLE_DEFAULT_PERM_USER;
SELECT pg_has_role('check_set_owner', 'USAGE');
SELECT _timescaledb_functions.compression_settings_validate('check_set', NULL);
ROLLBACK;

-- jobs
CREATE PROCEDURE check_job(job_id int, config jsonb) LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE FUNCTION check_job_config(config jsonb) RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE PROCEDURE check_job_config_proc(config jsonb) LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE FUNCTION check_job_sfunc(int, int, jsonb) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE AGGREGATE check_job_agg(int, jsonb) (SFUNC = check_job_sfunc, STYPE = int);
CREATE ROLE check_job_owner;
GRANT INSERT, UPDATE ON _timescaledb_catalog.bgw_job TO :ROLE_DEFAULT_PERM_USER;
GRANT USAGE ON SEQUENCE _timescaledb_catalog.bgw_job_id_seq TO :ROLE_DEFAULT_PERM_USER;

SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.bgw_job'::regclass AND contype = 'c';

-- valid values are accepted
INSERT INTO _timescaledb_catalog.bgw_job (application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, check_schema, check_name)
VALUES ('check_job', '1h', '1h', 0, '1h', 'public', 'check_job', :'ROLE_DEFAULT_PERM_USER', 'public', 'check_job_config') RETURNING id AS job_id \gset
-- a job procedure or check function that does not exist is accepted
BEGIN;
UPDATE _timescaledb_catalog.bgw_job SET proc_name = 'check_missing', check_name = 'check_missing' WHERE id = :job_id;
ROLLBACK;

\set ON_ERROR_STOP 0
-- invalid job owner
UPDATE _timescaledb_catalog.bgw_job SET owner = 4000000000::oid::regrole WHERE id = :job_id;
-- invalid job procedure
UPDATE _timescaledb_catalog.bgw_job SET proc_name = 'check_job_agg' WHERE id = :job_id;
-- invalid check functions
UPDATE _timescaledb_catalog.bgw_job SET check_name = 'check_job_config_proc' WHERE id = :job_id;
UPDATE _timescaledb_catalog.bgw_job SET check_name = NULL WHERE id = :job_id;
UPDATE _timescaledb_catalog.bgw_job SET check_schema = NULL WHERE id = :job_id;
-- a role can only write jobs of owners it has the privileges of
SET ROLE :ROLE_DEFAULT_PERM_USER;
UPDATE _timescaledb_catalog.bgw_job SET owner = :'ROLE_SUPERUSER' WHERE id = :job_id;
INSERT INTO _timescaledb_catalog.bgw_job (application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner)
VALUES ('check_job', '1h', '1h', 0, '1h', 'public', 'check_job', :'ROLE_SUPERUSER');
UPDATE _timescaledb_catalog.bgw_job SET owner = 'check_job_owner' WHERE id = :job_id;
\set ON_ERROR_STOP 1
UPDATE _timescaledb_catalog.bgw_job SET scheduled = false WHERE id = :job_id;
RESET ROLE;

-- the database owner can only write jobs of owners it has the privileges of
SELECT datdba::regrole AS db_owner FROM pg_database WHERE datname = current_database() \gset
ALTER DATABASE :TEST_DBNAME OWNER TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
UPDATE _timescaledb_catalog.bgw_job SET owner = 'check_job_owner' WHERE id = :job_id;
\set ON_ERROR_STOP 1
RESET ROLE;
GRANT check_job_owner TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;
UPDATE _timescaledb_catalog.bgw_job SET owner = 'check_job_owner' WHERE id = :job_id;
RESET ROLE;
REVOKE check_job_owner FROM :ROLE_DEFAULT_PERM_USER;
ALTER DATABASE :TEST_DBNAME OWNER TO :db_owner;

DELETE FROM _timescaledb_catalog.bgw_job WHERE id = :job_id;
DROP ROLE check_job_owner;

-- dimensions
CREATE TABLE check_dim(time bigint NOT NULL, device int, value int);
SELECT FROM create_hypertable('check_dim', 'time', 'device', 2, chunk_time_interval => 10);
ALTER TABLE check_dim OWNER TO :ROLE_DEFAULT_PERM_USER;
SELECT id AS dim_ht FROM _timescaledb_catalog.hypertable WHERE table_name = 'check_dim' \gset
CREATE FUNCTION check_part(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1';
CREATE FUNCTION check_part_volatile(int) RETURNS int LANGUAGE sql AS 'SELECT $1';
CREATE FUNCTION check_part_revoked(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1';
CREATE FUNCTION check_now() RETURNS bigint LANGUAGE sql STABLE AS 'SELECT 1::bigint';
CREATE FUNCTION check_now_revoked() RETURNS bigint LANGUAGE sql STABLE AS 'SELECT 1::bigint';
REVOKE EXECUTE ON FUNCTION check_part_revoked(int), check_now_revoked() FROM PUBLIC;
GRANT UPDATE ON _timescaledb_catalog.dimension TO :ROLE_DEFAULT_PERM_USER;

SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.dimension'::regclass AND conname = 'dimension_validate_check';

-- valid values are accepted
BEGIN;
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part' WHERE hypertable_id = :dim_ht AND column_name = 'device';
UPDATE _timescaledb_catalog.dimension SET integer_now_func_schema = 'public', integer_now_func = 'check_now' WHERE hypertable_id = :dim_ht AND column_name = 'time';
-- an integer now function that does not exist is accepted
UPDATE _timescaledb_catalog.dimension SET integer_now_func_schema = 'public', integer_now_func = 'check_missing' WHERE hypertable_id = :dim_ht AND column_name = 'time';
ROLLBACK;

\set ON_ERROR_STOP 0
-- invalid column
UPDATE _timescaledb_catalog.dimension SET column_name = 'check_missing' WHERE hypertable_id = :dim_ht AND column_name = 'device';
-- invalid partitioning functions
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_missing' WHERE hypertable_id = :dim_ht AND column_name = 'device';
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_volatile' WHERE hypertable_id = :dim_ht AND column_name = 'device';
-- functions that the table owner cannot execute
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_revoked' WHERE hypertable_id = :dim_ht AND column_name = 'device';
UPDATE _timescaledb_catalog.dimension SET integer_now_func_schema = 'public', integer_now_func = 'check_now_revoked' WHERE hypertable_id = :dim_ht AND column_name = 'time';
-- a role can only write dimensions of tables whose owner it has the privileges of
SET ROLE :ROLE_DEFAULT_PERM_USER;
UPDATE _timescaledb_catalog.dimension SET aligned = true WHERE hypertable_id <> :dim_ht;
\set ON_ERROR_STOP 1
UPDATE _timescaledb_catalog.dimension SET aligned = true WHERE hypertable_id = :dim_ht;
RESET ROLE;

-- partitioning functions must match the column type and the dimension type
CREATE FUNCTION check_part_text(text) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 1';
CREATE FUNCTION check_part_bigint(int) RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT $1::bigint';
CREATE FUNCTION check_part_bool(bigint) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT true';
CREATE FUNCTION check_part_time(bigint) RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT $1';
\set ON_ERROR_STOP 0
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_text' WHERE hypertable_id = :dim_ht AND column_name = 'device';
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_bigint' WHERE hypertable_id = :dim_ht AND column_name = 'device';
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_bool' WHERE hypertable_id = :dim_ht AND column_name = 'time';
\set ON_ERROR_STOP 1
BEGIN;
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part_time' WHERE hypertable_id = :dim_ht AND column_name = 'time';
ROLLBACK;

-- an overload that does not match is ignored
CREATE FUNCTION check_part(text) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 1';
REVOKE EXECUTE ON FUNCTION check_part(text) FROM PUBLIC;
BEGIN;
UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = 'public', partitioning_func = 'check_part' WHERE hypertable_id = :dim_ht AND column_name = 'device';
ROLLBACK;

-- the column type must match the type of the column
\set ON_ERROR_STOP 0
UPDATE _timescaledb_catalog.dimension SET column_type = 'uuid' WHERE hypertable_id = :dim_ht AND column_name = 'time';
\set ON_ERROR_STOP 1
