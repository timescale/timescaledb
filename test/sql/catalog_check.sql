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
