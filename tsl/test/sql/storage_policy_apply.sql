-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
\set VERBOSITY terse
SET client_min_messages TO warning;

CREATE SCHEMA ap;

-- a hypertable made under one policy, then the policy is changed
SELECT add_storage_policy('ap', 'ap', 'ap_.*', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "a", "orderby": "ts DESC"},
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "90 days"}
}', 'regex', 10);
CREATE TABLE ap.ap_drift (ts timestamptz NOT NULL, a int, b int);
SELECT alter_storage_policy('ap', policy => '{
    "partition_column": "ts",
    "chunk_interval": "2 days",
    "columnstore": {"segmentby": "b", "orderby": "ts"},
    "compression": {"mode": "AFTER", "after": "14 days"},
    "retention": {"mode": "AFTER", "after": "30 days"}
}');
SELECT setting, expected, actual FROM check_storage_policy_drift('ap', 'ap_drift') ORDER BY 1;

-- a dry run changes nothing
SELECT * FROM apply_storage_policy('ap', 'ap_drift', dry_run => true) ORDER BY 1;
SELECT count(*) AS still_drifted FROM check_storage_policy_drift('ap', 'ap_drift');

-- retention is held back without allow_retention, everything else is applied
SELECT * FROM apply_storage_policy('ap', 'ap_drift') ORDER BY 1;
SELECT setting, expected, actual FROM check_storage_policy_drift('ap', 'ap_drift');
SELECT * FROM apply_storage_policy('ap', 'ap_drift', allow_retention => true);
SELECT count(*) AS drift_left FROM check_storage_policy_drift('ap', 'ap_drift');
-- applying again has nothing to do
SELECT count(*) AS second_run_rows FROM apply_storage_policy('ap', 'ap_drift', allow_retention => true);
-- and the jobs are the ones the policy declares, without duplicates
SELECT proc_name, config->>'compress_after' AS compress_after, config->>'drop_after' AS drop_after
FROM timescaledb_information.jobs WHERE hypertable_name = 'ap_drift' ORDER BY 1;

-- NEVER removes the compression job
SELECT alter_storage_policy('ap', policy => '{
    "partition_column": "ts",
    "columnstore": {"segmentby": "b"},
    "compression": {"mode": "NEVER"}
}');
SELECT setting, expected, actual FROM check_storage_policy_drift('ap', 'ap_drift') ORDER BY 1;
SELECT setting, action FROM apply_storage_policy('ap', 'ap_drift') ORDER BY 1;
SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_name = 'ap_drift' AND proc_name = 'policy_compression';

-- a plain table: empty is converted, with the policy's settings
SELECT alter_storage_policy('ap', policy => '{
    "partition_column": "ts",
    "chunk_interval": "3 days",
    "columnstore": {"segmentby": "a"},
    "compression": {"mode": "AFTER", "after": "10 days"}
}');
CREATE TABLE ap.ap_plain (ts timestamptz NOT NULL, a int);
-- it matches the policy, so a new table is a hypertable already: make a plain one instead
DROP TABLE ap.ap_plain;
SET timescaledb.enable_storage_policies = off;
CREATE TABLE ap.ap_plain (ts timestamptz NOT NULL, a int);
RESET timescaledb.enable_storage_policies;
SELECT count(*) AS is_hypertable FROM timescaledb_information.hypertables WHERE hypertable_name = 'ap_plain';
SELECT * FROM apply_storage_policy('ap', 'ap_plain', dry_run => true) ORDER BY 1;
SELECT count(*) AS is_hypertable FROM timescaledb_information.hypertables WHERE hypertable_name = 'ap_plain';
SELECT * FROM apply_storage_policy('ap', 'ap_plain') ORDER BY 1;
SELECT count(*) AS drift_left FROM check_storage_policy_drift('ap', 'ap_plain');
SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'ap_plain';

\set ON_ERROR_STOP 0
-- a plain table with rows is not converted
SET timescaledb.enable_storage_policies = off;
CREATE TABLE ap.ap_rows (ts timestamptz NOT NULL, a int);
RESET timescaledb.enable_storage_policies;
INSERT INTO ap.ap_rows VALUES (now(), 1);
SELECT * FROM apply_storage_policy('ap', 'ap_rows');
-- no policy applies, and no such table
CREATE TABLE ap.nothing (ts timestamptz NOT NULL);
SELECT * FROM apply_storage_policy('ap', 'nothing');
SELECT * FROM apply_storage_policy('ap', 'ap_missing');
-- the partition column cannot be changed
CREATE TABLE ap.ap_col (t2 timestamptz NOT NULL, ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
SELECT created FROM create_hypertable('ap.ap_col', 't2');
SELECT * FROM apply_storage_policy('ap', 'ap_col');
\set ON_ERROR_STOP 1

-- a failure undoes the steps before it: the chunk interval is changed first and is put back
SELECT alter_storage_policy('ap', policy => '{"partition_column": "ts", "chunk_interval": "1 day"}');
CREATE TABLE ap.ap_fail (ts timestamptz NOT NULL, a int);
SELECT alter_storage_policy('ap', policy => '{
    "partition_column": "ts",
    "chunk_interval": "5 days",
    "columnstore": {"segmentby": "no_such_column"}
}');
SELECT setting, expected, actual FROM check_storage_policy_drift('ap', 'ap_fail') ORDER BY 1;
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('ap', 'ap_fail');
\set ON_ERROR_STOP 1
SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'ap_fail';

-- the caller needs the rights to change the table: it is not SECURITY DEFINER
SELECT alter_storage_policy('ap', policy => '{"partition_column": "ts", "chunk_interval": "4 days"}');
CREATE ROLE ap_other LOGIN;
GRANT USAGE ON SCHEMA ap TO ap_other;
SET ROLE ap_other;
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('ap', 'ap_drift');
\set ON_ERROR_STOP 1
RESET ROLE;
DROP OWNED BY ap_other;
DROP ROLE ap_other;

DROP SCHEMA ap CASCADE;
-- a plain table converted by apply is what CREATE TABLE would have made of it: the columnstore is
-- on and there is a compression policy at the chunk interval
SELECT remove_storage_policy('ap');
CREATE SCHEMA conv;
CREATE TABLE conv.c_plain (ts timestamptz NOT NULL, dev int);
SELECT add_storage_policy('conv', 'conv', 'c_.*', '{"partition_column": "ts", "chunk_interval": "2 days"}', match_kind => 'regex', priority => 1);
CREATE TABLE conv.c_new (ts timestamptz NOT NULL, dev int);
SELECT * FROM apply_storage_policy('conv', 'c_plain', dry_run => true);
SELECT * FROM apply_storage_policy('conv', 'c_plain');
SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'conv' ORDER BY 1;
SELECT hypertable_name, proc_name, config->>'compress_after' AS compress_after FROM timescaledb_information.jobs
WHERE hypertable_schema = 'conv' AND proc_name = 'policy_compression' ORDER BY 1;
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('conv');

-- the setting is for the tables a session creates: drift and apply, which are asked for by name,
-- still see the policy, and explain, which says what CREATE TABLE would do, does not
CREATE TABLE conv.c_guc (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
SET timescaledb.enable_storage_policies = off;
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('conv', include_plain => true);
SELECT * FROM apply_storage_policy('conv', 'c_guc', dry_run => true);
SELECT setting, value, source FROM explain_storage_policy('conv', 'c_guc');
RESET timescaledb.enable_storage_policies;
DROP TABLE conv.c_guc;

-- create_default_indexes and the columnstore index of the policy are applied, and a policy
-- that says NEVER leaves no compression policy
SELECT alter_storage_policy('conv', policy => '{
    "partition_column": "ts", "chunk_interval": "2 days", "create_default_indexes": false,
    "columnstore": {"segmentby": "dev", "orderby": "ts DESC"}, "compression": {"mode": "NEVER"}
}');
CREATE TABLE conv.c_plain2 (ts timestamptz NOT NULL, dev int) WITH (tsdb.hypertable = false);
CREATE TABLE conv.c_new2 (ts timestamptz NOT NULL, dev int);
SELECT * FROM apply_storage_policy('conv', 'c_plain2');
SELECT t.tablename, (SELECT count(*) FROM pg_indexes i WHERE i.schemaname = 'conv' AND i.tablename = t.tablename) AS indexes
FROM (VALUES ('c_new'), ('c_plain'), ('c_new2'), ('c_plain2')) t(tablename) ORDER BY 1;
SELECT hypertable, segmentby, orderby FROM timescaledb_information.hypertable_columnstore_settings
WHERE hypertable::text IN ('conv.c_new2', 'conv.c_plain2') ORDER BY 1;
SELECT count(*) AS compression_jobs FROM timescaledb_information.jobs
WHERE hypertable_schema = 'conv' AND hypertable_name IN ('c_new2', 'c_plain2') AND proc_name = 'policy_compression';

-- a dry run says the same of retention as the real run does
SELECT alter_storage_policy('conv', policy => '{"partition_column": "ts", "retention": {"mode": "AFTER", "after": "30 days"}}');
CREATE TABLE conv.c_plain3 (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
SELECT * FROM apply_storage_policy('conv', 'c_plain3', dry_run => true);
SELECT * FROM apply_storage_policy('conv', 'c_plain3');

-- and does not promise what create_hypertable would refuse: a unique index without the
-- partition column, and a type that cannot be partitioned
CREATE TABLE conv.c_plain5 (id int PRIMARY KEY, ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
CREATE TABLE conv.c_plain6 (ts text NOT NULL) WITH (tsdb.hypertable = false);
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('conv', 'c_plain5', dry_run => true);
SELECT * FROM apply_storage_policy('conv', 'c_plain5');
SELECT * FROM apply_storage_policy('conv', 'c_plain6', dry_run => true);
\set ON_ERROR_STOP 1

-- a uuid partition column takes an interval, as a time column does
SELECT add_storage_policy('conv_uuid', 'conv', 'c_uuid', '{"partition_column": "id", "chunk_interval": "1 day"}');
CREATE TABLE conv.c_uuid (id uuid NOT NULL) WITH (tsdb.hypertable = false);
SELECT * FROM apply_storage_policy('conv', 'c_uuid', dry_run => true);
SELECT * FROM apply_storage_policy('conv', 'c_uuid');
SELECT hypertable_name, time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'c_uuid';
SELECT remove_storage_policy('conv_uuid');

-- and does not promise what the caller may not do
CREATE TABLE conv.c_plain4 (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
CREATE ROLE conv_other LOGIN;
GRANT USAGE ON SCHEMA conv TO conv_other;
SET ROLE conv_other;
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('conv', 'c_plain4', dry_run => true);
\set ON_ERROR_STOP 1
RESET ROLE;
DROP OWNED BY conv_other;
DROP ROLE conv_other;
DROP SCHEMA conv CASCADE;
SELECT remove_storage_policy('conv');

-- chunks that are compressed already keep the settings they were compressed with, and the row says so
CREATE SCHEMA cc;
SELECT add_storage_policy('cc', 'cc', 'cc_.*', '{"partition_column": "ts", "columnstore": {"segmentby": "dev"}}', match_kind => 'regex', priority => 2);
CREATE TABLE cc.cc_1 (ts timestamptz NOT NULL, dev int, site int);
INSERT INTO cc.cc_1 VALUES (now(), 1, 1);
SELECT count(compress_chunk(c)) AS compressed FROM show_chunks('cc.cc_1') c;
SELECT alter_storage_policy('cc', policy => '{"partition_column": "ts", "columnstore": {"segmentby": "site"}}');
SELECT * FROM apply_storage_policy('cc', 'cc_1', dry_run => true);
SET client_min_messages TO warning;
SELECT * FROM apply_storage_policy('cc', 'cc_1');
RESET client_min_messages;
DROP SCHEMA cc CASCADE;
SELECT remove_storage_policy('cc');


