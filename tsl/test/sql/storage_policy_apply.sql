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
SELECT remove_storage_policy('ap');
