-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
\set VERBOSITY terse
SET client_min_messages TO warning;

CREATE SCHEMA ai;

SELECT add_storage_policy('ai', 'ai', 'ai_.*', '{"partition_column": "t", "chunk_interval": "10000"}', 'regex', 10);
-- made under the policy, in three integer widths
CREATE TABLE ai.ai_big (t bigint NOT NULL, a int);
CREATE TABLE ai.ai_int (t int NOT NULL, a int);
CREATE TABLE ai.ai_small (t smallint NOT NULL, a int);
SELECT hypertable_name, time_interval, integer_interval FROM timescaledb_information.dimensions
WHERE hypertable_schema = 'ai' ORDER BY 1;
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('ai');

-- the interval changes in the policy
SELECT alter_storage_policy('ai', policy => '{"partition_column": "t", "chunk_interval": "20000"}');
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('ai') ORDER BY 1, 2;
SELECT * FROM apply_storage_policy('ai', 'ai_big', dry_run => true);
SELECT * FROM apply_storage_policy('ai', 'ai_big');
SELECT * FROM apply_storage_policy('ai', 'ai_int');
SELECT * FROM apply_storage_policy('ai', 'ai_small');
SELECT hypertable_name, integer_interval FROM timescaledb_information.dimensions WHERE hypertable_schema = 'ai' ORDER BY 1;
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('ai', 'ai_big');

-- a plain empty table with an integer time column is converted
SET timescaledb.enable_storage_policies = off;
CREATE TABLE ai.ai_plain (t bigint NOT NULL, a int);
RESET timescaledb.enable_storage_policies;
SELECT * FROM apply_storage_policy('ai', 'ai_plain', dry_run => true);
SELECT * FROM apply_storage_policy('ai', 'ai_plain');
SELECT integer_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'ai_plain';
-- a policy with no chunk interval
SELECT alter_storage_policy('ai', policy => '{"partition_column": "t"}');
SET timescaledb.enable_storage_policies = off;
CREATE TABLE ai.ai_default (t bigint NOT NULL);
RESET timescaledb.enable_storage_policies;
SELECT * FROM apply_storage_policy('ai', 'ai_default');
SELECT integer_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'ai_default';

-- an interval that does not fit the column type fails when the table is created, naming the policy
SELECT add_storage_policy('too_big', 'ai', 'sm_.*', '{"partition_column": "t", "chunk_interval": "100000"}', 'regex', 20);
\set ON_ERROR_STOP 0
CREATE TABLE ai.sm_a (t smallint NOT NULL);
\set ON_ERROR_STOP 1
SELECT count(*) FROM pg_class WHERE relname = 'sm_a';
SELECT remove_storage_policy('too_big');

-- compress_after is an age, which an integer time column has no way to express, as the compression
-- policy there takes an integer and not an interval. Applying it fails and changes nothing, and so
-- does creating a table. The columnstore settings alone are fine.
SELECT alter_storage_policy('ai', policy => '{
    "partition_column": "t",
    "chunk_interval": "20000",
    "columnstore": {"segmentby": "a"},
    "compression": {"mode": "AFTER", "after": "7 days"}
}');
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('ai', 'ai_big');
CREATE TABLE ai.ai_new (t bigint NOT NULL, a int);
\set ON_ERROR_STOP 1
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('ai', 'ai_big');
SELECT alter_storage_policy('ai', policy => '{
    "partition_column": "t",
    "chunk_interval": "20000",
    "columnstore": {"segmentby": "a", "orderby": "t DESC"}
}');
SELECT * FROM apply_storage_policy('ai', 'ai_big');
SELECT segmentby, orderby FROM timescaledb_information.hypertable_columnstore_settings WHERE hypertable = 'ai.ai_big'::regclass;
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('ai', 'ai_big');

DROP SCHEMA ai CASCADE;
SELECT remove_storage_policy('ai');
