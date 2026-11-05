-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SHOW timescaledb.license;

CREATE SCHEMA fhr;

-- the Apache license has no columnstore or compression, so a policy that sets them is rejected
-- when it is stored, as the CREATE TABLE it would apply to must not fail halfway
\set ON_ERROR_STOP 0
\set VERBOSITY default
\set SHOW_CONTEXT never
SELECT add_storage_policy('apache1', 'fhr', 'a1', '{"columnstore": {"segmentby": "device"}}');
SELECT add_storage_policy('apache2', 'fhr', 'a2', '{"compression": {"mode": "AFTER", "after": "7 days"}}');
SELECT add_storage_policy('apache3', 'fhr', 'a3', '{"compression": {"mode": "NEVER"}}');
SELECT add_storage_policy('apache4', 'fhr', 'a5', '{"retention": {"mode": "AFTER", "after": "2 years"}}');
\set SHOW_CONTEXT errors
\set VERBOSITY terse
\set ON_ERROR_STOP 1
SELECT count(*) FROM _timescaledb_catalog.storage_policy;

-- a policy that only creates a hypertable is accepted and applied
SELECT add_storage_policy('hypertable_only', 'fhr', 'a4', '{"partition_column": "ts", "chunk_interval": "1 day"}');
CREATE TABLE fhr.a4 (ts timestamptz NOT NULL, v int);
SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_name = 'a4';
SELECT remove_storage_policy('hypertable_only');

-- the view is available without the Timescale license
SELECT add_storage_policy('listed', 'fhr', 'a6', '{"partition_column": "ts"}');
SELECT name, schema_name, table_name, match_kind, priority, enabled FROM timescaledb_information.storage_policies;
SELECT remove_storage_policy('listed');
