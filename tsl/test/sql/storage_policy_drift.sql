-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
\set VERBOSITY terse
SET client_min_messages TO warning;

CREATE SCHEMA drift;
CREATE SCHEMA drift_other;

-- how the spelling of a policy compares with what the hypertable reports
SELECT add_storage_policy('sp_plain', 'drift', 'sp_plain', '{
    "partition_column": "ts",
    "columnstore": {"segmentby": "a, b", "orderby": "ts"},
    "compression": {"mode": "AFTER", "after": "1 week"}
}');
SELECT add_storage_policy('sp_desc', 'drift', 'sp_desc', '{
    "partition_column": "ts",
    "columnstore": {"segmentby": "A", "orderby": "ts DESC NULLS LAST"}
}');
SELECT add_storage_policy('sp_asc', 'drift', 'sp_asc', '{
    "partition_column": "ts",
    "columnstore": {"orderby": "ts ASC, b DESC"}
}');
CREATE TABLE drift.sp_plain (ts timestamptz NOT NULL, a int, b int);
CREATE TABLE drift.sp_desc (ts timestamptz NOT NULL, a int, b int);
CREATE TABLE drift.sp_asc (ts timestamptz NOT NULL, a int, b int);
SELECT hypertable_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('drift', only_drift => false)
ORDER BY 1, 2;

-- NEVER means no compression job, and a job that is added by hand is drift
SELECT add_storage_policy('never', 'drift', 'nv', '{
    "partition_column": "ts",
    "columnstore": {"segmentby": "a"},
    "compression": {"mode": "NEVER"}
}');
CREATE TABLE drift.nv (ts timestamptz NOT NULL, a int);
SELECT hypertable_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('drift', 'nv', only_drift => false) WHERE setting = 'tsdb.compress_after';
SELECT add_compression_policy('drift.nv', interval '2 days') IS NOT NULL AS added;
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('drift', 'nv');

-- removing the jobs, and a table that never had columnstore settings
SELECT add_storage_policy('full', 'drift', 'fl', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "a", "orderby": "ts DESC"},
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "90 days"}
}');
CREATE TABLE drift.fl (ts timestamptz NOT NULL, a int);
SELECT remove_retention_policy('drift.fl');
SELECT remove_compression_policy('drift.fl');
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('drift', 'fl') ORDER BY 2;
SELECT set_chunk_time_interval('drift.fl', interval '1 day');
ALTER TABLE drift.fl SET (timescaledb.compress = false);
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('drift', 'fl') ORDER BY 2;

-- an integer time column
SELECT add_storage_policy('int', 'drift', 'it', '{"partition_column": "t", "chunk_interval": "100000"}');
CREATE TABLE drift.it (t bigint NOT NULL, a int);
SELECT hypertable_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('drift', 'it', only_drift => false);

-- scope: a schema, a table, a name that is not a hypertable, and a missing one
CREATE TABLE drift_other.sp_plain (ts timestamptz NOT NULL, a int, b int);
SELECT count(*) FROM check_storage_policy_drift('drift_other');
SELECT count(DISTINCT hypertable_name) > 0 AS drift_schema_has_rows
FROM check_storage_policy_drift('drift') WHERE hypertable_schema = 'drift';
SELECT count(*) FROM check_storage_policy_drift('drift') WHERE hypertable_schema <> 'drift';
SELECT count(*) FROM check_storage_policy_drift('drift', 'no_such_table');
CREATE TABLE drift_other.plain_table (ts timestamptz NOT NULL);
SELECT count(*) FROM check_storage_policy_drift('drift_other', 'plain_table', only_drift => false);
-- only a table name
SELECT DISTINCT hypertable_schema FROM check_storage_policy_drift(table_name => 'sp_plain', only_drift => false);

-- the exact policy is the expectation when a regex policy also matches
SELECT add_storage_policy('wide', 'drift', 'pr_.*', '{"partition_column": "ts", "chunk_interval": "1 week"}', 'regex', 50);
SELECT add_storage_policy('narrow', 'drift', 'pr_a', '{"partition_column": "ts", "chunk_interval": "2 days"}');
CREATE TABLE drift.pr_a (ts timestamptz NOT NULL);
CREATE TABLE drift.pr_b (ts timestamptz NOT NULL);
SELECT hypertable_name, policy_name, setting, expected, actual
FROM check_storage_policy_drift('drift', only_drift => false)
WHERE hypertable_name LIKE 'pr\_%' AND setting = 'tsdb.chunk_interval' ORDER BY 1;
-- after the narrow policy goes the regex one is the expectation
SELECT remove_storage_policy('narrow');
SELECT hypertable_name, policy_name, expected, actual FROM check_storage_policy_drift('drift', 'pr_a');
-- a disabled policy is not an expectation
SELECT alter_storage_policy('wide', enabled => false);
SELECT count(*) FROM check_storage_policy_drift('drift', 'pr_a', only_drift => false);
SELECT alter_storage_policy('wide', enabled => true);

-- a space dimension added by hand does not change what the partition column is compared with
SELECT add_storage_policy('sp2', 'drift', 'sp2', '{"partition_column": "ts", "chunk_interval": "3 days"}');
CREATE TABLE drift.sp2 (ts timestamptz NOT NULL, dev int);
SELECT created FROM add_dimension('drift.sp2', 'dev', number_partitions => 2);
SELECT hypertable_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('drift', 'sp2', only_drift => false) ORDER BY 2;

-- a name that needs quoting
SELECT add_storage_policy('quoted', 'drift', 'Mixed Case', '{"partition_column": "ts", "chunk_interval": "1 day"}');
CREATE TABLE drift."Mixed Case" (ts timestamptz NOT NULL);
SELECT hypertable_name, setting, in_sync FROM check_storage_policy_drift('drift', 'Mixed Case', only_drift => false) ORDER BY 2;

-- a role that may not manage policies can still ask
CREATE ROLE drift_reader LOGIN;
GRANT USAGE ON SCHEMA drift TO drift_reader;
SET ROLE drift_reader;
SELECT count(*) > 0 AS reader_sees_rows FROM check_storage_policy_drift('drift', only_drift => false);
RESET ROLE;
DROP OWNED BY drift_reader;
DROP ROLE drift_reader;

-- retention NEVER is compared too: a retention policy added by hand is drift, and apply removes it
SELECT add_storage_policy('no_ret', 'drift', 'no_ret', '{"partition_column": "ts", "retention": {"mode": "NEVER"}}');
CREATE TABLE drift.no_ret (ts timestamptz NOT NULL);
SELECT setting, value, source FROM explain_storage_policy('drift', 'no_ret') WHERE setting = 'retention_after';
SELECT hypertable_name, setting, expected FROM check_storage_policy_drift('drift', 'no_ret');
SELECT add_retention_policy('drift.no_ret', INTERVAL '30 days') IS NOT NULL AS added;
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('drift', 'no_ret');
SELECT * FROM apply_storage_policy('drift', 'no_ret', dry_run => true);
SELECT count(*) AS jobs FROM timescaledb_information.jobs WHERE hypertable_name = 'no_ret' AND proc_name = 'policy_retention';
-- removing a policy deletes nothing, so allow_retention is not needed
SELECT * FROM apply_storage_policy('drift', 'no_ret');
SELECT count(*) AS jobs FROM timescaledb_information.jobs WHERE hypertable_name = 'no_ret' AND proc_name = 'policy_retention';
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('drift', 'no_ret');
SELECT remove_storage_policy('no_ret');

-- the time column is added to the order when a list leaves it out, so that is not drift
SELECT add_storage_policy('ob', 'drift', 'ob_.*', '{"partition_column": "ts", "columnstore": {"segmentby": "dev", "orderby": "v"}}', match_kind => 'regex', priority => 901);
CREATE TABLE drift.ob_1 (ts timestamptz NOT NULL, dev int, v int);
SELECT orderby FROM timescaledb_information.hypertable_columnstore_settings WHERE hypertable = 'drift.ob_1'::regclass;
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('drift', 'ob_1');
SELECT remove_storage_policy('ob');
-- spellings of the same list: the defaults of a direction are not a difference, a quoted name keeps its case
SELECT s, _timescaledb_functions.storage_policy_normalize_columns(s) AS normalised FROM (VALUES
    ('ts DESC NULLS FIRST'), ('ts ASC NULLS LAST'), ('ts ASC NULLS FIRST'), ('ts DESC NULLS LAST'),
    ('TS  ,  Dev DESC'), ('"Dev", dev'), (NULL)) v(s);

-- plain tables that a policy matches: made before the policy, or opted out with
-- tsdb.hypertable = false, and so not hypertables
CREATE TABLE drift.pl_old (ts timestamptz NOT NULL, v int);
INSERT INTO drift.pl_old VALUES (now(), 1);
SELECT add_storage_policy('pl', 'drift', 'pl_.*', '{"partition_column": "ts", "chunk_interval": "1 day"}', match_kind => 'regex', priority => 900);
CREATE TABLE drift.pl_optout (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
CREATE TABLE drift.pl_new (ts timestamptz NOT NULL);
CREATE UNLOGGED TABLE drift.pl_unlogged (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
-- a plain table no policy matches, a partition, a child and a temporary table are not listed
CREATE TABLE drift.other_plain (ts timestamptz NOT NULL);
CREATE TABLE drift.pl_part (ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
CREATE TABLE drift.pl_part_1 PARTITION OF drift.pl_part FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TEMP TABLE pl_temp (ts timestamptz NOT NULL);
CREATE TABLE drift.pl_child () INHERITS (drift.other_plain);
SELECT hypertable_name, policy_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('drift', include_plain => true)
WHERE hypertable_name LIKE 'pl\_%' OR hypertable_name = 'other_plain'
ORDER BY 1, 3;
-- off by default
SELECT count(*) AS plain_rows_by_default FROM check_storage_policy_drift('drift')
WHERE hypertable_name LIKE 'pl\_%' OR hypertable_name = 'other_plain';
-- apply agrees: the empty one converts and the one with rows is an error
SELECT * FROM apply_storage_policy('drift', 'pl_optout', dry_run => true);
\set ON_ERROR_STOP 0
SELECT * FROM apply_storage_policy('drift', 'pl_old', dry_run => true);
\set ON_ERROR_STOP 1
SELECT * FROM apply_storage_policy('drift', 'pl_optout');
SELECT hypertable_name, setting FROM check_storage_policy_drift('drift', 'pl_optout', include_plain => true);
SELECT remove_storage_policy('pl');

-- an unlogged table gets a policy too, and so do its chunks
SELECT add_storage_policy('ul', 'drift', 'ul_1', '{"partition_column": "ts"}');
CREATE UNLOGGED TABLE drift.ul_1 (ts timestamptz NOT NULL);
INSERT INTO drift.ul_1 VALUES (now());
SELECT c.relpersistence FROM show_chunks('drift.ul_1') ch JOIN pg_class c ON c.oid = ch;
SELECT remove_storage_policy('ul');

-- generate_storage_policy describes a table, and a policy made from it shows no drift
SELECT add_storage_policy('gen_src', 'drift', 'gen_1', '{
  "partition_column": "ts", "chunk_interval": "2 days",
  "columnstore": {"segmentby": "device", "orderby": "ts DESC"},
  "compression": {"mode": "AFTER", "after": "14 days"},
  "retention": {"mode": "AFTER", "after": "90 days"}}');
CREATE TABLE drift.gen_1 (ts timestamptz NOT NULL, device int, v float8);
SELECT remove_storage_policy('gen_src');
SELECT generate_storage_policy('drift', 'gen_1');
SELECT add_storage_policy('gen_copy', 'drift', 'gen_1', generate_storage_policy('drift', 'gen_1'));
SELECT count(*) AS drift_rows FROM check_storage_policy_drift('drift', 'gen_1');
SELECT remove_storage_policy('gen_copy');
-- a table with no compression or retention job names neither
CREATE TABLE drift.gen_2 (ts timestamptz NOT NULL, v float8) WITH (tsdb.hypertable, tsdb.partition_column = 'ts');
SELECT remove_compression_policy('drift.gen_2');
SELECT generate_storage_policy('drift', 'gen_2');
-- a plain table has nothing to describe
CREATE TABLE drift.gen_plain (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
\set ON_ERROR_STOP 0
SELECT generate_storage_policy('drift', 'gen_plain');
\set ON_ERROR_STOP 1

DROP SCHEMA drift CASCADE;
DROP SCHEMA drift_other CASCADE;
SELECT remove_storage_policy(name) FROM timescaledb_information.storage_policies WHERE name IN
  ('sp_plain', 'sp_desc', 'sp_asc', 'never', 'full', 'int', 'wide', 'sp2', 'quoted', 'pl');
