-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE SCHEMA fhr;

SELECT add_storage_policy('gas', 'fhr', 'g_data_site42', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "sensor_id", "orderby": "ts DESC"},
    "compression": {"mode": "AFTER", "after": "7 days"}
}');

-- plain DDL without any WITH clause, as a pipeline or ORM would issue it
CREATE TABLE IF NOT EXISTS fhr.g_data_site42 (ts timestamptz NOT NULL, sensor_id int, value float8);
SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables
WHERE hypertable_name = 'g_data_site42';
SELECT config FROM timescaledb_information.jobs
WHERE hypertable_name = 'g_data_site42' AND proc_name = 'policy_compression';
SELECT segmentby, orderby FROM timescaledb_information.hypertable_columnstore_settings
WHERE hypertable = 'fhr.g_data_site42'::regclass;

-- an existing table is left alone
CREATE TABLE IF NOT EXISTS fhr.g_data_site42 (ts timestamptz NOT NULL, sensor_id int, value float8);

-- a name that matches no policy gives a plain table
CREATE TABLE fhr.other (ts timestamptz NOT NULL);
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'other';

-- options in the statement override the policy, also when spelled with an alias
SELECT add_storage_policy('override', 'fhr', 't2', '{
    "partition_column": "ts",
    "columnstore": {"segmentby": "sensor_id"},
    "compression": {"mode": "AFTER", "after": "7 days"}
}');
CREATE TABLE fhr.t2 (ts timestamptz NOT NULL, sensor_id int, dev int)
WITH (tsdb.segment_by = 'dev', tsdb.compress_after = '3 days');
SELECT config FROM timescaledb_information.jobs
WHERE hypertable_name = 't2' AND proc_name = 'policy_compression';
SELECT segmentby FROM timescaledb_information.hypertable_columnstore_settings
WHERE hypertable = 'fhr.t2'::regclass;

-- opting out
SELECT add_storage_policy('optout', 'fhr', 't3', '{"partition_column": "ts"}');
CREATE TABLE fhr.t3 (ts timestamptz NOT NULL) WITH (tsdb.hypertable = false);
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 't3';

-- the GUC turns policies off
SELECT add_storage_policy('guc', 'fhr', 't4', '{"partition_column": "ts"}');
SET timescaledb.enable_storage_policies = off;
CREATE TABLE fhr.t4 (ts timestamptz NOT NULL);
RESET timescaledb.enable_storage_policies;
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 't4';
DROP TABLE fhr.t4;
CREATE TABLE fhr.t4 (ts timestamptz NOT NULL);
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 't4';

-- a disabled policy does not apply
SELECT add_storage_policy('disabled', 'fhr', 't5', '{"partition_column": "ts"}', enabled => false);
CREATE TABLE fhr.t5 (ts timestamptz NOT NULL);
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 't5';
SELECT alter_storage_policy('disabled', enabled => true);
DROP TABLE fhr.t5;
CREATE TABLE fhr.t5 (ts timestamptz NOT NULL);
SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 't5';

-- a table without a time column cannot get the policy
SELECT add_storage_policy('notime', 'fhr', 'nots', '{"chunk_interval": "1 day"}');
\set ON_ERROR_STOP 0
\set VERBOSITY default
CREATE TABLE fhr.nots (a int, b text);
\set VERBOSITY terse
\set ON_ERROR_STOP 1
SELECT count(*) FROM pg_class WHERE relname = 'nots';

-- the error names the policy, whatever the reason the policy cannot be applied
SELECT add_storage_policy('badcol', 'fhr', 'nocol', '{"partition_column": "ts"}');
SELECT add_storage_policy('badseg', 'fhr', 'noseg', '{"partition_column": "ts", "columnstore": {"segmentby": "device"}}');
SELECT add_storage_policy('badpk', 'fhr', 'badpk', '{"partition_column": "ts"}');
\set ON_ERROR_STOP 0
\set VERBOSITY default
CREATE TABLE fhr.nocol (time timestamptz NOT NULL, v int);
CREATE TABLE fhr.noseg (ts timestamptz NOT NULL, v int);
CREATE TABLE fhr.badpk (ts timestamptz NOT NULL, id int PRIMARY KEY);
\set VERBOSITY terse
\set ON_ERROR_STOP 1
-- and the table is not left behind
SELECT count(*) FROM pg_class WHERE relname IN ('nots', 'nocol', 'noseg', 'badpk');
-- the opt-out named in the hint works
CREATE TABLE fhr.nocol (time timestamptz NOT NULL, v int) WITH (tsdb.hypertable = false);
DROP TABLE fhr.nocol;

\set ON_ERROR_STOP 0
-- invalid policies are rejected when they are stored
SELECT add_storage_policy('bad', 'fhr', 'x', '{"partion_column": "ts"}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"compression": {"mode": "NEVER", "after": "1 day"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"compression": {"mode": "AFTER"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"compression": {"mode": "SOMETIMES"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"tiering": {"mode": "AFTER", "after": "90 days"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"retention": {"mode": "AFTER"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"retention": {"mode": "NEVER", "after": "1 day"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"retention": {"mode": "AFTER", "after": "a week ago"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"retention": {"mode": "AFTER", "after": "0 days"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"retention": {"mode": "AFTER", "after": "-1 day"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"compression": {"mode": "AFTER", "after": "-7 days"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '{"columnstore": {"segmnetby": "a"}}');
SELECT add_storage_policy('bad', 'fhr', 'x', '"ts"');
SELECT add_storage_policy('bad', 'fhr', 'x', '{}', 'glob');
SELECT add_storage_policy('bad', 'fhr', 'x', '{}', priority => 1);
-- duplicates
SELECT add_storage_policy('gas', 'fhr', 'other_table', '{}');
SELECT add_storage_policy('gas_again', 'fhr', 'g_data_site42', '{}');
SELECT remove_storage_policy('nonexistent');
SELECT alter_storage_policy('nonexistent', enabled => true);
\set ON_ERROR_STOP 1
SELECT add_storage_policy('gas', 'fhr', 'other_table', '{}', if_not_exists => true);
SELECT remove_storage_policy('nonexistent', if_exists => true);

-- regex policies: the lowest priority value wins, an exact match beats any regex, and a regex has
-- to match the whole name
SELECT add_storage_policy('events', 'fhr', 'ev_.*', '{"partition_column": "ts", "chunk_interval": "1 day"}', 'regex', 10);
SELECT add_storage_policy('events_slow', 'fhr', 'ev_slow_.*', '{"partition_column": "ts", "chunk_interval": "1 week"}', 'regex', 5);
SELECT add_storage_policy('events_special', 'fhr', 'ev_special', '{"partition_column": "ts", "chunk_interval": "3 days"}');
SELECT add_storage_policy('audit', '.*', 'audit_log', '{"partition_column": "ts", "chunk_interval": "2 days"}', 'regex', 20);

SELECT s AS schema, n AS "table", m.name, m.match_kind, m.priority
FROM (VALUES ('fhr', 'ev_fast'), ('fhr', 'ev_slow_1'), ('fhr', 'ev_special'), ('fhr', 'my_ev_fast'),
             ('fhr', 'ev'), ('other', 'audit_log'), ('other', 'audit_log_old'),
             ('other', 'xaudit_log')) AS t(s, n)
LEFT JOIN LATERAL show_storage_policy_matches(t.s, t.n) AS m ON m.rank = 1
ORDER BY s, n;

-- all the policies a name matches, in the order of precedence: exact first, then regex by priority
SELECT add_storage_policy('events_slow_2', 'fhr', 'ev_slow_2', '{"partition_column": "ts"}');
SELECT rank, name, match_kind, priority FROM show_storage_policy_matches('fhr', 'ev_slow_2');
-- rank 1 is the policy CREATE TABLE uses, as its notice says
CREATE TABLE fhr.ev_slow_2 (ts timestamptz NOT NULL);
DROP TABLE fhr.ev_slow_2;
-- removing it promotes the next one
SELECT remove_storage_policy('events_slow_2');
SELECT rank, name, match_kind, priority FROM show_storage_policy_matches('fhr', 'ev_slow_2');
-- nothing matches
SELECT count(*) FROM show_storage_policy_matches('fhr', 'nothing_matches');

-- the view lists the policies in the order they are matched
SELECT name, schema_name, table_name, match_kind, priority, enabled
FROM timescaledb_information.storage_policies
WHERE name IN ('events', 'events_slow', 'events_special', 'audit');
SELECT name, policy->>'chunk_interval' AS chunk_interval, updated_by = current_user AS updated_by_me,
       created_at <= now() AS created_before_now
FROM timescaledb_information.storage_policies WHERE name = 'events_special';

CREATE SCHEMA other;
CREATE TABLE fhr.ev_fast (ts timestamptz NOT NULL);
CREATE TABLE fhr.ev_slow_1 (ts timestamptz NOT NULL);
CREATE TABLE fhr.ev_special (ts timestamptz NOT NULL);
CREATE TABLE fhr.my_ev_fast (ts timestamptz NOT NULL);
CREATE TABLE other.audit_log (ts timestamptz NOT NULL);
SELECT hypertable_schema, hypertable_name, time_interval FROM timescaledb_information.dimensions
WHERE hypertable_schema IN ('fhr', 'other') AND hypertable_name IN ('ev_fast', 'ev_slow_1', 'ev_special', 'my_ev_fast', 'audit_log')
ORDER BY 1, 2;

-- a disabled policy is skipped, so the next one that matches applies
SELECT alter_storage_policy('events_slow', enabled => false);
SELECT rank, name, match_kind, priority FROM show_storage_policy_matches('fhr', 'ev_slow_2');
SELECT alter_storage_policy('events_slow', enabled => true);
-- the priority can be changed, so the order of two policies can be swapped
SELECT alter_storage_policy('events', priority => 1);
SELECT rank, name, match_kind, priority FROM show_storage_policy_matches('fhr', 'ev_slow_2');
SELECT alter_storage_policy('events', priority => 10);

\set ON_ERROR_STOP 0
SELECT add_storage_policy('bad_regex', 'fhr', 'ev_(', '{"partition_column": "ts"}', 'regex', 30);
SELECT add_storage_policy('bad_schema', '[', 'x', '{"partition_column": "ts"}', 'regex', 30);
SELECT add_storage_policy('no_priority', 'fhr', 'x.*', '{"partition_column": "ts"}', 'regex');
SELECT add_storage_policy('same_priority', 'fhr', 'x.*', '{"partition_column": "ts"}', 'regex', 10);
SELECT alter_storage_policy('events', table_name => 'ev_(');
SELECT alter_storage_policy('events_special', priority => 3);
\set ON_ERROR_STOP 1
SELECT remove_storage_policy(name) FROM _timescaledb_catalog.storage_policy
WHERE name IN ('events', 'events_slow', 'events_special', 'audit');
DROP TABLE fhr.ev_fast, fhr.ev_slow_1, fhr.ev_special, fhr.my_ev_fast, other.audit_log;
DROP SCHEMA other;

-- retention
SELECT add_storage_policy('retained', 'fhr', 'ret_t', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "2 years"}
}');
CREATE TABLE fhr.ret_t (ts timestamptz NOT NULL, v int);
SELECT proc_name, config, schedule_interval FROM timescaledb_information.jobs
WHERE hypertable_name = 'ret_t' AND proc_name IN ('policy_compression', 'policy_retention') ORDER BY proc_name;

-- NEVER is the same as no retention
SELECT add_storage_policy('retain_never', 'fhr', 'ret_never', '{"partition_column": "ts", "retention": {"mode": "NEVER"}}');
CREATE TABLE fhr.ret_never (ts timestamptz NOT NULL, v int);
SELECT count(*) FROM timescaledb_information.jobs
WHERE hypertable_name = 'ret_never' AND proc_name = 'policy_retention';

-- an integer time column needs an integer_now function for a retention policy, which a table that
-- is only being created cannot have yet, so the policy cannot be applied, the same as for
-- add_retention_policy
SELECT add_storage_policy('retain_int', 'fhr', 'ret_int', '{
    "partition_column": "ts", "chunk_interval": "1000", "retention": {"mode": "AFTER", "after": "100000"}
}');
\set ON_ERROR_STOP 0
\set VERBOSITY default
CREATE TABLE fhr.ret_int (ts bigint NOT NULL, v int);
\set VERBOSITY terse
\set ON_ERROR_STOP 1
SELECT count(*) FROM pg_class WHERE relname = 'ret_int';

-- an age that does not fit the time column fails when the table is created, and no table is left
SELECT add_storage_policy('retain_wrong', 'fhr', 'ret_wrong', '{"partition_column": "ts", "retention": {"mode": "AFTER", "after": "2 years"}}');
\set ON_ERROR_STOP 0
\set VERBOSITY default
CREATE TABLE fhr.ret_wrong (ts bigint NOT NULL);
\set VERBOSITY terse
\set ON_ERROR_STOP 1
SELECT count(*) FROM pg_class WHERE relname = 'ret_wrong';

SELECT remove_storage_policy('retained');
SELECT remove_storage_policy('retain_never');
SELECT remove_storage_policy('retain_int');
SELECT remove_storage_policy('retain_wrong');
DROP TABLE fhr.ret_t, fhr.ret_never;

-- only the database owner or a superuser may manage policies
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
SELECT add_storage_policy('nope', 'fhr', 'y', '{}');
SELECT remove_storage_policy('gas');
\set ON_ERROR_STOP 1
RESET ROLE;

-- a role that owns the database without being a superuser can manage policies
CREATE ROLE storage_policy_dbowner NOSUPERUSER;
ALTER DATABASE :"TEST_DBNAME" OWNER TO storage_policy_dbowner;
SET ROLE storage_policy_dbowner;
SELECT add_storage_policy('owned', 'fhr', 'z', '{"partition_column": "ts"}');
SELECT alter_storage_policy('owned', enabled => false);
SELECT remove_storage_policy('owned');
-- and a role that is not the owner still cannot
RESET ROLE;
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
SELECT add_storage_policy('nope', 'fhr', 'y', '{}');
\set ON_ERROR_STOP 1
RESET ROLE;
ALTER DATABASE :"TEST_DBNAME" OWNER TO :ROLE_SUPERUSER;
DROP ROLE storage_policy_dbowner;

SELECT remove_storage_policy('gas');
SELECT remove_storage_policy('override');
SELECT remove_storage_policy('optout');
SELECT remove_storage_policy('guc');
SELECT remove_storage_policy('disabled');
SELECT remove_storage_policy('notime');
SELECT remove_storage_policy('badcol');
SELECT remove_storage_policy('badseg');
SELECT remove_storage_policy('badpk');

-- explain_storage_policy shows what CREATE TABLE would do, and creates nothing
SELECT add_storage_policy('ex_exact', 'fhr', 'ex_a', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "sensor_id"},
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "90 days"}
}');
SELECT add_storage_policy('ex_regex', 'fhr', 'ex_.*', '{"partition_column": "ts"}', match_kind => 'regex', priority => 7);

SELECT * FROM explain_storage_policy('fhr', 'ex_a');
-- the regex policy, with its priority
SELECT * FROM explain_storage_policy('fhr', 'ex_b');
-- statement options win, also when spelled with an alias, and are listed as such
SELECT * FROM explain_storage_policy('fhr', 'ex_a',
    jsonb_build_object('segment_by', 'dev', 'compress_after', '3 days'));
-- no match, with and without statement options
SELECT * FROM explain_storage_policy('fhr', 'nothing');
SELECT * FROM explain_storage_policy('fhr', 'nothing', jsonb_build_object('hypertable', true, 'partition_column', 'ts'));
-- the opt-out and the GUC
SELECT * FROM explain_storage_policy('fhr', 'ex_a', jsonb_build_object('hypertable', false));
SET timescaledb.enable_storage_policies = off;
SELECT * FROM explain_storage_policy('fhr', 'ex_a');
RESET timescaledb.enable_storage_policies;
-- the answer is what CREATE TABLE does
CREATE TABLE fhr.ex_a (ts timestamptz NOT NULL, sensor_id int);
SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_name IN ('ex_a', 'ex_b');
-- bad input
\set ON_ERROR_STOP 0
SELECT * FROM explain_storage_policy('fhr', 'ex_a', '[]');
SELECT * FROM explain_storage_policy('fhr', 'ex_a', jsonb_build_object('no_such_option', 1));
\set ON_ERROR_STOP 1
SELECT remove_storage_policy('ex_exact');
SELECT remove_storage_policy('ex_regex');

-- adding a policy warns when it overlaps with one of the other kind
SELECT add_storage_policy('ov_exact', 'fhr', 'ov_a', '{"partition_column": "ts"}');
-- a regex policy that covers an existing exact one: the exact policy wins
SELECT add_storage_policy('ov_regex', 'fhr', 'ov_.*', '{"partition_column": "ts"}', match_kind => 'regex', priority => 11);
-- an exact policy for a name a regex policy covers: the exact policy wins
SELECT add_storage_policy('ov_exact2', 'fhr', 'ov_b', '{"partition_column": "ts"}');
-- no overlap, and a disabled policy does not warn
SELECT add_storage_policy('ov_exact3', 'fhr', 'other_c', '{"partition_column": "ts"}');
SELECT add_storage_policy('ov_exact4', 'fhr', 'ov_d', '{"partition_column": "ts"}', enabled => false);
-- enabling it, or changing what a policy matches, warns
SELECT alter_storage_policy('ov_exact4', enabled => true);
SELECT alter_storage_policy('ov_exact3', table_name => 'ov_e');
-- changing only the policy does not
SELECT alter_storage_policy('ov_exact3', policy => '{"partition_column": "ts", "chunk_interval": "1 day"}');
SELECT remove_storage_policy('ov_exact');
SELECT remove_storage_policy('ov_exact2');
SELECT remove_storage_policy('ov_exact3');
SELECT remove_storage_policy('ov_exact4');
SELECT remove_storage_policy('ov_regex');

-- drift: existing hypertables against the policy that applies to them today
SELECT add_storage_policy('dr', 'fhr', 'dr_.*', '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "columnstore": {"segmentby": "sensor_id", "orderby": "ts DESC"},
    "compression": {"mode": "AFTER", "after": "7 days"},
    "retention": {"mode": "AFTER", "after": "90 days"}
}', 'regex', 90);
CREATE TABLE fhr.dr_ok (ts timestamptz NOT NULL, sensor_id int);
CREATE TABLE fhr.dr_manual (ts timestamptz NOT NULL, sensor_id int);
CREATE TABLE fhr.dr_explicit (ts timestamptz NOT NULL, sensor_id int, dev int)
WITH (tsdb.chunk_interval = '2 days', tsdb.segmentby = 'dev', tsdb.compress_after = '3 days');
-- a table created as the policy says is in sync, with every compared setting listed
SELECT hypertable_name, setting, expected, actual, in_sync
FROM check_storage_policy_drift('fhr', 'dr_ok', only_drift => false);
-- the tables created with their own options, which the policy does not know about
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('fhr') WHERE hypertable_name LIKE 'dr\_%';
-- manual changes
SELECT set_chunk_time_interval('fhr.dr_manual', interval '3 days');
CALL remove_columnstore_policy('fhr.dr_manual');
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('fhr', 'dr_manual');
-- editing the policy leaves the tables made under the old one behind
SELECT alter_storage_policy('dr', policy => '{
    "partition_column": "ts",
    "chunk_interval": "1 day",
    "retention": {"mode": "AFTER", "after": "30 days"}
}');
SELECT hypertable_name, setting, expected, actual FROM check_storage_policy_drift('fhr', 'dr_ok');
-- no policy matches
SELECT count(*) FROM check_storage_policy_drift('fhr', 'other');
SELECT remove_storage_policy('dr');
SELECT count(*) FROM check_storage_policy_drift() WHERE hypertable_name LIKE 'dr\_%';
