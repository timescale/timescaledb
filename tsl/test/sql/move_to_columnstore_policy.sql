-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE VIEW policy_jobs AS
SELECT h.table_name, j.proc_name, j.schedule_interval, j.config
FROM _timescaledb_catalog.hypertable h
JOIN _timescaledb_config.bgw_job j ON j.hypertable_id = h.id;

CREATE FUNCTION chunk_states(regclass) RETURNS TABLE(status text[]) LANGUAGE SQL AS $$
  SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks($1) c ORDER BY c::text;
$$;

-- Test 1 : Reject configs the check function cannot accept
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.policy_move_to_columnstore_check(NULL);
SELECT _timescaledb_functions.policy_move_to_columnstore_check('{}');
SELECT _timescaledb_functions.policy_move_to_columnstore_check('{"hypertable_id": 12345}');
\set ON_ERROR_STOP 1

-- Test 2 : Refuse to add the policy without columnstore enabled
CREATE TABLE plain(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('plain', 'time',
    chunk_time_interval => interval '1 day');

\set ON_ERROR_STOP 0
SELECT add_move_to_columnstore_policy('plain');
\set ON_ERROR_STOP 1

-- Test 3 : Add the policy and check its defaults
ALTER TABLE plain SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT add_move_to_columnstore_policy('plain') AS job_id \gset
SELECT proc_name, schedule_interval, config FROM policy_jobs WHERE table_name = 'plain';

-- Test 4 : Adding twice errors, if_not_exists downgrades it to a notice
\set ON_ERROR_STOP 0
SELECT add_move_to_columnstore_policy('plain');
\set ON_ERROR_STOP 1
SELECT add_move_to_columnstore_policy('plain', if_not_exists => true);

-- Test 5 : Remove, and removing again
SELECT remove_move_to_columnstore_policy('plain');
\set ON_ERROR_STOP 0
SELECT remove_move_to_columnstore_policy('plain');
\set ON_ERROR_STOP 1
SELECT remove_move_to_columnstore_policy('plain', if_exists => true);

-- Test 6 : Reject a negative max_chunks
\set ON_ERROR_STOP 0
SELECT add_move_to_columnstore_policy('plain', max_chunks => -1);
\set ON_ERROR_STOP 1

-- Test 7 : Non-default arguments are recorded in the config
SELECT add_move_to_columnstore_policy('plain',
    schedule_interval => INTERVAL '30 minutes',
    max_chunks => 2,
    allow_blocking_compression => true) AS job_id \gset
SELECT proc_name, schedule_interval, config FROM policy_jobs WHERE table_name = 'plain';
SELECT remove_move_to_columnstore_policy('plain');

DROP TABLE plain;

-- Test 8 : The policy replaces an existing columnstore policy
CREATE TABLE metrics(time timestamptz NOT NULL, device int, value float8);
SELECT table_name FROM create_hypertable('metrics', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT add_compression_policy('metrics', INTERVAL '7 days') AS unused \gset
SELECT add_move_to_columnstore_policy('metrics') AS job_id \gset
SELECT proc_name FROM policy_jobs WHERE table_name = 'metrics' ORDER BY proc_name;

-- Test 9 : Run the policy over several chunks
INSERT INTO metrics SELECT '2026-01-01'::timestamptz + (i || 'h')::interval, i % 5, i
  FROM generate_series(1, 72) i;
SELECT * FROM chunk_states('metrics');
CALL run_job(:job_id);
SELECT * FROM chunk_states('metrics');
SELECT count(*) AS total_rows FROM metrics;

-- Test 10 : A second run is a no-op
-- a recompress would clear UNORDERED, so the chunks still carrying it is
-- enough to show the run left them alone
CALL run_job(:job_id);
SELECT * FROM chunk_states('metrics');
SELECT count(*) AS total_rows FROM metrics;

-- Test 11 : A partial chunk is picked up again
INSERT INTO metrics SELECT '2026-01-01 00:30'::timestamptz + (i || 's')::interval, i % 5, i
  FROM generate_series(1, 20) i;
SELECT * FROM chunk_states('metrics');
CALL run_job(:job_id);
SELECT * FROM chunk_states('metrics');

-- Test 12 : max_chunks bounds a run
SELECT remove_move_to_columnstore_policy('metrics');
INSERT INTO metrics SELECT '2026-02-01'::timestamptz + (i || 'h')::interval, i % 5, i
  FROM generate_series(1, 72) i;
SELECT add_move_to_columnstore_policy('metrics', max_chunks => 1) AS capped \gset
SELECT count(*) AS uncompressed_before FROM show_chunks('metrics') c
  WHERE NOT ('COMPRESSED' = ANY(_timescaledb_functions.chunk_status_text(c)));
CALL run_job(:capped);
SELECT count(*) AS uncompressed_after_one_run FROM show_chunks('metrics') c
  WHERE NOT ('COMPRESSED' = ANY(_timescaledb_functions.chunk_status_text(c)));

-- Test 13 : Frozen chunks are left alone
SELECT remove_move_to_columnstore_policy('metrics');
DROP TABLE metrics;

CREATE TABLE frz(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('frz', 'time',
    chunk_time_interval => interval '100 years');
ALTER TABLE frz SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO frz SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;
SELECT _timescaledb_functions.freeze_chunk(c) FROM show_chunks('frz') c;
SELECT add_move_to_columnstore_policy('frz') AS frozen_job \gset
CALL run_job(:frozen_job);
SELECT * FROM chunk_states('frz');
SELECT _timescaledb_functions.unfreeze_chunk(c) FROM show_chunks('frz') c;
DROP TABLE frz;

-- Test 14 : A chunk the move refuses is skipped, unless blocking is allowed
CREATE TABLE uniq(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('uniq', 'time',
    chunk_time_interval => interval '100 years');
CREATE UNIQUE INDEX ON uniq(device, time);
ALTER TABLE uniq SET (timescaledb.compress, timescaledb.compress_segmentby='device');
INSERT INTO uniq SELECT '2026-01-01'::timestamptz + (i || 's')::interval, i
  FROM generate_series(1, 10) i;

SELECT add_move_to_columnstore_policy('uniq') AS skip_job \gset
CALL run_job(:skip_job);
SELECT * FROM chunk_states('uniq');

SELECT remove_move_to_columnstore_policy('uniq');
SELECT add_move_to_columnstore_policy('uniq', allow_blocking_compression => true) AS block_job \gset
CALL run_job(:block_job);
SELECT * FROM chunk_states('uniq');
SELECT count(*) AS total_rows FROM uniq;
DROP TABLE uniq;

-- Test 15 : Refuse the policy when direct compress is enabled
CREATE TABLE dc(time timestamptz NOT NULL, device int);
SELECT table_name FROM create_hypertable('dc', 'time',
    chunk_time_interval => interval '1 day');
ALTER TABLE dc SET (timescaledb.compress, timescaledb.compress_segmentby='device',
                    timescaledb.direct_compress);
\set ON_ERROR_STOP 0
SELECT add_move_to_columnstore_policy('dc');
\set ON_ERROR_STOP 1
DROP TABLE dc;

DROP FUNCTION chunk_states(regclass);
DROP VIEW policy_jobs;
