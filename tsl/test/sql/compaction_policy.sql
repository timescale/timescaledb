-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for the compaction policy. The policy runs compact_chunk on every
-- unordered chunk of a hypertable so query plans no longer need an extra sort.

-- Count the unordered chunks of a hypertable.
CREATE FUNCTION unordered_count(regclass) RETURNS bigint LANGUAGE SQL AS $$
  SELECT count(*) FROM show_chunks($1) c
  WHERE 'UNORDERED' = ANY(_timescaledb_functions.chunk_status_text(c));
$$;

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

----------------------------------------------------------------------
-- add/remove API and configuration
----------------------------------------------------------------------

-- A compaction policy requires compression to be enabled.
CREATE TABLE plain (time TIMESTAMPTZ NOT NULL, value float);
SELECT create_hypertable('plain', 'time');
\set ON_ERROR_STOP 0
SELECT add_compaction_policy('plain');
-- Negative max_chunks is rejected.
SELECT add_compaction_policy('metrics', max_chunks => -1);
\set ON_ERROR_STOP 1
DROP TABLE plain;

-- The config check rejects bad configuration.
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.policy_compaction_check('{"max_chunks": 1}');
SELECT _timescaledb_functions.policy_compaction_check('{"hypertable_id": 1, "max_chunks": -1}');
SELECT _timescaledb_functions.policy_compaction_check('{"hypertable_id": 1, "max_batches": -1}');
SELECT _timescaledb_functions.policy_compaction_check('{"hypertable_id": 1, "inactive_for": "-1 hour"}');
SELECT _timescaledb_functions.policy_compaction_check('{"hypertable_id": 1, "inactive_for": "not an interval"}');
\set ON_ERROR_STOP 1

-- Add the policy and inspect the resulting job. Default schedule is 5 minutes.
SELECT add_compaction_policy('metrics') AS job_id \gset
SELECT application_name, schedule_interval, proc_schema, proc_name, config
FROM _timescaledb_config.bgw_job WHERE id = :job_id;

-- Adding again errors unless if_not_exists is set.
\set ON_ERROR_STOP 0
SELECT add_compaction_policy('metrics');
\set ON_ERROR_STOP 1
SELECT add_compaction_policy('metrics', if_not_exists => true);

-- max_chunks is stored in the config when provided.
SELECT remove_compaction_policy('metrics');
SELECT add_compaction_policy('metrics', max_chunks => 2) AS job_id \gset
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :job_id;

----------------------------------------------------------------------
-- multi-chunk loop and max_chunks cap
----------------------------------------------------------------------

-- Three single inserts into three different weeks leave three unordered chunks.
INSERT INTO metrics SELECT '2025-01-06'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
INSERT INTO metrics SELECT '2025-01-13'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
INSERT INTO metrics SELECT '2025-01-20'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
SELECT unordered_count('metrics');

-- max_chunks = 2 stops after compacting two chunks, leaving one unordered.
CALL run_job(:job_id);
SELECT unordered_count('metrics');

-- A second run compacts the remaining chunk.
CALL run_job(:job_id);
SELECT unordered_count('metrics');

-- Running again with nothing unordered is a no-op.
CALL run_job(:job_id);
SELECT unordered_count('metrics');

-- Data is preserved across compaction.
SELECT count(*), min(time), max(time) FROM metrics;
SELECT remove_compaction_policy('metrics');
DROP TABLE metrics;

----------------------------------------------------------------------
-- partial and frozen chunks are skipped
----------------------------------------------------------------------

CREATE TABLE excl (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

-- Show each chunk's status, ordered by time.
CREATE FUNCTION excl_status() RETURNS TABLE(range_start timestamptz, status text[]) LANGUAGE SQL AS $$
  SELECT range_start,
         _timescaledb_functions.chunk_status_text(format('%I.%I', chunk_schema, chunk_name)::regclass)
  FROM timescaledb_information.chunks WHERE hypertable_name = 'excl' ORDER BY range_start;
$$;

-- An unordered chunk that will be compacted.
INSERT INTO excl SELECT '2025-03-03'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;

-- A partial chunk: compressed, then a row inserted into the uncompressed part.
INSERT INTO excl SELECT '2025-03-10'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
SET timescaledb.enable_direct_compress_insert = false;
INSERT INTO excl VALUES ('2025-03-10 00:00:30', 'd1', 1);
SET timescaledb.enable_direct_compress_insert = true;

-- Before the policy runs: one unordered chunk and one partial chunk.
SELECT * FROM excl_status();

SELECT add_compaction_policy('excl') AS job_id \gset
CALL run_job(:job_id);

-- After: the unordered chunk is compacted; the partial chunk is skipped and
-- left unchanged.
SELECT * FROM excl_status();

DROP TABLE excl;
DROP FUNCTION excl_status();

-- A frozen chunk is skipped. Use a single-chunk table so the chunk is easy to
-- select for freezing.
CREATE TABLE frz (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');
INSERT INTO frz SELECT '2025-04-01'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
SELECT _timescaledb_functions.freeze_chunk(c) FROM show_chunks('frz') c;
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('frz') c;

SELECT add_compaction_policy('frz') AS job_id \gset
CALL run_job(:job_id);

-- The frozen chunk keeps its UNORDERED flag: it was skipped.
SELECT _timescaledb_functions.chunk_status_text(c) FROM show_chunks('frz') c;

SELECT _timescaledb_functions.unfreeze_chunk(c) FROM show_chunks('frz') c;
DROP TABLE frz;

----------------------------------------------------------------------
-- a failing chunk is reported and does not abort the run
----------------------------------------------------------------------

CREATE TABLE fail (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

-- Two unordered chunks.
INSERT INTO fail SELECT '2025-05-05'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
INSERT INTO fail SELECT '2025-05-12'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
SELECT unordered_count('fail');

-- compact_chunk scans an index on the compressed chunk. Drop it on one chunk
-- so compaction of that chunk fails.
SELECT i.indexrelid::regclass AS doomed_index
FROM _timescaledb_catalog.chunk ch
JOIN _timescaledb_catalog.hypertable ht ON ch.hypertable_id = ht.id
JOIN _timescaledb_catalog.compression_settings cs
  ON cs.relid = ch.relid
JOIN pg_index i ON i.indrelid = cs.compress_relid
WHERE ht.table_name = 'fail' ORDER BY ch.id LIMIT 1 \gset
DROP INDEX :doomed_index;

SELECT add_compaction_policy('fail') AS job_id \gset
-- The run warns about the failed chunk but compacts the other one and finishes.
CALL run_job(:job_id);
SELECT unordered_count('fail');
DROP TABLE fail;

----------------------------------------------------------------------
-- inactive_for skips recently written chunks
----------------------------------------------------------------------

CREATE TABLE gate (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');
INSERT INTO gate SELECT '2025-06-02'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,1000) i;
SELECT unordered_count('gate');

-- The chunk was just written, so with inactive_for set it is skipped.
SELECT add_compaction_policy('gate', inactive_for => '1 hour') AS job_id \gset
CALL run_job(:job_id);
SELECT unordered_count('gate');

-- Once the chunk is no longer tracked as recently active it is compacted.
SELECT _timescaledb_functions.chunk_statistics_reset();
CALL run_job(:job_id);
SELECT unordered_count('gate');
DROP TABLE gate;

----------------------------------------------------------------------
-- max_batches limits batches recompressed per chunk
----------------------------------------------------------------------

CREATE TABLE mb (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

-- Negative max_batches is rejected.
\set ON_ERROR_STOP 0
SELECT add_compaction_policy('mb', max_batches => -1);
\set ON_ERROR_STOP 1

-- max_batches is unlimited (0)
SELECT add_compaction_policy('mb', max_batches => 0) AS job_id \gset
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :job_id;

INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,3000) i;
INSERT INTO mb SELECT '2025-07-06'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,3000) i;

SELECT unordered_count('mb');
CALL run_job(:job_id);
SELECT unordered_count('mb'); -- should be 0

SELECT remove_compaction_policy('mb');
TRUNCATE mb;

-- max_batches = 4
SELECT add_compaction_policy('mb', max_batches => 4) AS job_id \gset
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :job_id;

-- Three isolated overlap groups (2 + 2 + 3 batches) with gaps between them.
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1,200) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(100,300) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(500,700) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(600,800) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1000,1200) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1100,1300) i;
INSERT INTO mb SELECT '2025-07-07'::timestamptz + (i || ' minute')::interval, 'd1', i FROM generate_series(1200,1400) i;
SELECT unordered_count('mb');

-- First run (2 compactions), hitting the limit of 4. 3 batches remain unmerged.
CALL run_job(:job_id);
SELECT unordered_count('mb');

-- Second run compacts the remaining 3 batches.
CALL run_job(:job_id);
SELECT unordered_count('mb');

DROP TABLE mb;

----------------------------------------------------------------------
-- remove behavior
----------------------------------------------------------------------

CREATE TABLE m2 (time TIMESTAMPTZ NOT NULL, value float) WITH (tsdb.hypertable, tsdb.orderby='time');
SELECT add_compaction_policy('m2');
SELECT remove_compaction_policy('m2');
\set ON_ERROR_STOP 0
SELECT remove_compaction_policy('m2');
\set ON_ERROR_STOP 1
SELECT remove_compaction_policy('m2', if_exists => true);
DROP TABLE m2;

DROP FUNCTION unordered_count(regclass);
