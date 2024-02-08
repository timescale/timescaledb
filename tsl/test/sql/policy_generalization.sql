-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Validate utility functions and policies for INTEGER columns
-- using chunk creation time metadata. This allows to specify
-- INTERVAL boundary for INTEGER type columns.
DROP TABLE IF EXISTS test;
CREATE TABLE test(time INTEGER, device INTEGER, temp FLOAT);
SELECT create_hypertable('test', 'time', chunk_time_interval => 10);
INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;
select count(*) from timescaledb_information.chunks where hypertable_name='test';

-- Validate that show_chunks/drop chunks doesn't work with existing arguments
\set ON_ERROR_STOP 0
SELECT count(*) from show_chunks('test', newer_than => INTERVAL '1 hour');
SELECT count(*) from show_chunks('test', older_than => now());
SELECT count(*) from drop_chunks('test', older_than => now());
\set ON_ERROR_STOP 1

SELECT count(*) from show_chunks('test', created_after => INTERVAL '1 hour');
SELECT count(*) from show_chunks('test', created_before => now());
SELECT count(*) from drop_chunks('test', created_before => now());
select count(*) from timescaledb_information.chunks where hypertable_name='test';

INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;
select count(*) from timescaledb_information.chunks where hypertable_name='test';
SELECT count(*) from drop_chunks('test', created_after => INTERVAL '1 hour');
select count(*) from timescaledb_information.chunks where hypertable_name='test';

-- retention policy
INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;
\set ON_ERROR_STOP 0
-- interval input for "drop_after" for INTEGER partitioning errors out
SELECT add_retention_policy('test', INTERVAL '5 seconds', true);
-- integer input for "drop_after" for INTEGER partitioning without valid
-- integer_now function errors out
SELECT add_retention_policy('test', 2000, true);
-- both drop_created_before and drop_after should error out
SELECT add_retention_policy('test', drop_after => INTERVAL '5 seconds',
    drop_created_before => INTERVAL '2 seconds');
\set ON_ERROR_STOP 1
SELECT add_retention_policy('test', drop_created_before => INTERVAL '2 seconds',
    if_not_exists => true) as drop_chunks_job_id \gset
CALL run_job(:drop_chunks_job_id);
select count(*) from timescaledb_information.chunks where hypertable_name='test';
SELECT pg_sleep(3);
CALL run_job(:drop_chunks_job_id);
select count(*) from timescaledb_information.chunks where hypertable_name='test';
-- check for WARNING/NOTICE if policy already exists
SELECT add_retention_policy('test', drop_created_before => INTERVAL '2 seconds',
    if_not_exists => true);
SELECT add_retention_policy('test', drop_created_before => INTERVAL '20 seconds',
    if_not_exists => true);
SELECT remove_retention_policy('test');

-- compression policy
ALTER TABLE test SET (timescaledb.compress);
INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;

-- Chunk compression status
SELECT DISTINCT compression_status FROM _timescaledb_internal.compressed_chunk_stats;

-- Compression policy
SELECT add_compression_policy('test', compress_created_before => INTERVAL '1 hour') AS compress_chunks_job_id \gset
SELECT pg_sleep(3);
CALL run_job(:compress_chunks_job_id);
-- Chunk compression status
SELECT DISTINCT compression_status FROM _timescaledb_internal.compressed_chunk_stats;
SELECT remove_compression_policy('test');
SELECT add_compression_policy('test', compress_created_before => INTERVAL '2 seconds') AS compress_chunks_job_id \gset
CALL run_job(:compress_chunks_job_id);
-- Chunk compression status
SELECT DISTINCT compression_status FROM _timescaledb_internal.compressed_chunk_stats;

-- check for WARNING/NOTICE if policy already exists
SELECT add_compression_policy('test', compress_created_before => INTERVAL '2 seconds',
    if_not_exists => true);
SELECT add_compression_policy('test', compress_created_before => INTERVAL '20 seconds',
    if_not_exists => true);
SELECT remove_compression_policy('test');

DROP TABLE test;
