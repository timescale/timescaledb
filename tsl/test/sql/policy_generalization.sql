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

-- Validate show_chunks
SELECT count(*) from show_chunks('test', newer_than => INTERVAL '1 hour');
SELECT pg_sleep(2);
SELECT count(*) from show_chunks('test', older_than => INTERVAL '1 second');

-- Validate drop_chunks
SELECT count(*) from drop_chunks('test', older_than => INTERVAL '1 second');
select count(*) from timescaledb_information.chunks where hypertable_name='test';

INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;
select count(*) from timescaledb_information.chunks where hypertable_name='test';
SELECT count(*) from drop_chunks('test', newer_than => INTERVAL '1 hour');
select count(*) from timescaledb_information.chunks where hypertable_name='test';

-- Validate retention policy
INSERT INTO test SELECT i, i %10, 0.10 FROM generate_series(1, 100, 1) i;
select count(*) from timescaledb_information.chunks where hypertable_name='test';

SELECT add_retention_policy('test', INTERVAL '5 seconds', true) as drop_chunks_job_id \gset
SELECT pg_sleep(5);
CALL run_job(:drop_chunks_job_id);
select count(*) from timescaledb_information.chunks where hypertable_name='test';

SELECT remove_retention_policy('test');
DROP TABLE test;
