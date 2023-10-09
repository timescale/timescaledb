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

DROP TABLE test;
