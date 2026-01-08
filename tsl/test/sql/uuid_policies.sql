-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Test retention policy on UUID-partitioned hypertables
--

CREATE TABLE uuid_retention_test(id uuid primary key, device int, temp float);
SELECT create_hypertable('uuid_retention_test', 'id', chunk_time_interval => interval '1 day');

-- Insert data spanning multiple days using fixed UUIDv7 values
-- These UUIDs encode specific timestamps:
-- 2025-01-05: 019433c2-ec00-7000-8000-000000000001
-- 2025-01-06: 019438e9-4800-7000-8000-000000000002
-- 2025-01-07: 01943e0f-a400-7000-8000-000000000003
-- 2025-01-08: 01944336-0000-7000-8000-000000000004
-- 2025-01-09: 0194485c-5c00-7000-8000-000000000005
-- 2025-01-10: 01944d82-b800-7000-8000-000000000006
INSERT INTO uuid_retention_test VALUES
       ('019433c2-ec00-7000-8000-000000000001', 1, 1.0),
       ('019438e9-4800-7000-8000-000000000002', 2, 2.0),
       ('01943e0f-a400-7000-8000-000000000003', 3, 3.0),
       ('01944336-0000-7000-8000-000000000004', 4, 4.0),
       ('0194485c-5c00-7000-8000-000000000005', 5, 5.0),
       ('01944d82-b800-7000-8000-000000000006', 6, 6.0);

-- Show the chunks before adding retention policy
-- Should have 6 chunks (one per day)
SELECT count(*) AS chunks_before FROM show_chunks('uuid_retention_test');

-- Test adding retention policy with INTERVAL - this should work
-- Policy: drop chunks older than 3 days
SELECT add_retention_policy('uuid_retention_test', INTERVAL '3 days') AS retention_job_id \gset

-- Verify the policy was created
SELECT proc_name, config FROM timescaledb_information.jobs WHERE job_id = :retention_job_id;

-- Test that invalid drop_after type is rejected (non-interval for UUID)
\set ON_ERROR_STOP 0
SELECT add_retention_policy('uuid_retention_test', 12345, if_not_exists => true);
\set ON_ERROR_STOP 1

-- Set mock time to 2025-01-10 00:00:00 UTC for deterministic test results
-- With 3-day retention, chunks older than 2025-01-07 should be dropped
SET timescaledb.current_timestamp_mock = '2025-01-10 00:00:00+00';

-- Run the retention job
-- This should drop chunks older than 3 days (Jan 5 and Jan 6)
-- and keep chunks that are 3 days old or newer (Jan 7, 8, 9, 10)
CALL run_job(:retention_job_id);

-- Should have 4 chunks remaining
SELECT count(*) AS chunks_after FROM show_chunks('uuid_retention_test');

-- Verify the data - should have 4 rows remaining (devices 3, 4, 5, 6)
SELECT count(*) AS rows_after FROM uuid_retention_test;
SELECT device FROM uuid_retention_test ORDER BY device;

-- Update the policy to keep only the most recent chunk (12 hour retention)
-- This should drop all but the Jan 10 chunk
SELECT remove_retention_policy('uuid_retention_test');
SELECT add_retention_policy('uuid_retention_test', INTERVAL '12 hours') AS retention_job_id \gset

-- Verify the updated policy
SELECT proc_name, config FROM timescaledb_information.jobs WHERE job_id = :retention_job_id;

-- Advance mock time slightly so boundary falls after Jan 9 chunk end
SET timescaledb.current_timestamp_mock = '2025-01-10 12:00:01+00';

-- Run the updated retention job
-- With 12-hour retention from 2025-01-10 12:00:01, boundary is 2025-01-10 00:00:01
-- Jan 9 chunk (ends at 2025-01-10 00:00:00) should be dropped, only Jan 10 remains
CALL run_job(:retention_job_id);

-- Should have 1 chunk remaining
SELECT count(*) AS chunks_final FROM show_chunks('uuid_retention_test');

-- Verify the data - should have 1 row remaining (device 6)
SELECT count(*) AS rows_final FROM uuid_retention_test;
SELECT device FROM uuid_retention_test ORDER BY device;

-- Clean up retention test
RESET timescaledb.current_timestamp_mock;
SELECT remove_retention_policy('uuid_retention_test', if_exists => true);
DROP TABLE uuid_retention_test;

--
-- Test compression policy on UUID-partitioned hypertables
--
CREATE TABLE uuid_compress_test(id uuid primary key, device int, temp float);
SELECT create_hypertable('uuid_compress_test', 'id', chunk_time_interval => interval '1 day');

-- Enable compression on the hypertable
ALTER TABLE uuid_compress_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device'
);

-- Insert data using same fixed UUIDs as retention test
INSERT INTO uuid_compress_test VALUES
       ('019433c2-ec00-7000-8000-000000000001', 1, 1.0),
       ('019438e9-4800-7000-8000-000000000002', 2, 2.0),
       ('01943e0f-a400-7000-8000-000000000003', 3, 3.0),
       ('01944336-0000-7000-8000-000000000004', 4, 4.0),
       ('0194485c-5c00-7000-8000-000000000005', 5, 5.0),
       ('01944d82-b800-7000-8000-000000000006', 6, 6.0);

-- Verify chunks before compression
SELECT count(*) AS chunks_before_compress FROM show_chunks('uuid_compress_test');

-- Add compression policy with INTERVAL - compress chunks older than 2 days
SELECT add_compression_policy('uuid_compress_test', INTERVAL '2 days') AS compression_job_id \gset

-- Verify the policy was created
SELECT proc_name, config FROM timescaledb_information.jobs WHERE job_id = :compression_job_id;

-- Test that invalid compress_after type is rejected (non-interval for UUID)
\set ON_ERROR_STOP 0
SELECT add_compression_policy('uuid_compress_test', 12345, if_not_exists => true);
\set ON_ERROR_STOP 1

-- Set mock time to 2025-01-10 00:00:00 UTC
-- With 2-day compression, chunks older than 2025-01-08 should be compressed (Jan 5, 6, 7)
SET timescaledb.current_timestamp_mock = '2025-01-10 00:00:00+00';

-- Run the compression job
CALL run_job(:compression_job_id);

-- Verify compressed chunks
SELECT count(*) AS compressed_chunks FROM chunk_compression_stats('uuid_compress_test')
WHERE compression_status = 'Compressed';

-- Clean up compression test
RESET timescaledb.current_timestamp_mock;
SELECT remove_compression_policy('uuid_compress_test', if_exists => true);
DROP TABLE uuid_compress_test;

--
-- Test reorder policy on UUID-partitioned hypertables
--
CREATE TABLE uuid_reorder_test(id uuid primary key, device int, temp float);
SELECT create_hypertable('uuid_reorder_test', 'id', chunk_time_interval => interval '1 day');

-- Create an index for reorder (by device)
CREATE INDEX uuid_reorder_test_device_idx ON uuid_reorder_test(device);

-- Insert data into multiple chunks so older ones are eligible for reorder
-- Chunk 1 (Jan 5): insert in reverse device order (3, 2, 1)
INSERT INTO uuid_reorder_test VALUES
       ('019433c2-ec00-7000-8000-000000000003', 3, 3.0),
       ('019433c2-ec00-7000-8000-000000000002', 2, 2.0),
       ('019433c2-ec00-7000-8000-000000000001', 1, 1.0);
-- Chunk 2 (Jan 6): insert in reverse device order (6, 5, 4)
INSERT INTO uuid_reorder_test VALUES
       ('019438e9-4800-7000-8000-000000000006', 6, 6.0),
       ('019438e9-4800-7000-8000-000000000005', 5, 5.0),
       ('019438e9-4800-7000-8000-000000000004', 4, 4.0);
-- Chunk 3 (Jan 7) - will be skipped as too recent
INSERT INTO uuid_reorder_test VALUES
       ('01943e0f-a400-7000-8000-000000000009', 9, 9.0);

SELECT count(*) AS num_chunks FROM show_chunks('uuid_reorder_test');

-- Show physical order in oldest chunk BEFORE reorder (using ctid)
-- Data should be in insert order: device 3, 2, 1
SELECT device, chunk_name FROM (
    SELECT device, tableoid::regclass::text AS chunk_name, ctid
    FROM uuid_reorder_test
    WHERE id < '019438e9-4800-7000-8000-000000000000'  -- Jan 5 chunk only
    ORDER BY ctid
) sub;

-- Add reorder policy
SELECT add_reorder_policy('uuid_reorder_test', 'uuid_reorder_test_device_idx') AS reorder_job_id \gset

-- Verify the policy was created
SELECT proc_name, config FROM timescaledb_information.jobs WHERE job_id = :reorder_job_id;

-- Run the reorder job (reorders eligible older chunks)
CALL run_job(:reorder_job_id);

-- Show physical order in oldest chunk AFTER reorder (using ctid)
-- Data should now be in index order: device 1, 2, 3
SELECT device, chunk_name FROM (
    SELECT device, tableoid::regclass::text AS chunk_name, ctid
    FROM uuid_reorder_test
    WHERE id < '019438e9-4800-7000-8000-000000000000'  -- Jan 5 chunk only
    ORDER BY ctid
) sub;

-- Clean up reorder test
SELECT remove_reorder_policy('uuid_reorder_test', if_exists => true);
DROP TABLE uuid_reorder_test;
