-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Test VACUUM FULL with compressed chunks and missing attributes
CREATE TABLE vacuum_missing_test(ts int, c1 int);
SELECT create_hypertable('vacuum_missing_test', 'ts', chunk_time_interval => 1000);
INSERT INTO vacuum_missing_test VALUES (0, 1);
ALTER TABLE vacuum_missing_test SET (timescaledb.compress, timescaledb.compress_segmentby = '');
SELECT compress_chunk(show_chunks('vacuum_missing_test'), true);
ALTER TABLE vacuum_missing_test ADD COLUMN c2 int DEFAULT 7;
VACUUM FULL ANALYZE vacuum_missing_test;
SELECT * FROM vacuum_missing_test;
DROP TABLE vacuum_missing_test;

CREATE TABLE vacuum_missing_test(ts int, c1 int);
SELECT create_hypertable('vacuum_missing_test', 'ts', chunk_time_interval => 1000);
INSERT INTO vacuum_missing_test VALUES (0, 1);
ALTER TABLE vacuum_missing_test SET (timescaledb.compress, timescaledb.compress_segmentby = '');
SELECT compress_chunk(show_chunks('vacuum_missing_test'), true);
ALTER TABLE vacuum_missing_test ADD COLUMN c2 int DEFAULT 7;
VACUUM FULL;
SELECT * FROM vacuum_missing_test;
DROP TABLE vacuum_missing_test;
