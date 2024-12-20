-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test NULL NOT DISTINCT unique constraint

-- this test is run only on PG15 since support for NULL NOT DISTINCT
-- only exists since then. once we drop support for PG14, this test
-- can be moved to non-shared compression_conflicts test

set timescaledb.debug_compression_path_info to on;
CREATE TABLE nulls_not_distinct(time timestamptz not null, device text, label text, value float);
CREATE UNIQUE INDEX ON nulls_not_distinct (time, device, label) NULLS NOT DISTINCT;
SELECT table_name FROM create_hypertable('nulls_not_distinct', 'time');
ALTER TABLE nulls_not_distinct SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');

INSERT INTO nulls_not_distinct SELECT '2024-01-01'::timestamptz + format('%s',i)::interval, 'd1', 'l1', i FROM generate_series(1,6000) g(i);
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.5', NULL, 'l1', 1);
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.5', 'd2', NULL, 1);
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.10', 'd2', 'l1', 1);

SELECT count(compress_chunk(c)) FROM show_chunks('nulls_not_distinct') c;

-- shouldn't succeed because nulls are not distinct
\set ON_ERROR_STOP 0
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.5', NULL, 'l1', 1);
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.5', 'd2', NULL, 1);
\set ON_ERROR_STOP 1

-- should insert without error, no conflict
INSERT INTO nulls_not_distinct VALUES ('2024-01-01 0:00:00.5', 'd2', 'l1', 1);

RESET timescaledb.debug_compression_path_info;
DROP TABLE nulls_not_distinct;
