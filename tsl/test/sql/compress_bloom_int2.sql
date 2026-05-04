-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE int2_crosstype(ts int NOT NULL, i2 smallint, i4 int)
WITH (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.compress_orderby = 'ts', tsdb.compress_index = 'bloom(i2), bloom(i2, i4)')
;

INSERT INTO int2_crosstype SELECT i, (i % 100)::smallint, i % 100 FROM generate_series(1, 5000) i;
INSERT INTO int2_crosstype SELECT i, (-1 * (i % 100))::smallint, i % 100 FROM generate_series(5001, 10000) i;
SELECT compress_chunk(c) FROM show_chunks('int2_crosstype') c;

SELECT count(*) FROM int2_crosstype WHERE i2 = 1::smallint;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::bigint;

SELECT count(*) FROM int2_crosstype WHERE i2 = -1::smallint;
SELECT count(*) FROM int2_crosstype WHERE i2 = -1::bigint;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::smallint AND i4 = 1;
SELECT count(*) FROM int2_crosstype WHERE i2 = 1::bigint AND i4 = 1;

DROP TABLE int2_crosstype;
