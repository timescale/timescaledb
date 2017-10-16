--
--  This test will create chunks in two dimenisions, time (x) and
--  space (y), where the time dimension is aligned. The figure below
--  shows the expected result. The chunk number in the figure
--  indicates the creation order.
--
--  +
--  +
--  +     +-----+     +-----+
--  +     |  2  |     |  3  |
--  +     |     +---+-+     |
--  +     +-----+ 5 |6+-----+
--  +     |  1  +---+-+-----+     +---------+
--  +     |     |   |4|  7  |     |    8    |
--  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
--  0         5         10        15        20
--
-- Partitioning:
--
-- Chunk #  |  time  | space
--    1     |   3    |   2
--    4     |   1    |   3
--    5     |   5    |   3
--

CREATE TABLE chunk_test(time integer, temp float8, tag integer, color integer);

SELECT create_hypertable('chunk_test', 'time', 'tag', 2, chunk_time_interval => 3);

INSERT INTO chunk_test VALUES (4, 24.3, 1, 1);

SELECT * FROM _timescaledb_catalog.dimension_slice;

INSERT INTO chunk_test VALUES (4, 24.3, 2, 1);
INSERT INTO chunk_test VALUES (10, 24.3, 2, 1);

SELECT c.table_name AS chunk_name, d.id AS dimension_id, ds.id AS slice_id, range_start, range_end FROM _timescaledb_catalog.chunk c
LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test'
ORDER BY c.id, d.id;

\c single :ROLE_SUPERUSER
UPDATE _timescaledb_catalog.dimension SET num_slices = 3 WHERE id = 2;
\c single :ROLE_DEFAULT_PERM_USER
SELECT set_chunk_time_interval('chunk_test', 1::bigint);

INSERT INTO chunk_test VALUES (8, 24.3, 11233, 1);

SELECT set_chunk_time_interval('chunk_test', 5::bigint);

SELECT * FROM _timescaledb_catalog.dimension;
INSERT INTO chunk_test VALUES (7, 24.3, 79669, 1);
INSERT INTO chunk_test VALUES (8, 24.3, 79669, 1);
INSERT INTO chunk_test VALUES (10, 24.3, 11233, 1);
INSERT INTO chunk_test VALUES (16, 24.3, 11233, 1);

SELECT c.table_name AS chunk_name, d.id AS dimension_id, ds.id AS slice_id, range_start, range_end FROM _timescaledb_catalog.chunk c
LEFT JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
LEFT JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
LEFT JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'chunk_test'
ORDER BY c.id, d.id;
