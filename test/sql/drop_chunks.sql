\o /dev/null
\ir include/create_single_db.sql
\o

CREATE TABLE PUBLIC.drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test2(time bigint, temp float8, device_id text);
CREATE INDEX ON drop_chunk_test1(time DESC);
SELECT create_hypertable('public.drop_chunk_test1', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test2', 'time', chunk_time_interval => 1, create_default_indexes=>false);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT  _timescaledb_internal.get_partition_for_key('dev1');
SELECT  _timescaledb_internal.get_partition_for_key('dev7');

INSERT INTO PUBLIC.drop_chunk_test1 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test1 VALUES(6, 6.0, 'dev7');

INSERT INTO PUBLIC.drop_chunk_test2 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test2 VALUES(6, 6.0, 'dev7');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');
\dt "_timescaledb_internal".*

SELECT _timescaledb_internal.drop_chunks_older_than(2);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT _timescaledb_internal.drop_chunks_older_than(3, 'drop_chunk_test1');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*
