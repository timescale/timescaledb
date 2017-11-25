CREATE TABLE PUBLIC.drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test2(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test3(time bigint, temp float8, device_id text);
CREATE INDEX ON drop_chunk_test1(time DESC);
SELECT create_hypertable('public.drop_chunk_test1', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test2', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test3', 'time', chunk_time_interval => 1, create_default_indexes=>false);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT  _timescaledb_internal.get_partition_for_key('dev1'::text);
SELECT  _timescaledb_internal.get_partition_for_key('dev7'::varchar(5));

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

INSERT INTO PUBLIC.drop_chunk_test3 VALUES(1, 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(2, 2.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(3, 3.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(4, 4.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(5, 5.0, 'dev7');
INSERT INTO PUBLIC.drop_chunk_test3 VALUES(6, 6.0, 'dev7');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');
\dt "_timescaledb_internal".*

CREATE VIEW dependent_view AS SELECT * FROM _timescaledb_internal._hyper_1_1_chunk;

\set ON_ERROR_STOP 0
SELECT drop_chunks(2);
\set ON_ERROR_STOP 1

SELECT drop_chunks(2, CASCADE=>true);

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT drop_chunks(3, 'drop_chunk_test1');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

-- 2,147,483,647 is the largest int so this tests that BIGINTs work
SELECT drop_chunks(2147483648, 'drop_chunk_test3');

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2' OR h.table_name = 'drop_chunk_test3');

\dt "_timescaledb_internal".*

-- should error because no hypertable
\set ON_ERROR_STOP 0
SELECT drop_chunks(5, 'drop_chunk_test4');
\set ON_ERROR_STOP 1

DROP TABLE _timescaledb_internal._hyper_1_6_chunk;

SELECT c.id AS chunk_id, c.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = time_dimension.id)
INNER JOIN  _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

CREATE TABLE PUBLIC.drop_chunk_test_ts(time timestamp, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_ts', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

CREATE TABLE PUBLIC.drop_chunk_test_tstz(time timestamptz, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_tstz', 'time', chunk_time_interval => interval '1 minute', create_default_indexes=>false);

SET timezone = '+1';
INSERT INTO PUBLIC.drop_chunk_test_ts VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');
INSERT INTO PUBLIC.drop_chunk_test_tstz VALUES(now()-INTERVAL '5 minutes', 1.0, 'dev1');

SELECT * FROM test.show_subtables('drop_chunk_test_ts');
SELECT * FROM test.show_subtables('drop_chunk_test_tstz');

BEGIN;
    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT drop_chunks(interval '1 minute', 'drop_chunk_test_tstz');
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

BEGIN;
    SELECT drop_chunks(now()::timestamp-interval '1 minute', 'drop_chunk_test_ts');
    SELECT * FROM test.show_subtables('drop_chunk_test_ts');
    SELECT drop_chunks(now()-interval '1 minute', 'drop_chunk_test_tstz');
    SELECT * FROM test.show_subtables('drop_chunk_test_tstz');
ROLLBACK;

\set ON_ERROR_STOP 0
SELECT drop_chunks(interval '1 minute');
SELECT drop_chunks(interval '1 minute', 'drop_chunk_test3');
SELECT drop_chunks(now()-interval '1 minute', 'drop_chunk_test_ts');
SELECT drop_chunks(now()::timestamp-interval '1 minute', 'drop_chunk_test_tstz');
\set ON_ERROR_STOP 1


CREATE TABLE PUBLIC.drop_chunk_test_date(time date, temp float8, device_id text);
SELECT create_hypertable('public.drop_chunk_test_date', 'time', chunk_time_interval => interval '1 day', create_default_indexes=>false);

SET timezone = '+100';
INSERT INTO PUBLIC.drop_chunk_test_date VALUES(now()-INTERVAL '2 day', 1.0, 'dev1');

BEGIN;
    SELECT drop_chunks(interval '1 day', 'drop_chunk_test_date');
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

BEGIN;
    SELECT drop_chunks((now()-interval '1 day')::date, 'drop_chunk_test_date');
    SELECT * FROM test.show_subtables('drop_chunk_test_date');
ROLLBACK;

