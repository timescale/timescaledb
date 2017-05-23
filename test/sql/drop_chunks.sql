\o /dev/null
\ir include/create_single_db.sql
\o

CREATE TABLE PUBLIC.drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE PUBLIC.drop_chunk_test2(time bigint, temp float8, device_id text);
CREATE INDEX ON drop_chunk_test1(time DESC);
SELECT create_hypertable('public.drop_chunk_test1', 'time', chunk_time_interval => 1, create_default_indexes=>false);
SELECT create_hypertable('public.drop_chunk_test2', 'time', chunk_time_interval => 1, create_default_indexes=>false);

SELECT c.id AS chunk_id, c.partition_id, pe.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, c.start_time, c.end_time
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
INNER JOIN _timescaledb_catalog.hypertable h ON (pe.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT  _timescaledb_internal.get_partition_for_key('dev1', 32768);
SELECT  _timescaledb_internal.get_partition_for_key('dev7', 32768);

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

SELECT c.id AS chunk_id, c.partition_id, pe.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, c.start_time, c.end_time
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
INNER JOIN _timescaledb_catalog.hypertable h ON (pe.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT _timescaledb_internal.drop_chunks_older_than(2);

SELECT c.id AS chunk_id, c.partition_id, pe.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, c.start_time, c.end_time
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
INNER JOIN _timescaledb_catalog.hypertable h ON (pe.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*

SELECT _timescaledb_internal.drop_chunks_older_than(3, 'drop_chunk_test1');

SELECT c.id AS chunk_id, c.partition_id, pe.hypertable_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, c.start_time, c.end_time
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
INNER JOIN _timescaledb_catalog.hypertable h ON (pe.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND (h.table_name = 'drop_chunk_test1' OR h.table_name = 'drop_chunk_test2');

\dt "_timescaledb_internal".*
