\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/create_clustered_db.sql

\o
\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

\c Test1
CREATE TABLE drop_chunk_test1(time bigint, temp float8, device_id text);
CREATE TABLE drop_chunk_test2(time bigint, temp float8, device_id text);
SELECT create_hypertable('drop_chunk_test1', 'time', 'device_id', chunk_size_bytes => 10000);
SELECT create_hypertable('drop_chunk_test2', 'time', 'device_id', chunk_size_bytes => 10000);

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

\dt "_iobeamdb_internal".*

SELECT get_partition_for_key('dev1', 32768);
SELECT get_partition_for_key('dev7', 32768);

INSERT INTO drop_chunk_test1 VALUES(1, 1.0, 'dev1');
INSERT INTO drop_chunk_test1 VALUES(2, 2.0, 'dev1');
INSERT INTO drop_chunk_test1 VALUES(3, 3.0, 'dev1');
INSERT INTO drop_chunk_test1 VALUES(4, 4.0, 'dev7');
INSERT INTO drop_chunk_test1 VALUES(5, 5.0, 'dev7');
INSERT INTO drop_chunk_test1 VALUES(6, 6.0, 'dev7');

INSERT INTO drop_chunk_test2 VALUES(1, 1.0, 'dev1');
INSERT INTO drop_chunk_test2 VALUES(2, 2.0, 'dev1');
INSERT INTO drop_chunk_test2 VALUES(3, 3.0, 'dev1');
INSERT INTO drop_chunk_test2 VALUES(4, 4.0, 'dev7');
INSERT INTO drop_chunk_test2 VALUES(5, 5.0, 'dev7');
INSERT INTO drop_chunk_test2 VALUES(6, 6.0, 'dev7');

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c test2

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c meta
SELECT _iobeamdb_meta.drop_chunks_older_than(2);

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c Test1

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c test2

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c meta
SELECT _iobeamdb_meta.drop_chunks_older_than(3, 'drop_chunk_test1');

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c Test1

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*

\c test2

SELECT c.id AS chunk_id, pr.partition_id, pr.hypertable_name, crn.schema_name AS chunk_schema, crn.table_name AS chunk_table, c.start_time, c.end_time
FROM _iobeamdb_catalog.chunk c
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
WHERE hypertable_name = 'public.drop_chunk_test1' OR hypertable_name = 'public.drop_chunk_test2';

SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
\dt "_iobeamdb_internal".*
