\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

\c Test1

CREATE TABLE PUBLIC."testNs" (
  time BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);
CREATE INDEX ON PUBLIC."testNs" (device_id, time DESC NULLS LAST) WHERE device_id IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (time DESC NULLS LAST, series_0) WHERE series_0 IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (time DESC NULLS LAST, series_1)  WHERE series_1 IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (time DESC NULLS LAST, series_2) WHERE series_2 IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (time DESC NULLS LAST, series_bool) WHERE series_bool IS NOT NULL;

SELECT * FROM add_hypertable('"public"."testNs"', 'time', 'device_id', hypertable_name=>'testNs', associated_schema_name=>'testNs' );

SELECT set_is_distinct_flag('"public"."testNs"', 'device_id', TRUE);

\c Test1
BEGIN;
SELECT *
FROM create_temp_copy_table('testNs', 'copy_t');
\COPY copy_t FROM 'data/ds1_dev1_1.tsv' NULL AS '';
SELECT *
FROM insert_data('testNs', 'copy_t');
COMMIT;

SELECT close_chunk_end(c.id)
FROM get_open_partition_for_key('testNs', 'dev1') part
INNER JOIN chunk c ON (c.partition_id = part.id);

\c Test1
BEGIN;
SELECT *
FROM create_temp_copy_table('testNs', 'copy_t');
\COPY copy_t FROM 'data/ds1_dev1_2.tsv' NULL AS '';
SELECT *
FROM insert_data('testNs', 'copy_t');
COMMIT;

\c test2
BEGIN;
SELECT *
FROM create_temp_copy_table('testNs', 'copy_t');
\COPY copy_t FROM 'data/ds1_dev2_1.tsv' NULL AS '';
SELECT *
FROM insert_data('testNs', 'copy_t');
COMMIT;

\c Test1
\d+ "testNs".*

\c test2
\d+ "testNs".*
SELECT *
FROM "testNs"._hyper_1_0_replica;
SELECT *
FROM "testNs"._hyper_1_0_distinct;
