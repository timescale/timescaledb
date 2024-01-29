# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test recompression doesn't block (or deadlock with) read operations
###

setup {
   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null,
   temperature double precision null);

   SELECT FROM create_hypertable('sensor_data','time', create_default_indexes => false);

   INSERT INTO sensor_data
   SELECT time + (INTERVAL '1 minute' * random()) AS time, sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01 00:00:00', '2022-01-01 23:59:59', INTERVAL '1 minute') AS g1(time),
   generate_series(1, 5, 1) AS g2(sensor_id)
   ORDER BY time;

   ALTER TABLE sensor_data SET (
   timescaledb.compress, 
   timescaledb.compress_segmentby = 'sensor_id',
   timescaledb.compress_orderby = 'time');

   SELECT compress_chunk(show_chunks('sensor_data'));

   -- Create partially compressed chunk that we can recompress using segmentwise
   INSERT INTO sensor_data
   SELECT time + (INTERVAL '1 minute' * random()) AS time, sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01 00:00:00', '2022-01-01 23:59:59', INTERVAL '1 minute') AS g1(time),
   generate_series(10, 15, 1) AS g2(sensor_id)
   ORDER BY time;
}

teardown {
   DROP TABLE sensor_data;
}

session "s1"
step "s1_begin" {
   BEGIN;
}
step "s1_recompress_chunk" {
   SELECT count(_timescaledb_functions.recompress_chunk_segmentwise(i)) AS recompress
   FROM show_chunks('sensor_data') i
   LIMIT 1;
}
step "s1_rollback" {
	ROLLBACK;
}

session "s2"
## locking up the catalog table will block the recompression from releasing the index lock
## we should not be deadlocking since the index lock has been reduced to ExclusiveLock
step "s2_block_on_compressed_chunk_size" {
	BEGIN;
	LOCK TABLE _timescaledb_catalog.compression_chunk_size;
}
step "s2_unblock" {
	ROLLBACK;
}
step "s2_select_from_compressed_chunk" {
	SELECT sum(temperature) > 1 FROM sensor_data WHERE sensor_id = 2;
}
## select should not be blocked by the recompress_chunk_segmentwise in progress
step "s2_wait_for_select_to_finish" {
}


permutation "s2_block_on_compressed_chunk_size" "s1_begin" "s1_recompress_chunk" "s2_select_from_compressed_chunk" "s2_wait_for_select_to_finish" "s2_unblock" "s1_rollback"
