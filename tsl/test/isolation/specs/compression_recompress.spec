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

   CREATE UNIQUE INDEX ON sensor_data (time, sensor_id);

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

step "s1_compress" {
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
}

step "s1_decompress" {
   SELECT count(*) FROM (SELECT decompress_chunk(i) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
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

step "s2_insert" {
   INSERT INTO sensor_data VALUES ('2022-01-01 20:00'::timestamptz, 1, 1.0, 1.0), ('2022-01-01 21:00'::timestamptz, 2, 2.0, 2.0) ON CONFLICT (time, sensor_id) DO NOTHING;
}

session "s3"

step "s3_block_chunk_insert" {
	SELECT debug_waitpoint_enable('chunk_insert_before_lock');
}

step "s3_release_chunk_insert" {
	SELECT debug_waitpoint_release('chunk_insert_before_lock');
}


permutation "s2_block_on_compressed_chunk_size" "s1_begin" "s1_recompress_chunk" "s2_select_from_compressed_chunk" "s2_wait_for_select_to_finish" "s2_unblock" "s1_rollback"

permutation "s1_compress" "s3_block_chunk_insert" "s2_insert" "s1_decompress" "s1_compress" "s3_release_chunk_insert"
