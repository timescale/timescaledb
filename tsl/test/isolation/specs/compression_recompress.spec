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
step "s1_commit" {
	COMMIT;
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

step "s1_show_chunk_state" {
   SELECT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
   SELECT count(*) FROM sensor_data;
}

step "s1_insert_for_recompression" {
   INSERT INTO sensor_data
   SELECT time + (INTERVAL '1 minute' * random()) AS time, 
   5 as sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01 00:00:00', '2022-01-01 00:59:59', INTERVAL '1 minute') AS g1(time)
   ORDER BY time;
}

session "s2"
step "s2_begin" {
	BEGIN;
}
step "s2_commit" {
	COMMIT;
}
	
step "s2_select_from_compressed_chunk" {
	SELECT sum(temperature) > 1 FROM sensor_data WHERE sensor_id = 2;
}
## select should not be blocked by the recompress_chunk_segmentwise in progress
step "s2_wait_for_finish" {
}

step "s2_insert_do_nothing" {
   INSERT INTO sensor_data
   VALUES ('2022-01-01 20:00'::timestamptz, 1, 1.0, 1.0), ('2022-01-01 21:00'::timestamptz, 2, 2.0, 2.0) 
   ON CONFLICT (time, sensor_id) DO NOTHING;
}

step "s2_insert_existing_do_nothing" {
   INSERT INTO sensor_data 
   SELECT time, sensor_id, 1.0, 1.0 FROM sensor_data
   WHERE sensor_id = 4
   LIMIT 1
   ON CONFLICT (time, sensor_id) DO NOTHING;
}

step "s2_upsert" {
   INSERT INTO sensor_data 
   VALUES ('2022-01-01 20:00'::timestamptz, 100, 9999, 9999), ('2022-01-01 21:00'::timestamptz, 101, 9999, 9999) 
   ON CONFLICT (time, sensor_id) DO UPDATE SET cpu = EXCLUDED.cpu, temperature = EXCLUDED.temperature;
}

step "s2_upsert_existing" {
   INSERT INTO sensor_data 
   SELECT time, sensor_id, 9999, 9999 FROM sensor_data
   WHERE sensor_id = 4
   LIMIT 1
   ON CONFLICT (time, sensor_id) DO UPDATE SET cpu = EXCLUDED.cpu, temperature = EXCLUDED.temperature;
}

step "s2_delete_compressed" {
	DELETE FROM sensor_data WHERE sensor_id = 1;
}

step "s2_delete_uncompressed" {
	DELETE FROM sensor_data WHERE sensor_id = 11;
}

step "s2_delete_recompressed" {
	DELETE FROM sensor_data WHERE sensor_id = 5 AND time > '2022-01-01 01:00'::timestamptz;
}

step "s2_update_compressed" {
	UPDATE sensor_data SET cpu = 9999 WHERE sensor_id = 1;
}

step "s2_update_uncompressed" {
	UPDATE sensor_data SET cpu = 9999 WHERE sensor_id = 11;
}

step "s2_update_recompressed" {
	UPDATE sensor_data SET cpu = 9999 WHERE sensor_id = 5 AND time > '2022-01-01 01:00'::timestamptz;
}

step "s2_show_updated_count" {
	SELECT COUNT(*) FROM sensor_data WHERE cpu = 9999;
}

session "s3"

step "s3_block_chunk_insert" {
	SELECT debug_waitpoint_enable('chunk_insert_before_lock');
}

step "s3_release_chunk_insert" {
	SELECT debug_waitpoint_release('chunk_insert_before_lock');
}

step "s3_block_exclusive_lock" {
	BEGIN;
	LOCK TABLE sensor_data IN ROW EXCLUSIVE MODE;
}
step "s3_release_exclusive_lock" {
	ROLLBACK;
}

step "s3_begin" {
	BEGIN;
}

step "s3_commit" {
	COMMIT;
}

step "s3_rollback" {
	ROLLBACK;
}

step "s3_delete_uncompressed" {
	DELETE FROM sensor_data WHERE sensor_id = 11;
}

step "s3_recompress_chunk" {
   SELECT count(_timescaledb_functions.recompress_chunk_segmentwise(i)) AS recompress
   FROM show_chunks('sensor_data') i
   LIMIT 1;
}


permutation "s1_begin" "s1_recompress_chunk" "s2_select_from_compressed_chunk" "s2_wait_for_finish" "s1_rollback"

permutation "s1_compress" "s3_block_chunk_insert" "s2_insert_do_nothing" "s1_decompress" "s1_compress" "s3_release_chunk_insert"

## test inserts and recompression
permutation "s1_show_chunk_state" "s2_begin" "s2_insert_do_nothing" "s1_begin" "s1_recompress_chunk" "s2_insert_do_nothing" "s2_wait_for_finish" "s2_commit" "s1_commit" "s1_show_chunk_state"
permutation "s1_show_chunk_state" "s2_begin" "s2_insert_do_nothing" "s1_begin" "s1_recompress_chunk" "s2_insert_existing_do_nothing" "s2_wait_for_finish" "s2_commit" "s1_commit" "s1_show_chunk_state"
permutation "s1_show_chunk_state" "s2_begin" "s2_insert_do_nothing" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state"
permutation "s1_show_chunk_state" "s2_begin" "s2_insert_existing_do_nothing" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state"
permutation "s1_recompress_chunk" "s1_show_chunk_state" "s1_insert_for_recompression" "s1_show_chunk_state" "s2_begin" "s2_insert_existing_do_nothing" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state"
## recompression can block inserts if its able to get the ExclusiveLock to update
## chunk status, it should be quick to release it
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_upsert" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_upsert_existing" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count"
## if recompression cannot update the status, there is no blocking
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_upsert" "s2_wait_for_finish" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count" "s3_release_exclusive_lock"
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_upsert_existing" "s2_wait_for_finish" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count" "s3_release_exclusive_lock"
permutation "s1_show_chunk_state" "s2_begin" "s2_upsert" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_show_chunk_state" "s2_begin" "s2_upsert_existing" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s1_show_chunk_state" "s2_begin" "s2_upsert_existing" "s1_recompress_chunk" "s2_wait_for_finish" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"
# test that we don't update chunk status to fully compressed if there were concurrent inserts to uncompressed chunk
permutation "s1_show_chunk_state" "s3_begin" "s1_begin" "s3_delete_uncompressed" "s1_recompress_chunk" "s2_insert_do_nothing" "s3_rollback" "s1_commit" "s1_show_chunk_state"

## test delete and recompression
## recompression can block deletes if its able to get the ExclusiveLock to update
## chunk status, it should be quick to release it
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_delete_compressed" "s1_commit" "s1_show_chunk_state"
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_delete_uncompressed" "s1_commit" "s1_show_chunk_state"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_delete_recompressed" "s1_commit" "s1_show_chunk_state"
## if recompression cannot update the status, there is no blocking
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_delete_compressed" "s2_wait_for_finish" "s1_commit" "s1_show_chunk_state" "s3_release_exclusive_lock"
## unless they block each other on tuple level
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_delete_uncompressed" "s1_commit" "s1_show_chunk_state" "s3_release_exclusive_lock"
permutation "s1_recompress_chunk" "s3_block_exclusive_lock" "s1_insert_for_recompression" "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_delete_recompressed" "s1_commit" "s1_show_chunk_state" "s3_release_exclusive_lock"
permutation "s1_show_chunk_state" "s2_begin" "s2_delete_uncompressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state"
permutation "s1_show_chunk_state" "s2_begin" "s2_delete_compressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s1_show_chunk_state" "s2_begin" "s2_delete_recompressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state"

##test update and recompression
## recompression can block deletes if its able to get the ExclusiveLock to update
## chunk status, it should be quick to release it
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_update_compressed"  "s1_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_show_chunk_state" "s1_begin" "s1_recompress_chunk" "s2_update_uncompressed" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s1_begin" "s1_recompress_chunk" "s2_update_recompressed" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count"
## if recompression cannot update the status, there is no blocking
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_update_compressed" "s2_wait_for_finish" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count" "s3_release_exclusive_lock"
## unless they block each other on tuple level
permutation "s1_show_chunk_state" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_update_uncompressed" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count" "s3_release_exclusive_lock"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s3_block_exclusive_lock" "s1_begin" "s1_recompress_chunk" "s2_update_recompressed" "s1_commit" "s1_show_chunk_state" "s2_show_updated_count" "s3_release_exclusive_lock"
permutation "s1_show_chunk_state" "s2_begin" "s2_update_uncompressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_show_chunk_state" "s2_begin" "s2_update_compressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"
permutation "s1_recompress_chunk" "s1_insert_for_recompression" "s1_show_chunk_state" "s2_begin" "s2_update_recompressed" "s1_recompress_chunk" "s2_commit" "s1_show_chunk_state" "s2_show_updated_count"

## test multiple recompressions running at same time
## blocking each other since they acquire ShareUpdateExclusive locks on the chunk
permutation "s1_show_chunk_state" "s1_begin" "s3_begin" "s1_recompress_chunk" "s3_recompress_chunk"  "s1_commit" "s3_commit" "s1_show_chunk_state" "s2_show_updated_count"
