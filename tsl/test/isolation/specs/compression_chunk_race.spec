# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test the execution of two compression jobs in parallel, and compression
# running at the same time as drop_chunks
###

setup {
   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null,
   temperature double precision null);

   -- Create large chunks that take a long time to compress
   SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '14 days');

   INSERT INTO sensor_data
   SELECT
   time + (INTERVAL '1 minute' * random()) AS time,
   sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01', '2022-01-15', INTERVAL '1 minute') AS g1(time),
   generate_series(1, 50, 1) AS g2(sensor_id)
   ORDER BY time;

   SELECT count(*) FROM sensor_data;

   ALTER TABLE sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id');
   DO $$ BEGIN PERFORM add_compression_policy('sensor_data', INTERVAL '1 day'); END $$;
}

teardown {
   DROP TABLE sensor_data;
}


session "s1"
setup {
   SET client_min_messages=ERROR; -- Suppress chunk "_hyper_XXX_chunk" is not compressed messages 
}

step "s1_compress" {
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
}

step "s1_compress_first" {
   SELECT count(compress_chunk(i)) FROM (SELECT i FROM show_chunks('sensor_data') i ORDER BY i LIMIT 1) s;
}

step "s1_decompress_first" {
   SELECT count(decompress_chunk(i)) FROM (SELECT i FROM show_chunks('sensor_data') i ORDER BY i LIMIT 1) s;
}

step "s1_recompress_first" {
   SELECT count(compress_chunk(i, recompress => true)) FROM (SELECT i FROM show_chunks('sensor_data') i ORDER BY i LIMIT 1) s;
}

step "s1_insert_first" {
   INSERT INTO sensor_data VALUES ('2022-01-02', 1, 0.5, 50);
}

step "s1_move_first" {
   SELECT count(_timescaledb_functions.move_to_columnstore(i)) FROM (SELECT i FROM show_chunks('sensor_data') i ORDER BY i LIMIT 1) s;
}

step "s1_decompress" {
   SELECT count(*) FROM (SELECT decompress_chunk(i, if_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
}

session "s2"
setup {
   SET client_min_messages=ERROR; -- Suppress chunk "_hyper_XXX_chunk" is not compressed messages 
}

step "s2_compress" {
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
   RESET client_min_messages;
}

step "s2_decompress" {
   SELECT count(*) FROM (SELECT decompress_chunk(i, if_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
   SELECT count(*) FROM sensor_data;
   RESET client_min_messages;
}

session "s3"
step "s3_lock_compression" {
    SELECT debug_waitpoint_enable('compress_chunk_impl_start');
}

step "s3_lock_decompression" {
    SELECT debug_waitpoint_enable('decompress_chunk_impl_start');
}

step "s3_unlock_compression" {
    -- Ensure that we are waiting on our debug waitpoint and one chunk
    -- Note: The OIDs of the advisory locks are based on the hash value of the lock name (see debug_point_init())
    --       compress_chunk_impl_start = 3379597659.
    -- 'SELECT relation::regclass, ....' can not be used, because it returns a field with a variable length
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND (locktype = 'advisory' or relation::regclass::text LIKE '%chunk') ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compress_chunk_impl_start');
}

step "s3_unlock_decompression" {
    -- Ensure that we are waiting on our debug waitpoint and one chunk
    -- Note: The OIDs of the advisory locks are based on the hash value of the lock name (see debug_point_init())
    --       decompress_chunk_impl_start = 2415054640.
    -- 'SELECT relation::regclass, ....' can not be used, because it returns a field with a variable length
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND (locktype = 'advisory' or relation::regclass::text LIKE '%chunk') ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('decompress_chunk_impl_start');
}

step "s3_release_compression" {
    SELECT debug_waitpoint_release('compress_chunk_impl_start');
}

step "s3_release_decompression" {
    SELECT debug_waitpoint_release('decompress_chunk_impl_start');
}

step "s3_lock_in_memory" {
    SELECT debug_waitpoint_enable('recompress_in_memory_start');
}

step "s3_release_in_memory" {
    SELECT debug_waitpoint_release('recompress_in_memory_start');
}

step "s3_lock_segmentwise" {
    SELECT debug_waitpoint_enable('recompress_segmentwise_start');
}

step "s3_release_segmentwise" {
    SELECT debug_waitpoint_release('recompress_segmentwise_start');
}

step "s3_lock_move" {
    SELECT debug_waitpoint_enable('move_to_columnstore_start');
}

step "s3_release_move" {
    SELECT debug_waitpoint_release('move_to_columnstore_start');
}

step "s3_lock_drop" {
    SELECT debug_waitpoint_enable('drop_chunks_chunks_found');
}

step "s3_release_drop" {
    SELECT debug_waitpoint_release('drop_chunks_chunks_found');
}

session "s4"
setup {
   SET client_min_messages=ERROR;
}

step "s4_drop" {
   SELECT count(*) FROM drop_chunks('sensor_data', older_than => '2023-01-01'::timestamptz);
}

step "s4_drop_created" {
   SELECT count(*) FROM drop_chunks('sensor_data', created_before => now());
}

step "s4_show" {
   SELECT count(*) FROM show_chunks('sensor_data');
}

session "s5"
step "s5_policy" {
   DO $$
   DECLARE
      job int;
   BEGIN
      SELECT job_id INTO job FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression';
      CALL run_job(job);
   END
   $$;
}

permutation "s3_lock_compression" "s3_lock_decompression" "s1_compress" "s2_compress" (s1_compress) "s3_unlock_compression" "s1_decompress" "s2_decompress" (s1_decompress) "s3_unlock_decompression"

# Compression stops at the waitpoint while it holds the chunk table lock,
# then drop_chunks starts and has to wait for it. Compression updates the
# chunk catalog row later, so drop_chunks has to lock the chunk tables
# before the catalog rows or one of them fails with a deadlock.
permutation "s3_lock_compression" "s1_compress_first" "s4_drop" "s3_release_compression" "s4_show"

# The same for decompression, recompression of a fully compressed chunk,
# segmentwise recompression of a partial chunk and move_to_columnstore
permutation "s1_compress_first" "s3_lock_decompression" "s1_decompress_first" "s4_drop" "s3_release_decompression" "s4_show"
permutation "s1_compress_first" "s3_lock_in_memory" "s1_recompress_first" "s4_drop" "s3_release_in_memory" "s4_show"
permutation "s1_compress_first" "s1_insert_first" "s3_lock_segmentwise" "s1_compress_first" "s4_drop" "s3_release_segmentwise" "s4_show"
permutation "s3_lock_move" "s1_move_first" "s4_drop" "s3_release_move" "s4_show"

# drop_chunks by creation time locks the chunk catalog rows while it looks
# for the chunks, so it has to lock the chunk tables before that
permutation "s3_lock_compression" "s1_compress_first" "s4_drop_created" "s3_release_compression" "s4_show"

# drop_chunks stops at the waitpoint after it has locked the chunks, so the
# compression policy waits for it, finds the chunks gone and should skip them
# instead of failing.
permutation "s3_lock_drop" "s4_drop" "s5_policy" "s3_release_drop" "s4_show"

