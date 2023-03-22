# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# This isolation test checks that SELECT queries can be performed in parallel to 
# chunk decompression operations. This version of the isolation test creates a
# hypertable without any index. So, no locks on the index can be creates which
# leads to a different level of concurrency.
###

setup {
   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null,
   temperature double precision null);

   -- Create the hypertable without an index on the hypertable
   SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '60 days', create_default_indexes => FALSE);
   
   -- All generated data is part of one chunk. Only one chunk is used because 'compress_chunk' is
   -- used in this isolation test. In contrast to 'policy_compression_execute' all decompression
   -- operations are executed in one transaction. So, processing more than one chunk with 'compress_chunk'
   -- could lead to deadlocks that do not occur real-world scenarios (due to locks hold on a completely
   -- decompressed chunk).

   INSERT INTO sensor_data
   SELECT time + (INTERVAL '1 minute' * random()) AS time,
      sensor_id,
      random() AS cpu,
      random()* 100 AS temperature
   FROM generate_series('2022-01-01', '2022-01-15', INTERVAL '1 minute') AS g1(time),
      generate_series(1, 50, 1) AS g2(sensor_id)
   ORDER BY time;

   SELECT count(*) FROM sensor_data;
   
   ALTER TABLE sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id');

   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
}

teardown {
   DROP TABLE sensor_data;
}

session "s1"

step "s1_decompress" {
   SELECT count(*) FROM (SELECT decompress_chunk(i, if_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
}

session "s2"

step "s2_read_sensor_data" {
   SELECT FROM sensor_data;
}

session "s3"

step "s3_lock_decompression_locks" {
   -- This waitpoint is defined before the decompressed chunk is re-indexed. Up to this 
   -- point parallel SELECTs should be possible. 
   SELECT debug_waitpoint_enable('decompress_chunk_impl_before_reindex');

   -- This waitpoint is defined after all locks for the decompression and the deletion
   -- of the compressed chunk are requested.
   SELECT debug_waitpoint_enable('decompress_chunk_impl_after_reindex');
}

step "s3_unlock_decompression_before_reindex_lock" {
   -- Ensure that we are waiting on our debug waitpoint
   -- Note: The OIDs of the advisory locks are based on the hash value of the lock name (see debug_point_init())
   --       decompress_chunk_impl_before_reindex = 3966149665.
   SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND locktype = 'advisory' ORDER BY relation, locktype, mode, granted;

   SELECT debug_waitpoint_release('decompress_chunk_impl_before_reindex');
}

step "s3_unlock_decompression_after_reindex_lock" {
   -- Ensure that we are waiting on our debug waitpoint
   -- Note: The OIDs of the advisory locks are based on the hash value of the lock name (see debug_point_init())
   --       decompress_chunk_impl_after_reindex = 1858017383.
   SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND locktype = 'advisory' ORDER BY relation, locktype, mode, granted;

   SELECT debug_waitpoint_release('decompress_chunk_impl_after_reindex');
}

# Desired execution (no locks should be performed in s2_read_sensor_data):
# s3_lock_decompression_locks - Locks the decompression waitpoints.
# s2_read_sensor_data - Read the compressed chunk. This should be executed without blocking.
# s1_decompress - Start the decompression and block on the first waitpoint.
# s2_read_sensor_data - Read the compressed chunk again. This should be still possible without blocking.
# s3_unlock_decompression_before_reindex_lock - Releases the decompress_chunk_impl_before_reindex waitpoint.
# s1_decompress continues - No reindex is performed due to no existing indexes
# s2_read_sensor_data - Read the chunk without any lock.
# s3_unlock_decompression_after_reindex_lock - Releases the decompress_chunk_impl_after_compressed_chunk_lock.
# s1_decompress continues - Finishes the decompression operation and releases the locks.
# s2_read_sensor_data - Read the chunk without any lock.
permutation "s3_lock_decompression_locks" "s2_read_sensor_data" "s1_decompress" "s2_read_sensor_data" "s3_unlock_decompression_before_reindex_lock" "s2_read_sensor_data" "s3_unlock_decompression_after_reindex_lock" "s2_read_sensor_data"
