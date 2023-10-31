# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# This test verifies if the compressed and uncompressed data are seen in parallel. Since 
# we freeze the compressed data immediately, it becomes visible to all transactions
# that are running concurrently. However, parallel transactions should not be able to
# see the compressed hypertable in the catalog and query the data two times.
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
   generate_series('2022-01-01', '2022-01-15', INTERVAL '1 hour') AS g1(time),
   generate_series(1, 50, 1) AS g2(sensor_id)
   ORDER BY time;

   SELECT count(*) FROM sensor_data;

   ALTER TABLE sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id');
}

teardown {
   DROP TABLE sensor_data;
}

session "s1"
step "s1_compress" {
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
}

step "s1_select_count" {
   SELECT count(*) FROM sensor_data;
}

session "s2"
step "s2_select_count_and_stats" {
   SELECT count(*) FROM sensor_data;
   SELECT chunk_schema, chunk_name, compression_status FROM chunk_compression_stats('sensor_data') ORDER BY 1, 2, 3;
}

step "s2_lock_compression" {
    SELECT debug_waitpoint_enable('compression_done_before_truncate_uncompressed');
}

step "s2_unlock_compression" {
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND (locktype = 'advisory' or relation::regclass::text LIKE '%chunk') ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compression_done_before_truncate_uncompressed');
}

permutation "s1_select_count" "s2_select_count_and_stats"
permutation "s1_select_count" "s1_compress" "s1_select_count" "s2_select_count_and_stats"
permutation "s2_lock_compression" "s2_select_count_and_stats" "s1_compress" "s2_select_count_and_stats" "s2_unlock_compression" "s2_select_count_and_stats"
