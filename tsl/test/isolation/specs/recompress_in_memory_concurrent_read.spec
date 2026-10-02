# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test in-memory recompression does not block reads of the chunk being
# recompressed until the compressed relations are swapped
###

setup {
   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null);

   SELECT FROM create_hypertable('sensor_data', 'time', create_default_indexes => false);

   ALTER TABLE sensor_data SET (
   timescaledb.compress,
   timescaledb.compress_segmentby = 'sensor_id',
   timescaledb.compress_orderby = 'time');

   INSERT INTO sensor_data
   SELECT time, sensor_id, 1.0
   FROM generate_series('2022-01-01 00:00:00', '2022-01-01 11:59:59', INTERVAL '1 minute') AS g1(time),
   generate_series(1, 5, 1) AS g2(sensor_id);

   SELECT count(compress_chunk(c)) FROM show_chunks('sensor_data') c;
}

teardown {
   DROP TABLE sensor_data;
}

session "s1"
step "s1_recompress" {
   SELECT count(compress_chunk(c, recompress => true)) AS recompressed
   FROM show_chunks('sensor_data') c;
}
step "s1_show" {
   SELECT _timescaledb_functions.chunk_status_text(c) AS status
   FROM show_chunks('sensor_data') c;
   SELECT count(*) FROM sensor_data;
   SELECT count(*) AS leftover_relations FROM pg_class
   WHERE relname LIKE 'compress_new_%' OR relname LIKE 'compress_old_%';
}

session "s2"
step "s2_select" {
   SELECT count(*) FROM sensor_data;
}

session "s3"
step "s3_wp_enable" {
   SELECT debug_waitpoint_enable('recompress_in_memory_before_swap');
}
step "s3_wp_release" {
   SELECT debug_waitpoint_release('recompress_in_memory_before_swap');
}

# the select must not wait for the recompression to finish
permutation "s3_wp_enable" "s1_recompress" "s2_select" "s3_wp_release" "s1_show"
