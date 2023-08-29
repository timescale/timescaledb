# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# This isolation test checks that SELECT queries can be performed in parallel to 
# chunk decompression operations. This version of the isolation tests creates the 
# default index on the time column. See the decompression_chunk_and_parallel_query_wo_idx
# test for a version without any index.
###

setup {

   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null,
   temperature double precision null);

   -- Create the hypertable
   SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '60 days');

-- SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '60 days', create_default_indexes => FALSE);
   

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
   FROM generate_series('2022-01-01', '2022-01-15', INTERVAL '5 minute') AS g1(time),
       generate_series(1, 10, 1) AS g2(sensor_id)
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

step "s1_begin" {
  BEGIN;
}

step "s1_decompress" {
   SELECT count(*) FROM (SELECT decompress_chunk(i, if_compressed => true) FROM show_chunks('sensor_data') i) i;
   SELECT compression_status FROM chunk_compression_stats('sensor_data');
}

step "s1_commit" {
  COMMIT;
}

session "s2"

step "s2_explain_update" {
    UPDATE sensor_data SET cpu = cpu + 1 WHERE cpu = 0.1111111;
}

# UPDATE/DELETE queries don't use TimescaleDB hypertable expansion, and use the
# Postgres expansion code, which might have different locking behavior. Test it
# as well.
permutation "s2_explain_update" "s1_begin" "s1_decompress" "s2_explain_update" "s1_commit" "s2_explain_update"

