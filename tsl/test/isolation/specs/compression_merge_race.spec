# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test the execution of two merge compression jobs in parallel
###

setup {
   -- Compressing a lot of chunks from a single hypertable in a single transaction can
   -- cause deadlocks due to locking the compressed hypertable when creating compressed chunks.
   -- Hence we compress them in their individual transactions, similar how the compression
   -- policies work.
   CREATE OR REPLACE PROCEDURE compress_chunks_in_individual_transactions(query text)
   LANGUAGE plpgsql
   AS $$
   DECLARE
     chunk regclass;
   BEGIN
     FOR chunk in execute query
     LOOP 
       PERFORM public.compress_chunk(chunk); 
       COMMIT;
     END LOOP;
   END;
   $$;
   
   CREATE TABLE sensor_data (
   time timestamptz not null,
   sensor_id integer not null,
   cpu double precision null,
   temperature double precision null);

   -- Create large chunks that take a long time to compress
   SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '1 hour', create_default_indexes => false);

   INSERT INTO sensor_data
   SELECT time + (INTERVAL '1 minute' * random()) AS time, sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01 00:00:00', '2022-01-02 23:59:59', INTERVAL '1 minute') AS g1(time),
   generate_series(1, 50, 1) AS g2(sensor_id)
   ORDER BY time;

   
   ALTER TABLE sensor_data SET (
   timescaledb.compress, 
   timescaledb.compress_segmentby = 'sensor_id',
   timescaledb.compress_orderby = 'time',
   timescaledb.compress_chunk_time_interval = '2 hours');
}

teardown {
   DROP TABLE sensor_data;
}

session "s1"
step "s1_compress_first_half_of_chunks" {
   call compress_chunks_in_individual_transactions(
     $$
       select show_chunks('sensor_data') limit 24
     $$
   ); 
}

step "s1_compress_all_chunks_single_transaction" {
    BEGIN;
    select count(compress_chunk(c, true)) from show_chunks('sensor_data') c;
}

step "s1_compress_finish" {
    COMMIT;
}

step "s1_compress_single_chunk" {
    select compress_chunk(c, true) from show_chunks('sensor_data') c limit 1;
}


# Compress first two chunks, skip two and compress next two etc.
step "s1_compress_first_two_by_two" {
   call compress_chunks_in_individual_transactions(
     $$
       select i.show_chunks 
       from (
         select row_number() over () as row_number, i as show_chunks 
         from show_chunks('sensor_data') i
       ) i where i.row_number%4 in (1,2) 
     $$); 
}

session "s2"
step "s2_compress_second_half_of_chunks" {
   call compress_chunks_in_individual_transactions($$select show_chunks('sensor_data') i  offset 24$$); 
}

# Compress second two chunks, skip two and compress next two etc.
step "s2_compress_second_two_by_two" {
   call compress_chunks_in_individual_transactions(
     $$
       select i.show_chunks 
       from (
         select row_number() over () as row_number, i as show_chunks 
         from show_chunks('sensor_data') i
       ) i where i.row_number%4 in (3,0) 
     $$); 
}

session "s3"
step "s3_lock_compression" {
    SELECT debug_waitpoint_enable('compress_chunk_impl_start');
}

step "s3_unlock_compression" {
    SELECT debug_waitpoint_release('compress_chunk_impl_start');
}

step "s3_count_chunks_pre_compression" {
    select count(*), 48 as expected from show_chunks('sensor_data');
}

step "s3_count_chunks_post_compression" {
    select count(*), 24 as expected from show_chunks('sensor_data');
}

step "s3_select_on_compressed_chunk" {
    DO $$
    DECLARE
      hyper_id int;
      chunk_id int;
    BEGIN
      SELECT h.compressed_hypertable_id, c.compressed_chunk_id 
      INTO hyper_id, chunk_id
      FROM _timescaledb_catalog.hypertable h 
      INNER JOIN _timescaledb_catalog.chunk c  
      ON h.id = c.hypertable_id 
      WHERE h.table_name = 'sensor_data' 
      AND c.status = 1;
      EXECUTE format('SELECT * 
        FROM _timescaledb_internal.compress_hyper_%s_%s_chunk 
        WHERE sensor_id = 40 
        AND temperature IS NOT NULL;', 
        hyper_id, 
        chunk_id);
    END;
    $$;
}

step "s3_wait_for_finish" {
}



# Check that we produce 24 chunks out of 48 chunks by merging two 1hour chunks
# into 2 hour chunks from two different sessions. First session will run until
# it hits an already compressed chunk.
permutation "s3_count_chunks_pre_compression" "s3_lock_compression" "s1_compress_first_half_of_chunks" "s2_compress_second_half_of_chunks" (s1_compress_first_half_of_chunks) "s3_unlock_compression" (s2_compress_second_half_of_chunks, s1_compress_first_half_of_chunks) "s3_count_chunks_post_compression"
permutation "s3_count_chunks_pre_compression" "s3_lock_compression" "s1_compress_first_two_by_two" "s2_compress_second_two_by_two" (s1_compress_first_two_by_two) "s3_unlock_compression" (s2_compress_second_two_by_two, s1_compress_first_two_by_two) "s3_count_chunks_post_compression"

# Check that we can read existing compressed data during chunk compression when merging.
# The query uses an index scan to verify we are not blocked on using it during compression.
permutation "s1_compress_single_chunk" "s1_compress_all_chunks_single_transaction" "s3_select_on_compressed_chunk"  "s3_wait_for_finish" "s1_compress_finish"
