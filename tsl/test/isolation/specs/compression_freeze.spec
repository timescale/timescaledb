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

step "s1_compress_delete" {
   SET timescaledb.compress_truncate_behaviour TO truncate_disabled;
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
}

step "s1_compress_delete_then_insert" {
   BEGIN;
   SET LOCAL timescaledb.compress_truncate_behaviour TO truncate_disabled;
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
   -- Insert new rows so the chunks become partially compressed; this forces
   -- the scan to read from BOTH the compressed and uncompressed sides.
   INSERT INTO sensor_data VALUES ('2022-01-05 00:00', 999, 0, 0);
   INSERT INTO sensor_data VALUES ('2022-01-12 00:00', 999, 0, 0);
   COMMIT;
}

step "s1_compress_truncate_or_delete" {
   SET timescaledb.compress_truncate_behaviour TO truncate_or_delete;
   SELECT count(*) FROM (SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('sensor_data') i) i;
}

step "s1_select_count" {
   SELECT count(*) FROM sensor_data;
}

session "s2"
step "s2_select_count_and_stats" {
   SELECT count(*) FROM sensor_data;
   SELECT compression_status, count(*) FROM chunk_compression_stats('sensor_data') GROUP BY 1 ORDER BY 1, 2;
}

step "s2_lock_compression" {
    SELECT debug_waitpoint_enable('compression_done_before_truncate_uncompressed');
}

step "s2_lock_compression_after_truncate" {
    SELECT debug_waitpoint_enable('compression_done_after_truncate_uncompressed');
}

step "s2_lock_compression_after_delete" {
    SELECT debug_waitpoint_enable('compression_done_after_delete_uncompressed');
}

step "s2_lock_compression_after_truncate_or_delete" {
    SELECT debug_waitpoint_enable('compression_done_after_truncate_or_delete_uncompressed');
}

step "s2_unlock_compression" {
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE not granted AND (locktype = 'advisory' or relation::regclass::text LIKE '%chunk') ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compression_done_before_truncate_uncompressed');
}

step "s2_unlock_compression_after_truncate" {
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE granted AND relation::regclass::text LIKE '%hyper%chunk' ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compression_done_after_truncate_uncompressed');
}

step "s2_unlock_compression_after_delete" {
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE granted AND relation::regclass::text LIKE '%hyper%chunk' ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compression_done_after_delete_uncompressed');
}

step "s2_unlock_compression_after_truncate_or_delete" {
    SELECT locktype, mode, granted, objid FROM pg_locks WHERE granted AND relation::regclass::text LIKE '%hyper%chunk' ORDER BY relation, locktype, mode, granted;
    SELECT debug_waitpoint_release('compression_done_after_truncate_or_delete_uncompressed');
}

step "s2_begin_rr" {
    BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    SELECT count(*) FROM sensor_data;
}

step "s2_count_in_tx" {
    SELECT count(*) FROM sensor_data;
}

step "s2_commit" {
    COMMIT;
}

permutation "s1_select_count" "s2_select_count_and_stats"
permutation "s1_select_count" "s1_compress" "s1_select_count" "s2_select_count_and_stats"
permutation "s2_lock_compression" "s2_select_count_and_stats" "s1_compress" "s2_select_count_and_stats" "s2_unlock_compression" "s2_select_count_and_stats"

# Check after TRUNCATE
permutation "s2_lock_compression_after_truncate" "s2_select_count_and_stats" "s1_compress" "s2_unlock_compression_after_truncate" "s2_select_count_and_stats"

# Check after DELETE
permutation "s2_lock_compression_after_delete" "s2_select_count_and_stats" "s1_compress_delete" "s2_select_count_and_stats" "s2_unlock_compression_after_delete" "s2_select_count_and_stats"

# Check after TRUNCATE OR DELETE
# Ideally, we want s2 to be a transaction, so that we can test the spin-lock that is triggered here.
# However, spin locks don't play well with isolation tests, so that is tesed in a TAP test instead `tsl/test/t/004_truncate_or_delete_spin_lock_test.pl`

permutation "s2_lock_compression_after_truncate_or_delete" "s2_select_count_and_stats" "s1_compress_truncate_or_delete" "s2_unlock_compression_after_truncate_or_delete" "s2_select_count_and_stats"

# Test that a concurrent reader does not observe duplicate rows when a
# compression commits with truncate_disabled and the chunks are left
# partially compressed.
#
# Scenario:
#   s2 starts a REPEATABLE READ transaction and takes a count snapshot.
#   s1 compresses every chunk with truncate_disabled (DELETE on the
#   uncompressed side) and then INSERTs new rows into the same chunks in
#   the same transaction. The chunks end up partially compressed, so the
#   planner scans both the compressed companion and the uncompressed
#   chunk relation.
#
# Expected: s2's count stays the same across s1's commit. The DELETE is
# invisible to s2's older snapshot, so the uncompressed rows are still
# visible; if the compressed rows were inserted with HEAP_INSERT_FROZEN
# they would also be visible to that snapshot and the count would double.
permutation "s2_begin_rr" "s1_compress_delete_then_insert" "s2_count_in_tx" "s2_commit"
