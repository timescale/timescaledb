# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Order of locks in drop index and vacuum analyze was wrong, and DROP
# INDEX took the locks in order index-table, while VACUUM ANALYZE took
# them in order table-index.
#
# Create deadlock if the locking order is wrong. Problem here is that
# vacuum takes two locks. First one to read a table and then one to do
# the actual vacuum. For this reason we need two processes that end up
# in the deadlock (VACUUM ANALYZE and DROP INDEX respectively) and
# then two extra sessions working in lock-step to create the deadlock.
#
# It can be illustrated with this sequence, where we have a chunk
# table and a chunk index. The sessions between the brackets ([]) is
# the lock queue and session holding the lock is in angle brackets
# (<>) is the session that holds the lock on the object in question:
#
# S1: Lock chunk from hypertable
#      index:      []
#      table: <S1> []
# S3: Start VACUUM ANALYZE, will attempt to lock chunk table, but get queued.
#      index:      []
#      table: <S1> [S3]
# S2: Lock chunk table from hypertable, which will be queued
#      index:      []
#      table: <S1> [S3 S2]
# S1: Unlock chunk table
#      index:      []
#      table:      [S3 S2]
# S3: VACUUM ANALYZE continues and takes the lock on the chunk table.
#      index:      []
#      table: <S3> [S2]
# S3: VACUUM ANALYZE will release the lock on the chunk table.
#      index:      []
#      table:      [S2]
# S3: VACUUM ANALYZE will attempt to lock the chunk table again
#      index:      []
#      table:      [S2 S3]
# S2: The LOCK statement gets the lock and VACUUM will wait
#      index:      []
#      table: <S2> [S3]
# S4: DROP INDEX starts and takes lock in index first and then is
#     queued for the chunk table
#      index: <S4> []
#      table: <S2> [S3 S4]
# S2: Release lock on chunk table
#      index: <S4> []
#      table:      [S3 S4]
# S3: VACUUM continues and takes table lock and then tries index lock
#      index: <S4> [S3]
#      table: <S3> [S4]
# Deadlock

setup {
    CREATE TABLE metrics (time timestamptz, device_id integer, temp float);
    SELECT create_hypertable('metrics', 'time', chunk_time_interval => interval '1 day');
    INSERT INTO metrics
    SELECT generate_series('2018-12-01 00:00'::timestamp,
			   '2018-12-03 00:00',
			   '1 hour'),
           (random()*30)::int,
	   random()*80 - 40;
    
    CREATE INDEX metrics_device_time_idx ON metrics(device_id, time NULLS FIRST);

    -- Rename chunks so that we have known names. We cannot execute
    -- VACUUM in a function block. 
    DO $$
    DECLARE
      chunk regclass;
      count int = 1;
    BEGIN
    FOR chunk IN SELECT ch FROM show_chunks('metrics') ch
    LOOP
      EXECUTE format('ALTER TABLE %s RENAME TO metrics_chunk_%s', chunk, count);
      count = count + 1;
    END LOOP;
    END
    $$;
}

teardown {
    DROP TABLE metrics;
}

session "S1"
setup {
    START TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '300ms';
}

step "S1_lock" {
    LOCK TABLE _timescaledb_internal.metrics_chunk_2 IN ACCESS EXCLUSIVE MODE;
}

step "S1_commit" {
    COMMIT;
}

session "S2"
setup {
    START TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '300ms';
}

step "S2_lock" {
    LOCK TABLE _timescaledb_internal.metrics_chunk_2 IN ACCESS EXCLUSIVE MODE;
}

step "S2_commit" {
    COMMIT;
}

session "S3"
step "S3_vacuum" {
    VACUUM ANALYZE _timescaledb_internal.metrics_chunk_2;
}

session "S4"
step "S4_drop_index" {
    DROP INDEX metrics_device_time_idx;
}

permutation S1_lock S3_vacuum S2_lock S1_commit S4_drop_index S2_commit
