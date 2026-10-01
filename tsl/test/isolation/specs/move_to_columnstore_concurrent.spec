# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test move_to_columnstore against concurrent DML.
#
# The move takes a ShareUpdateExclusiveLock, which does not conflict with the
# RowExclusiveLock that DML takes, so ordinary writes are expected to run
# alongside it.
###

setup {
    CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
    WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');

    INSERT INTO metrics
    SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float
    FROM generate_series(1,100) i;
}

teardown {
    DROP TABLE metrics;
}

session "s1"
step "s1_move" {
    SELECT _timescaledb_functions.move_to_columnstore(chunk)
    FROM show_chunks('metrics') chunk;
}
step "s1_status" {
    SELECT _timescaledb_functions.chunk_status_text(chunk) AS status
    FROM show_chunks('metrics') chunk;
}
step "s1_count" {
    SELECT count(*) FROM metrics;
}
# Flush every 10 rows so the scan loop reaches a batch boundary, which is the
# only point where the mover checks for waiting transactions.
step "s1_set_limit" {
    SET timescaledb.move_to_columnstore_tuple_sort_limit = 10;
}
step "s1_reset_limit" {
    RESET timescaledb.move_to_columnstore_tuple_sort_limit;
}

session "s2"
setup {
    SET timescaledb.enable_direct_compress_insert = true;
}
step "s2_begin" { BEGIN; }
step "s2_commit" { COMMIT; }
# Direct compress needs an estimated batch of at least 10 rows to engage,
# which scenario 4 depends on.
step "s2_insert" {
    INSERT INTO metrics
    SELECT '2025-01-02 00:00:30'::timestamptz + (i || ' second')::interval, 'd2', -1.0
    FROM generate_series(1,20) i;
}
step "s2_insert_one" {
    -- a plain rowstore insert; SET LOCAL keeps it scoped to this transaction
    SET LOCAL timescaledb.enable_direct_compress_insert = false;
    INSERT INTO metrics VALUES ('2025-01-02 00:00:45', 'd3', -3.0);
}
step "s2_select" {
    SELECT count(*) FROM metrics;
}
step "s2_update" {
    UPDATE metrics SET value = -2.0 WHERE value = 1.0;
}

session "s3"
step "s3_wp_start_on"  { SELECT debug_waitpoint_enable('move_to_columnstore_start'); }
step "s3_wp_start_off" { SELECT debug_waitpoint_release('move_to_columnstore_start'); }
step "s3_wp_create_on"  { SELECT debug_waitpoint_enable('move_to_columnstore_after_create'); }
step "s3_wp_create_off" { SELECT debug_waitpoint_release('move_to_columnstore_after_create'); }
step "s3_wp_delete_on"  { SELECT debug_waitpoint_enable('move_to_columnstore_after_delete'); }
step "s3_wp_delete_off" { SELECT debug_waitpoint_release('move_to_columnstore_after_delete'); }
step "s3_wp_commit_on"  { SELECT debug_waitpoint_enable('move_to_columnstore_before_commit'); }
step "s3_wp_commit_off" { SELECT debug_waitpoint_release('move_to_columnstore_before_commit'); }
step "s3_wp_insert_on"  { SELECT debug_waitpoint_enable('insert_create_compressed'); }
step "s3_wp_insert_off" { SELECT debug_waitpoint_release('insert_create_compressed'); }

# Scenario 1: the move holds a ShareUpdateExclusiveLock but does not block
# DML. s2_insert and s2_select must complete without waiting.
permutation "s3_wp_start_on" "s1_move" "s2_insert" "s2_select" "s3_wp_start_off" "s1_status" "s1_count"

# Scenario 2: the move loses to an UPDATE that commits first
permutation "s2_begin" "s2_update" "s1_move" "s2_commit" "s1_status" "s1_count"

# Scenario 3: an UPDATE loses to the move
permutation "s3_wp_delete_on" "s1_move" "s2_update" "s3_wp_delete_off" "s1_status" "s1_count"

# Scenario 4a: the insert pauses just before taking the chunk tuple lock, so
# the move creates the compressed chunk first. On release the insert takes the
# lock, rechecks, finds the chunk already compressed and skips creation.
permutation "s3_wp_insert_on" "s2_insert" "s1_move" "s3_wp_insert_off" "s1_status" "s1_count"

# Scenario 4b: the move creates the compressed chunk first and holds the chunk
# tuple lock, which delays a concurrent direct compress insert.
permutation "s3_wp_create_on" "s1_move" "s2_insert" "s3_wp_create_off" "s1_status" "s1_count"

# Scenario 5: the move stops early when another transaction is waiting on it.
permutation "s1_set_limit" "s3_wp_delete_on" "s1_move" "s2_update" "s3_wp_delete_off" "s1_status" "s1_count" "s1_reset_limit"

# Scenario 6: mark chunk partial during move compressed chunk creation
# so that parallel inserts will be visible
permutation "s2_begin" "s2_insert_one" "s1_move" "s2_commit" "s1_status" "s1_count"

# Scenario 7: an insert after the move has cleared the chunk partial status.
# Inserts should wait and not insert a row when partial status being unset is
# invisible to it.
permutation "s3_wp_commit_on" "s1_move" "s2_insert_one" "s3_wp_commit_off" "s1_status" "s1_count"
