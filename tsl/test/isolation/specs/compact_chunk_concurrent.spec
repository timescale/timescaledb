# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test compact_chunk conflicts with concurrent DML transactions
###

setup {
    CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
    WITH (tsdb.hypertable, tsdb.orderby='time');

    SET timescaledb.enable_direct_compress_insert = true;

    -- Insert overlapping batches so compact_chunk has work to do
    INSERT INTO metrics
    SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float
    FROM generate_series(1,2000) i;

    INSERT INTO metrics
    SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', (i + 0.5)::float
    FROM generate_series(1,2000) i;

    -- Second table for concurrent DML rescan test:
    CREATE TABLE metrics_dml_rescan (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
    WITH (tsdb.hypertable, tsdb.orderby='time');

    INSERT INTO metrics_dml_rescan
    SELECT '2025-01-02 00:01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
    FROM generate_series(1,500) i;

    INSERT INTO metrics_dml_rescan
    SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
    FROM generate_series(1,2000) i;

    INSERT INTO metrics_dml_rescan
    SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', (i + 0.5)::float
    FROM generate_series(1,2000) i;
}

teardown {
    DROP TABLE metrics;
    DROP TABLE metrics_dml_rescan;
}

session "s1"
step "s1_compact" {
    SELECT count(_timescaledb_functions.compact_chunk(chunk)) AS compact
    FROM show_chunks('metrics') chunk;
}

step "s1_show_status" {
    SELECT _timescaledb_functions.chunk_status_text(chunk) AS status
    FROM show_chunks('metrics') chunk;
}

step "s1_count" {
    SELECT count(*) FROM metrics;
}

step "s1_compact_rescan" {
    SELECT count(_timescaledb_functions.compact_chunk(chunk)) AS compact
    FROM show_chunks('metrics_dml_rescan') chunk;
}

step "s1_rescan_status" {
    SELECT _timescaledb_functions.chunk_status_text(chunk) AS status
    FROM show_chunks('metrics_dml_rescan') chunk;
}

step "s1_rescan_count" {
    SELECT count(*) FROM metrics_dml_rescan;
}

session "s2"
step "s2_begin" {
    BEGIN;
}

step "s2_begin_rr" {
    BEGIN ISOLATION LEVEL REPEATABLE READ;
}

step "s2_insert" {
    INSERT INTO metrics VALUES ('2025-01-02 12:00', 'd1', -1.0);
}

step "s2_direct_insert" {
    SET timescaledb.enable_direct_compress_insert = true;
    INSERT INTO metrics
    SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', (i + 0.1)::float
    FROM generate_series(1,100) i;
}

step "s2_update" {
    UPDATE metrics SET value = -1.0 WHERE value = 1.0;
}

step "s2_delete" {
    DELETE FROM metrics WHERE value = 1.0;
}

step "s2_select" {
    SELECT count(*) FROM metrics;
}

step "s2_update_rescan" {
    UPDATE metrics_dml_rescan SET value = value + 0.001 WHERE device = 'd1';
}

step "s2_check_rescan_update" {
    SELECT count(*) AS total,
           count(*) FILTER (WHERE value != round(value::numeric, 0)
                             AND value != round(value::numeric, 1)) AS updated
    FROM metrics_dml_rescan;
}

step "s2_commit" {
    COMMIT;
}

step "s2_rollback" {
    ROLLBACK;
}

session "s3"
step "s3_wp_enable" {
    SELECT debug_waitpoint_enable('compact_chunk_after_find_overlaps');
}

step "s3_wp_release" {
    SELECT debug_waitpoint_release('compact_chunk_after_find_overlaps');
}

step "s3_wp_enable_after_delete" {
    SELECT debug_waitpoint_enable('compact_chunk_after_batch_delete');
}

step "s3_wp_release_after_delete" {
    SELECT debug_waitpoint_release('compact_chunk_after_batch_delete');
}

step "s3_wp_enable_dml_batch" {
    SELECT debug_waitpoint_enable('decompress_batches_after_batch');
}

step "s3_wp_release_dml_batch" {
    SELECT debug_waitpoint_release('decompress_batches_after_batch');
}


# compact_chunk should not block concurrent reads
permutation "s2_begin" "s2_select" "s1_compact" "s2_commit" "s1_show_status" "s1_count"

# compact_chunk should detect concurrent insert and error
permutation "s2_begin" "s2_insert" "s1_compact" "s2_commit" "s1_show_status" "s1_count"

# compact_chunk should detect concurrent update and error
permutation "s2_begin" "s2_update" "s1_compact" "s2_commit" "s1_show_status" "s1_count"

# compact_chunk should detect concurrent delete and error
permutation "s2_begin" "s2_delete" "s1_compact" "s2_commit" "s1_show_status" "s1_count"

# compact_chunk doesn't run on partial chunks; committed insert makes the chunk partial
permutation "s2_begin" "s2_insert" "s2_commit" "s1_compact" "s1_show_status" "s1_count"

# compact_chunk should succeed after concurrent DML rolls back
permutation "s2_begin" "s2_insert" "s2_rollback" "s1_compact" "s1_show_status" "s1_count"

# compact_chunk should succeed after committed direct compress insert (chunk stays fully compressed)
permutation "s2_begin" "s2_direct_insert" "s2_commit" "s1_compact" "s1_show_status" "s1_count"

# concurrent update triggers serialization error:
permutation "s3_wp_enable" "s1_compact" "s2_update" "s3_wp_release" "s1_show_status" "s1_count"

# concurrent delete triggers serialization error
permutation "s3_wp_enable" "s1_compact" "s2_delete" "s3_wp_release" "s1_show_status" "s1_count"

# concurrent update after batch delete triggers serialization error
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_update" "s3_wp_release_after_delete" "s1_show_status" "s1_count"

# concurrent delete after batch delete triggers serialization error
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_delete" "s3_wp_release_after_delete" "s1_show_status" "s1_count"

# Repeatable Read DML: concurrent update after batch delete.
# s2 runs in Repeatable Read and should get a serialization error.
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_begin_rr" "s2_update" "s3_wp_release_after_delete" "s2_commit" "s1_show_status" "s1_count"

# Repeatable Read DML: concurrent delete after batch delete.
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_begin_rr" "s2_delete" "s3_wp_release_after_delete" "s2_commit" "s1_show_status" "s1_count"

# DML mid-scan restart with batch already processed:
# s1 compaction pauses after deleting overlapping batches. s2 UPDATE starts,
# processes the non-overlapping batch (early times), then pauses at DML
# waitpoint. s2 resumes, hits compaction's row lock on the overlapping batch,
# waits. s1 resumes and commits. s2 gets TM_Updated, restarts scan.
# The UPDATE should take effect on all rows, chunk should be PARTIAL.
permutation "s3_wp_enable_after_delete" "s3_wp_enable_dml_batch" "s1_compact_rescan" "s2_update_rescan" "s3_wp_release_dml_batch" "s3_wp_release_after_delete" "s2_check_rescan_update" "s1_rescan_status" "s1_rescan_count"
