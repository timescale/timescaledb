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
}

teardown {
    DROP TABLE metrics;
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

step "s2_check_update" {
    SELECT count(*) AS updated_rows FROM metrics WHERE value = -1.0;
}

step "s2_check_delete" {
    SELECT count(*) AS remaining FROM metrics WHERE value = 1.0;
}

step "s2_select" {
    SELECT count(*) FROM metrics;
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

# DML scan restart safety: concurrent update completes correctly after
# compaction modifies batches mid-scan. The update should actually take
# effect (value=1.0 changed to -1.0) even though the scan restarted.
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_update" "s3_wp_release_after_delete" "s2_check_update" "s1_count"

# DML scan restart safety: concurrent delete completes correctly after
# compaction modifies batches mid-scan. The row with value=1.0 should
# actually be deleted.
permutation "s3_wp_enable_after_delete" "s1_compact" "s2_delete" "s3_wp_release_after_delete" "s2_check_delete" "s1_count"

# DML mid-scan restart: s1 starts compaction first (passes lock check),
# pauses after finding overlaps. s2 starts DML, processes one batch,
# pauses. s1 resumes and modifies batches. s2 resumes, hits TM_Updated
# on a compacted batch, restarts scan. Data should be correct.
permutation "s3_wp_enable" "s3_wp_enable_dml_batch" "s1_compact" "s2_update" "s3_wp_release" "s3_wp_release_dml_batch" "s2_check_update" "s1_count"

permutation "s3_wp_enable" "s3_wp_enable_dml_batch" "s1_compact" "s2_delete" "s3_wp_release" "s3_wp_release_dml_batch" "s2_check_delete" "s1_count"
