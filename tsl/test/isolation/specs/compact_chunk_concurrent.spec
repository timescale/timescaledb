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

step "s2_commit" {
    COMMIT;
}

step "s2_rollback" {
    ROLLBACK;
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
