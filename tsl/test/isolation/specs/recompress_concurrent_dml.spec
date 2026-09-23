# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
    CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
    WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');

    INSERT INTO metrics
    SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float
    FROM generate_series(1,100) i;

    SELECT count(compress_chunk(c)) FROM show_chunks('metrics') c;

    INSERT INTO metrics VALUES ('2025-01-02 00:00:30', 'd1', 1.0);
    INSERT INTO metrics
    SELECT '2025-01-02 00:00:30'::timestamptz + (i || ' second')::interval, 'd1', i::float + 1000
    FROM generate_series(1,20) i;
}

teardown {
    DROP TABLE metrics;
}

session "s1"
step "s1_recompress" {
    SELECT count(_timescaledb_functions.recompress_chunk_segmentwise(c)) AS recompressed
    FROM show_chunks('metrics') c;
}
step "s1_status" {
    SELECT _timescaledb_functions.chunk_status_text(c) AS status FROM show_chunks('metrics') c;
}
step "s1_count" {
    SELECT count(*) FROM metrics;
}

session "s2"
step "s2_update" {
    UPDATE metrics SET value = -2.0 WHERE value = 1.0;
}
step "s2_delete" {
    DELETE FROM metrics WHERE value = 1.0;
}

session "s3"
step "s3_wp_on"  { SELECT debug_waitpoint_enable('recompress_after_delete'); }
step "s3_wp_off" { SELECT debug_waitpoint_release('recompress_after_delete'); }

# Test UPDATE/DELETE when they lose to recompression and are told the row moved to the columnstore.
permutation "s3_wp_on" "s1_recompress" "s2_update" "s3_wp_off" "s1_status" "s1_count"
permutation "s3_wp_on" "s1_recompress" "s2_delete" "s3_wp_off" "s1_status" "s1_count"
