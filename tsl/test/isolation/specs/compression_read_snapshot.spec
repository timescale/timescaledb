# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Without timescaledb.scan_uncompressed_chunks a compressed-only plan can miss
# rows. The legacy counts are wrong on purpose.

setup
{
    CREATE TABLE metrics_legacy(time INT NOT NULL, device INT, value INT);
    CREATE TABLE metrics_safe(time INT NOT NULL, device INT, value INT);
    SELECT create_hypertable('metrics_legacy', 'time', chunk_time_interval => 100);
    SELECT create_hypertable('metrics_safe', 'time', chunk_time_interval => 100);
    INSERT INTO metrics_legacy SELECT i, 1, i FROM generate_series(1,29) i;
    INSERT INTO metrics_safe SELECT i, 1, i FROM generate_series(1,29) i;
    ALTER TABLE metrics_legacy SET (timescaledb.compress,
                                    timescaledb.compress_segmentby = 'device');
    ALTER TABLE metrics_safe SET (timescaledb.compress,
                                  timescaledb.compress_segmentby = 'device');
    SELECT count(compress_chunk(c)) FROM show_chunks('metrics_legacy') c;
    SELECT count(compress_chunk(c)) FROM show_chunks('metrics_safe') c;

    -- Partial chunk: 29 compressed rows, 10 uncompressed rows.
    CREATE TABLE metrics_rr(time INT NOT NULL, device INT, value INT);
    SELECT create_hypertable('metrics_rr', 'time', chunk_time_interval => 100);
    INSERT INTO metrics_rr SELECT i, 1, i FROM generate_series(1,29) i;
    ALTER TABLE metrics_rr SET (timescaledb.compress,
                                timescaledb.compress_segmentby = 'device');
    SELECT count(compress_chunk(c)) FROM show_chunks('metrics_rr') c;
    INSERT INTO metrics_rr SELECT i, 1, i FROM generate_series(30,39) i;

    -- Two chunks with different column layouts, 99 device 1 rows each.
    CREATE TABLE metrics_dc(time INT NOT NULL, junk INT, device INT, value INT);
    SELECT create_hypertable('metrics_dc', 'time', chunk_time_interval => 100);
    INSERT INTO metrics_dc SELECT i, 0, d, i FROM generate_series(1,99) i, generate_series(1,2) d;
    ALTER TABLE metrics_dc DROP COLUMN junk;
    INSERT INTO metrics_dc SELECT i, d, i FROM generate_series(101,199) i, generate_series(1,2) d;
    ALTER TABLE metrics_dc SET (timescaledb.compress,
                                timescaledb.compress_segmentby = 'device');
    SELECT count(compress_chunk(c)) FROM show_chunks('metrics_dc') c;
}

teardown
{
    DROP TABLE metrics_legacy;
    DROP TABLE metrics_safe;
    DROP TABLE metrics_rr;
    DROP TABLE metrics_dc;
}

session "R"
step "r_legacy" {
    SET timescaledb.scan_uncompressed_chunks = off;
    SELECT count(*) FROM metrics_legacy WHERE device = 1 AND time > 20;
}
step "r_safe" {
    SET timescaledb.scan_uncompressed_chunks = on;
    SELECT count(*) FROM metrics_safe WHERE device = 1 AND time > 20;
}

# Sessions are reused across permutations, so every step sets the GUC.
step "rr_begin_legacy" {
    SET timescaledb.scan_uncompressed_chunks = off;
    BEGIN ISOLATION LEVEL REPEATABLE READ;
    SELECT count(*) FROM pg_class WHERE false;
}
step "rr_begin_safe" {
    SET timescaledb.scan_uncompressed_chunks = on;
    BEGIN ISOLATION LEVEL REPEATABLE READ;
    SELECT count(*) FROM pg_class WHERE false;
}
step "rr_count" { SELECT count(*) FROM metrics_rr; }
step "rr_commit" { COMMIT; }

# Rows read from the uncompressed part of the first chunk by this transaction.
step "dc_legacy" {
    SET timescaledb.scan_uncompressed_chunks = off;
    BEGIN;
    SELECT device, count(*) FROM metrics_dc WHERE device = 1 GROUP BY device;
}
step "dc_safe" {
    SET timescaledb.scan_uncompressed_chunks = on;
    BEGIN;
    SELECT device, count(*) FROM metrics_dc WHERE device = 1 GROUP BY device;
}
step "dc_rows_read" {
    SELECT seq_tup_read FROM pg_stat_xact_user_tables
    WHERE relid = (SELECT c FROM show_chunks('metrics_dc') c ORDER BY c LIMIT 1);
    COMMIT;
}

session "W"
step "w_legacy" { DELETE FROM metrics_legacy WHERE time <= 7; }
step "w_safe" { DELETE FROM metrics_safe WHERE time <= 7; }
step "w_dc" { DELETE FROM metrics_dc WHERE time <= 3 AND device = 1; }
step "w_recompress" {
    SET client_min_messages = error;
    SELECT count(compress_chunk(c)) FROM show_chunks('metrics_rr') c;
}

session "Control"
step "enable_legacy" { SELECT debug_waitpoint_enable('columnar_scan_uncompressed_part_decided'); }
step "release_legacy" { SELECT debug_waitpoint_release('columnar_scan_uncompressed_part_decided'); }
step "enable_safe" { SELECT debug_waitpoint_enable('columnar_scan_uncompressed_part_decided'); }
step "release_safe" { SELECT debug_waitpoint_release('columnar_scan_uncompressed_part_decided'); }
step "enable_dc" { SELECT debug_waitpoint_enable('columnar_scan_uncompressed_part_decided'); }
step "release_dc" { SELECT debug_waitpoint_release('columnar_scan_uncompressed_part_decided'); }

# The writer commits after the reader planned, before it executes.
permutation "enable_legacy" "r_legacy" "w_legacy" "release_legacy"
permutation "enable_safe" "r_safe" "w_safe" "release_safe"

# Recompression clears the partial flag after the snapshot was taken, but the
# snapshot still sees the 10 uncompressed rows.
permutation "rr_begin_legacy" "w_recompress" "rr_count" "rr_commit"
permutation "rr_begin_safe" "w_recompress" "rr_count" "rr_commit"

# Multi-chunk aggregate with different column layouts. The rows decompressed
# from the first chunk are read through the pooled uncompressed scans.
permutation "enable_dc" "dc_legacy" "w_dc" "release_dc" "dc_rows_read"
permutation "enable_dc" "dc_safe" "w_dc" "release_dc" "dc_rows_read"
