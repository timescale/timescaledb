# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Test that pg_stat_progress_create_index reports progress during
# hypertable index creation (GitHub issue #3157).

setup {
    CREATE TABLE progress_test(time int, temp float, location int);
    SELECT create_hypertable('progress_test', 'time', chunk_time_interval => 10, create_default_indexes => false);
    INSERT INTO progress_test VALUES (1, 23.4, 1),
        (11, 21.3, 2),
        (21, 19.5, 3);
}

teardown {
    DROP TABLE progress_test;
}

session "Waitpoints"
step "WPE"     { SELECT debug_waitpoint_enable('process_index_chunk_done'); }
step "WPR"     { SELECT debug_waitpoint_release('process_index_chunk_done'); }
step "WPE_MT"  { SELECT debug_waitpoint_enable('process_index_chunk_multitransaction_done'); }
step "WPR_MT"  { SELECT debug_waitpoint_release('process_index_chunk_multitransaction_done'); }

session "CREATE_INDEX"
step "CI"    { CREATE INDEX progress_test_idx ON progress_test(location); }
step "CI_MT" { CREATE INDEX progress_test_mt_idx ON progress_test(location) WITH (timescaledb.transaction_per_chunk); }

session "CHECK_PROGRESS"
step "check" {
    SELECT
        relid::regclass AS relation,
        CASE WHEN index_relid::text = index_relid::regclass::text
             THEN 'not yet visible'
             ELSE index_relid::regclass::text
        END AS index,
        command,
        phase,
        partitions_total,
        partitions_done
    FROM pg_stat_progress_create_index
    WHERE relid = 'progress_test'::regclass;
}

# Single-transaction index creation
permutation "WPE" "CI" "check" "WPR"
# Multi-transaction index creation
permutation "WPE_MT" "CI_MT" "check" "WPR_MT"
