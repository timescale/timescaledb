# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

setup {
  DROP TABLE IF EXISTS measurements;
  CREATE TABLE measurements (time timestamptz, device int, temp float);
  SELECT create_hypertable('measurements', 'time', 'device', 2);
  INSERT INTO measurements VALUES ('2020-01-03 10:30', 1, 1.0), ('2021-01-03 10:30', 2, 2.0);
}

teardown {
  DROP TABLE measurements;
}

# Test concurrent querying and drop chunks. The wait point happens
# after chunks have been found for table expansion, but before the
# chunks are locked. Because one chunk will dropped before the lock is
# acqurired, the chunk should also be ignored.

session "s1"
step "s1_wp_enable" { SELECT debug_waitpoint_enable('hypertable_expansion_before_lock_chunk'); }
step "s1_wp_release" { SELECT debug_waitpoint_release('hypertable_expansion_before_lock_chunk'); }
step "s1_drop_chunks" { SELECT count(*) FROM drop_chunks('measurements', TIMESTAMPTZ '2020-03-01'); }

session "s2"
step "s2_show_num_chunks"  { SELECT count(*) FROM show_chunks('measurements') ORDER BY 1; }
step "s2_query"  { SELECT * FROM measurements ORDER BY 1; }

permutation "s2_query" "s1_wp_enable" "s2_query" "s1_drop_chunks" "s1_wp_release" "s2_show_num_chunks"
