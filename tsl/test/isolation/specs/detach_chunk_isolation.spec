# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
  DROP TABLE IF EXISTS detach_test;
  DROP TABLE IF EXISTS chunk_to_detach;
  CREATE TABLE detach_test (time timestamptz, device int, temp float);
  SELECT create_hypertable('detach_test', 'time');
  CREATE TABLE chunk_to_detach(time timestamptz NOT NULL, device int, temp float);
  CALL attach_chunk('detach_test', 'chunk_to_detach', jsonb_build_object('time', jsonb_build_array('2025-06-01', '2025-06-11')));
  INSERT INTO detach_test VALUES ('2025-06-02 10:30', 1, 31.5);
}

teardown {
  DROP TABLE IF EXISTS chunk_to_detach;
  DROP TABLE detach_test;
}

session "s1"
step "s1b"  { BEGIN; }
step "s1_detach"  { CALL detach_chunk('chunk_to_detach'); }
step "s1c"   { COMMIT; }

session "s2"
step "s2_wait_before_lock" { SELECT debug_waitpoint_enable('chunk_detach_before_lock'); }
step "s2_release_lock" { SELECT debug_waitpoint_release('chunk_detach_before_lock'); }
step "s2_show_num_chunks" { SELECT count(*) FROM show_chunks('detach_test'); }
step "s2_show_num_rows" { SELECT count(*) FROM detach_test; }
step "s2_show_chunk_rows" { SELECT count(*) FROM chunk_to_detach; }


session "s3"
step "s3_insert" { INSERT INTO detach_test VALUES ('2025-06-02 10:30', 1, 31.5); }
step "s3_truncate" { TRUNCATE TABLE detach_test; }
step "s3_drop_chunk" { SELECT count(*) FROM drop_chunks('detach_test', TIMESTAMPTZ '2025-07-01'); }

session "s4"
step "s4b"  { BEGIN; }
step "s4_detach"  { CALL detach_chunk('chunk_to_detach'); }
step "s4c"   { COMMIT; }

# Insert rows into the chunk before detach_chunk takes locks, also try to insert after locks but before detach commits.
permutation "s2_wait_before_lock" "s1b" "s1_detach" "s2_show_num_rows" "s3_insert" "s2_show_num_rows" "s2_release_lock" "s3_insert" "s1c" "s2_show_num_rows" "s2_show_num_chunks" "s2_show_chunk_rows"

# Truncate the hypertable before detach_chunk takes locks
permutation "s2_wait_before_lock" "s1b" "s1_detach" "s2_show_num_rows" "s2_show_num_chunks" "s3_truncate"  "s2_release_lock" "s1c" "s2_show_num_chunks" "s2_show_chunk_rows"
# Truncate the hypertable after detach_chunk takes locks
permutation "s1b"  "s2_show_num_chunks" "s2_show_num_rows" "s1_detach" "s3_truncate" "s1c" "s2_show_num_chunks" "s2_show_chunk_rows"

# Drop the chunk before detach_chunk takes locks
permutation "s2_wait_before_lock" "s1b" "s1_detach" "s2_show_num_chunks" "s3_drop_chunk" "s2_show_num_chunks" "s2_release_lock" "s1c" "s2_show_num_chunks" "s2_show_chunk_rows"
# Drop the chunk after detach_chunk takes locks
permutation "s1b" "s1_detach" "s2_show_num_chunks" "s3_drop_chunk" "s2_show_num_chunks" "s1c" "s2_show_num_chunks" "s2_show_chunk_rows"

# Two sessions detaching the same chunk
permutation "s1b" "s4b" "s1_detach" "s4_detach" "s1c" "s4c"
