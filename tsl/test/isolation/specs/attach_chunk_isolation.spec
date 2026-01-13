# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
  DROP TABLE IF EXISTS attach_test;
  DROP TABLE IF EXISTS chunk_to_attach;
  CREATE TABLE attach_test (time timestamptz, device int, temp float);
  SELECT create_hypertable('attach_test', 'time');
  ALTER TABLE attach_test SET (timescaledb.enable_columnstore = true, timescaledb.orderby = 'time', timescaledb.segmentby = 'device');
  CREATE TABLE chunk_to_attach(time timestamptz NOT NULL, device int, temp float);
  CREATE TABLE temp_chunk(time timestamptz NOT NULL, device int, temp float);
  }

teardown {
  DROP TABLE IF EXISTS chunk_to_attach;
  DROP TABLE IF EXISTS temp_chunk;
  DROP TABLE attach_test;
}

session "s1"
step "s1b"  { BEGIN; }
step "s1_attach"  { CALL attach_chunk('attach_test', 'chunk_to_attach', jsonb_build_object('time', jsonb_build_array('2025-06-01', '2025-06-11'))); }
step "s1c"   { COMMIT; }

session "s2"
step "s2b"  { BEGIN; }
step "s2_attach"  { CALL attach_chunk('attach_test', 'temp_chunk', jsonb_build_object('time', jsonb_build_array('2025-05-01', '2025-05-11'))); }
step "s2_attach_same" { CALL attach_chunk('attach_test', 'chunk_to_attach', jsonb_build_object('time', jsonb_build_array('2025-06-01', '2025-06-11'))); }
step "s2_detach"  { CALL detach_chunk('temp_chunk'); }
step "s2_compress" { CALL convert_to_columnstore('temp_chunk'); }
step "s2c"   { COMMIT; }

session "s3"
step "s3_wait_before_lock" { SELECT debug_waitpoint_enable('chunk_attach_before_lock'); }
step "s3_release_lock" { SELECT debug_waitpoint_release('chunk_attach_before_lock'); }
step "s3_show_num_chunks" { SELECT count(*) FROM show_chunks('attach_test'); }
step "s3_show_num_rows" { SELECT count(*) FROM attach_test; }
step "s3_show_chunk_rows" { SELECT count(*) FROM chunk_to_attach; }
step "s3_wait_before_detach" { SELECT debug_waitpoint_enable('chunk_detach_before_lock'); }
step "s3_release_detach" { SELECT debug_waitpoint_release('chunk_detach_before_lock'); }

session "s4"
step "s4_insert" { INSERT INTO attach_test VALUES ('2025-06-03 00:15', 1, 31.5); }
step "s4_truncate" { TRUNCATE TABLE attach_test; }
step "s4_compress" { CALL convert_to_columnstore('chunk_to_attach'); }
step "s4_detach" { CALL detach_chunk('chunk_to_attach'); }
step "s4_drop" { DROP TABLE chunk_to_attach; }
step "s4_violate" { INSERT INTO chunk_to_attach VALUES ('2025-08-01 20:00', 1, 34); }

# Create another chunk with overlaping slices before attach takes locks so attach fails
permutation "s3_wait_before_lock" "s1b" "s1_attach" "s4_insert" "s3_release_lock" "s1c" "s3_show_num_chunks"
# Attach takes locks before trying to insert
permutation "s1b" "s1_attach" "s3_show_num_rows" "s4_insert" "s1c" "s3_show_num_chunks" "s3_show_num_rows"

# Insert row that violates chunk constraints into the chunk during attach
permutation "s3_wait_before_lock" "s1b" "s1_attach" "s3_show_chunk_rows" "s4_violate" "s3_release_lock" "s1c" "s3_show_num_chunks" "s3_show_chunk_rows"
# Attach takes locks first before trying to insert
permutation "s1b" "s3_show_num_chunks" "s1_attach" "s4_violate" "s1c" "s3_show_num_chunks" "s3_show_chunk_rows"

# Test dropping the chunk before and after attach_chunk takes locks
permutation "s3_wait_before_lock" "s1b" "s1_attach" "s4_drop" "s3_release_lock" "s1c"
permutation "s1b" "s3_show_num_chunks" "s1_attach" "s4_drop" "s1c" "s3_show_num_chunks" "s3_show_chunk_rows"

# Test truncating the hypertable before and after attach_chunk takes locks
permutation "s3_wait_before_lock" "s1b" "s1_attach" "s4_truncate" "s3_release_lock" "s1c" "s3_show_num_chunks"
permutation "s1b" "s3_show_num_chunks" "s1_attach" "s4_truncate" "s1c" "s3_show_num_chunks" "s3_show_chunk_rows"

# Compress the chunk during attach
permutation "s3_wait_before_lock" "s1b" "s1_attach" "s4_compress" "s3_release_lock" "s1c" "s3_show_num_chunks"
permutation "s1b" "s3_show_num_chunks" "s1_attach" "s4_compress" "s1c" "s3_show_num_chunks" "s3_show_chunk_rows"
# Try to compress another chunk during attach
permutation "s2_attach" "s3_wait_before_lock" "s1b" "s1_attach" "s2_compress" "s3_release_lock" "s1c"

# Attach two chunks at the same time
permutation "s1b" "s2b" "s3_show_num_chunks" "s1_attach" "s2_attach" "s1c" "s2c" "s3_show_num_chunks"
# Two sessions attaching the same chunk
permutation "s1b" "s2b" "s3_show_num_chunks" "s1_attach" "s2_attach_same" "s1c" "s2c" "s3_show_num_chunks"

# Both detach and attach the same chunk. Detach fails since the attaching transaction has not committed yet.
permutation "s3_wait_before_detach" "s1b" "s3_show_num_chunks" "s1_attach" "s4_detach" "s3_release_detach" "s1c" "s3_show_num_chunks"
# Detach another chunk
permutation "s2_attach" "s3_wait_before_lock" "s1b" "s1_attach" "s2_detach" "s3_release_lock" "s1c" "s3_show_num_chunks"
