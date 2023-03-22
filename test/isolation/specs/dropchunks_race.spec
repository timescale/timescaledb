# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

setup {
  DROP TABLE IF EXISTS dropchunks_race_t1;
  CREATE TABLE dropchunks_race_t1 (time timestamptz, device int, temp float);
  SELECT create_hypertable('dropchunks_race_t1', 'time', 'device', 2);
  INSERT INTO dropchunks_race_t1 VALUES ('2020-01-03 10:30', 1, 32.2);
}

teardown {
  DROP TABLE dropchunks_race_t1;
}

session "s1"
step "s1_drop_chunks"	{ SELECT count(*) FROM drop_chunks('dropchunks_race_t1', TIMESTAMPTZ '2020-03-01'); }

session "s2"
step "s2_drop_chunks"	{ SELECT count(*) FROM drop_chunks('dropchunks_race_t1', TIMESTAMPTZ '2020-03-01'); }

session "s3"
step "s3_chunks_found_wait"           { SELECT debug_waitpoint_enable('drop_chunks_chunks_found'); }
step "s3_chunks_found_release"      { SELECT debug_waitpoint_release('drop_chunks_chunks_found'); }
step "s3_show_missing_slices"	{ SELECT count(*) FROM _timescaledb_catalog.chunk_constraint WHERE dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice); }
step "s3_show_num_chunks"  { SELECT count(*) FROM show_chunks('dropchunks_race_t1') ORDER BY 1; }
step "s3_show_data"  { SELECT * FROM dropchunks_race_t1 ORDER BY 1; }

session "s4"
step "s4_chunks_dropped_wait"         { SELECT debug_waitpoint_enable('drop_chunks_end'); }
step "s4_chunks_dropped_release"      { SELECT debug_waitpoint_release('drop_chunks_end'); }

session "s5"
step "s5_insert_old_chunk" { INSERT INTO dropchunks_race_t1 VALUES ('2020-01-02 10:31', 1, 1.1); }
step "s5_insert_new_chunk" { INSERT INTO dropchunks_race_t1 VALUES ('2020-03-01 10:30', 1, 2.2); }

# Test race between two drop_chunks processes.
permutation "s3_chunks_found_wait" "s1_drop_chunks" "s2_drop_chunks" "s3_chunks_found_release" "s3_show_missing_slices" "s3_show_num_chunks" "s3_show_data"

# Test race between drop_chunks and an insert into a new chunk. The
# new chunk will share a slice with the chunk that is about to be
# dropped. The shared slice must persist after drop_chunks completes,
# or otherwise the new chunk will lack one slice.
permutation "s4_chunks_dropped_wait" "s1_drop_chunks" "s5_insert_new_chunk" "s4_chunks_dropped_release" "s3_show_missing_slices" "s3_show_num_chunks" "s3_show_data"

# Test race between drop_chunks and an insert into the chunk being
# concurrently dropped. The chunk and slices should be recreated.
permutation "s4_chunks_dropped_wait" "s1_drop_chunks" "s5_insert_old_chunk" "s4_chunks_dropped_release" "s3_show_missing_slices" "s3_show_num_chunks" "s3_show_data"
