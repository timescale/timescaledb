# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
  CREATE TABLE metrics (time timestamptz not null, device text, value float) WITH (tsdb.hypertable, tsdb.partition_column = 'time');
	INSERT INTO metrics (time, device, value) VALUES ('2025-01-01 00:00:00', 'd1', 0.1);
  SELECT count(compress_chunk(chunk)) FROM show_chunks('metrics') chunk;
	INSERT INTO metrics (time, device, value) VALUES ('2025-02-01 00:00:00', 'd1', 0.1);
	INSERT INTO metrics (time, device, value) VALUES ('2025-03-01 00:00:00', 'd1', 0.1);
}

teardown {
  DROP TABLE metrics;
}


session "s1"
setup {
  SET timescaledb.enable_direct_compress_copy = true;
  SET timescaledb.enable_direct_compress_copy_client_sorted = true;
}

step "s1_begin" {
  BEGIN;
}

step "s1_copy" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-01-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s1_copy_february" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-02-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s1_commit" {
  COMMIT;
}

session "s2"
setup {
  SET timescaledb.enable_direct_compress_copy = true;
  SET timescaledb.enable_direct_compress_copy_client_sorted = true;
}

step "s2_begin" {
  BEGIN;
}

step "s2_copy" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-01-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s2_copy_march" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-03-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s2_commit" {
  COMMIT;
}

# test parallel insertions with direct compress copy in the same chunk
# chunk has to be precreated in this scenario otherwise there will be locking
permutation "s1_begin" "s2_begin" "s1_copy" "s2_copy" "s2_commit" "s1_commit"
# test parallel insertions into distinct chunks with direct compress copy
permutation "s1_begin" "s2_begin" "s1_copy_february" "s2_copy_march" "s2_commit" "s1_commit"

