# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
  CREATE TABLE metrics (time timestamptz not null, device text, value float) WITH (tsdb.hypertable, tsdb.partition_column = 'time');
	INSERT INTO metrics (time, device, value) VALUES ('2025-01-01 00:00:00', 'd1', 0.1);
}

teardown {
  DROP TABLE metrics;
}


session "s1"
setup {
  SET timescaledb.enable_compressed_copy = true;
}

step "s1_begin" {
  BEGIN;
}

step "s1_copy" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-01-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s1_commit" {
  COMMIT;
}

session "s2"
setup {
  SET timescaledb.enable_compressed_copy = true;
}

step "s2_begin" {
  BEGIN;
}

step "s2_copy" {
COPY metrics FROM PROGRAM 'seq 10 | xargs -II echo 2025-01-01 0:00:00,d1,0.5' WITH (FORMAT CSV);
}

step "s2_commit" {
  COMMIT;
}

permutation "s1_begin" "s2_begin" "s1_copy" "s2_copy" "s1_commit" "s2_commit"

