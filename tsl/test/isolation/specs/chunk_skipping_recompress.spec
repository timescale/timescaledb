# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test chunk skipping ranges stay invalid when segmentwise recompression
# cannot clear the partial status of a chunk
###

setup {
   SET timescaledb.enable_chunk_skipping = on;
   CREATE TABLE skip_data(time timestamptz NOT NULL, device int, value int);
   SELECT FROM create_hypertable('skip_data', 'time', chunk_time_interval => interval '1 day');
   SELECT FROM enable_chunk_skipping('skip_data', 'value');
   ALTER TABLE skip_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
   INSERT INTO skip_data VALUES ('2025-01-01', 1, 10), ('2025-01-01 01:00', 1, 20);
   SELECT count(compress_chunk(c)) FROM show_chunks('skip_data') c;
   INSERT INTO skip_data VALUES ('2025-01-01 02:00', 1, 30);
}

teardown {
   DROP TABLE skip_data;
}

session "s1"
setup {
   SET timescaledb.enable_chunk_skipping = on;
}
step "s1_recompress" {
   SELECT count(_timescaledb_functions.recompress_chunk_segmentwise(c)) FROM show_chunks('skip_data') c;
}
step "s1_show_ranges" {
   SELECT c.status, s.valid, s.range_start, s.range_end
   FROM _timescaledb_catalog.chunk_column_stats s
   JOIN _timescaledb_catalog.chunk c ON c.id = s.chunk_id;
}

session "s2"
setup {
   SET timescaledb.enable_chunk_skipping = on;
}
step "s2_insert" {
   INSERT INTO skip_data VALUES ('2025-01-01 03:00', 2, 1000);
}
step "s2_query" {
   SELECT count(*) FROM skip_data WHERE value = 1000;
   SELECT count(*) FROM skip_data WHERE time > now() - interval '100 years' AND value = 1000;
}

session "s3"
step "s3_lock" {
   BEGIN;
   LOCK TABLE skip_data IN ROW EXCLUSIVE MODE;
   INSERT INTO skip_data VALUES ('2025-01-01 04:00', 3, 15);
}
step "s3_commit" {
   COMMIT;
}

# s3 keeps recompression from taking the lock it needs to clear the partial status
permutation "s3_lock" "s1_recompress" "s3_commit" "s1_show_ranges" "s2_insert" "s1_show_ranges" "s2_query"

# Recompression that clears the partial status recalculates the ranges
permutation "s1_recompress" "s1_show_ranges" "s2_query"
