# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Under READ COMMITTED, a statement must see all rows committed before
# its snapshot is taken, and the snapshot used for execution is taken
# after planning.
#
# For a fully compressed chunk, the planner only scans the compressed
# relation and leaves the (empty) non-compressed chunk relation out of
# the plan. If a concurrent session inserts into the chunk and commits
# after the reader made that decision, but before the reader takes its
# execution snapshot, the new row lands in the non-compressed relation
# that is not part of the plan. The reader does not return the row
# although its snapshot can see it.
#
# To show this, the reader scans the hypertable and a regular
# partitioned table in the same statement, that is, with the same
# snapshot. The writer inserts one row in each table in a single
# transaction. The reader must return both rows or none of them.
#
# The reader is suspended in the planner by holding a lock on the
# compressed chunk relation. The planner opens that relation after it
# has read the chunk's status and found the chunk to be fully
# compressed. The insert does not need the compressed chunk relation,
# so it is not blocked.

setup
{
 CREATE TABLE metrics(time timestamptz NOT NULL, device int, value int);
 SELECT table_name FROM create_hypertable('metrics', 'time', chunk_time_interval => interval '1 day');
 ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
 INSERT INTO metrics SELECT '2026-01-01'::timestamptz + g * interval '1 hour', 1, g FROM generate_series(1, 3) g;
 SELECT count(compress_chunk(c)) FROM show_chunks('metrics') c;

 CREATE TABLE plain(time timestamptz NOT NULL, device int, value int) PARTITION BY RANGE (time);
 CREATE TABLE plain_p1 PARTITION OF plain FOR VALUES FROM ('2026-01-01') TO ('2026-01-02');
 INSERT INTO plain SELECT '2026-01-01'::timestamptz + g * interval '1 hour', 1, g FROM generate_series(1, 3) g;

 -- Lock the compressed chunk relation of the only chunk of metrics
 CREATE FUNCTION lock_compressed_chunk() RETURNS void LANGUAGE plpgsql AS
 $$
 DECLARE
   compressed_chunk regclass;
 BEGIN
   SELECT cs.compress_relid INTO STRICT compressed_chunk
   FROM _timescaledb_catalog.compression_settings cs
   JOIN show_chunks('metrics') ch ON cs.relid = ch;
   EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', compressed_chunk);
 END
 $$;
}

teardown
{
 DROP TABLE metrics;
 DROP TABLE plain;
 DROP FUNCTION lock_compressed_chunk;
}

session "s1"
step "s1_select" {
 SELECT 'hypertable' AS source, value FROM metrics
 UNION ALL
 SELECT 'partitioned table', value FROM plain
 ORDER BY 1, 2;
}

session "s2"
step "s2_insert" {
 BEGIN;
 INSERT INTO metrics VALUES ('2026-01-01 10:00', 1, 100);
 INSERT INTO plain VALUES ('2026-01-01 10:00', 1, 100);
 COMMIT;
}

session "s3"
step "s3_lock"		{ BEGIN; SELECT lock_compressed_chunk(); }
step "s3_unlock"	{ COMMIT; }

# No concurrency: the reader sees both rows inserted by s2.
permutation "s2_insert" "s1_select"

# s2 commits while s1 is in the planner. s1 takes its snapshot after
# that, so it should return value 100 from both tables. Instead, it
# returns it from the partitioned table only. The second select is
# planned after the chunk became partially compressed and returns both.
permutation "s3_lock" "s1_select" "s2_insert" "s3_unlock" "s1_select"
