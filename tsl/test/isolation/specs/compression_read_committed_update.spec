# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# A waiting UPDATE or DELETE on a partially compressed chunk under READ
# COMMITTED must recheck the row version committed by the other session
# and act on it. See https://github.com/timescale/timescaledb/issues/10902

setup
{
 CREATE TABLE hyper(id int, ts timestamptz NOT NULL, dev int, val int);
 SELECT table_name FROM create_hypertable('hyper', 'ts', chunk_time_interval => interval '1 day');
 ALTER TABLE hyper SET (timescaledb.compress, timescaledb.compress_segmentby = 'dev');
 INSERT INTO hyper SELECT g, '2026-01-01'::timestamptz + g * interval '1 hour', g % 2, 1 FROM generate_series(0, 5) g;
 SELECT count(compress_chunk(c)) FROM show_chunks('hyper') c;
 INSERT INTO hyper VALUES (10, '2026-01-01 10:00', 2, 1);
}

teardown
{
 DROP TABLE hyper;
}

session "s1"
step "s1_begin"		{ BEGIN; }
step "s1_update"	{ UPDATE hyper SET val = val + 1 WHERE dev = 2; }
step "s1_commit"	{ COMMIT; }

session "s2"
step "s2_update"	{ UPDATE hyper SET val = val + 1 WHERE dev = 2 RETURNING id, dev, val; }
step "s2_delete"	{ DELETE FROM hyper WHERE dev = 2 RETURNING id, dev, val; }

session "s3"
step "s3_select"	{ SELECT id, dev, val FROM hyper ORDER BY id; }

permutation "s1_begin" "s1_update" "s2_update" "s1_commit" "s3_select"
permutation "s1_begin" "s1_update" "s2_delete" "s1_commit" "s3_select"
