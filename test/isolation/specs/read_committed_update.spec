# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# A waiting UPDATE or DELETE under READ COMMITTED must recheck the row
# version committed by the other session and act on it, like it does
# on a plain table. See https://github.com/timescale/timescaledb/issues/10902

setup
{
 CREATE TABLE hyper(id text, ts timestamp NOT NULL, val int);
 CREATE INDEX ON hyper(ts);
 SELECT table_name FROM create_hypertable('hyper', 'ts', chunk_time_interval => interval '1 day');
 INSERT INTO hyper VALUES ('a', '2026-10-27 08:53:48', 1);

 CREATE TABLE hyper_pk(id text, ts timestamp NOT NULL, val int, PRIMARY KEY (id, ts));
 SELECT table_name FROM create_hypertable('hyper_pk', 'ts', chunk_time_interval => interval '1 day');
 INSERT INTO hyper_pk VALUES ('a', '2026-10-27 08:53:48', 1);

 CREATE TABLE src(id text, val int);
 INSERT INTO src VALUES ('a', 100);
}

teardown
{
 DROP TABLE hyper;
 DROP TABLE hyper_pk;
 DROP TABLE src;
}

session "s1"
step "s1_begin"		{ BEGIN; }
step "s1_update"	{ UPDATE hyper SET val = val + 1 WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s1_update_pk"	{ UPDATE hyper_pk SET val = val + 1 WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s1_update_val"	{ UPDATE hyper SET val = 10 WHERE id = 'a' RETURNING *; }
step "s1_delete"	{ DELETE FROM hyper WHERE id = 'a' RETURNING *; }
step "s1_commit"	{ COMMIT; }

session "s2"
step "s2_update"	{ UPDATE hyper SET val = val + 1 WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s2_update_pk"	{ UPDATE hyper_pk SET val = val + 1 WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s2_update_val1"	{ UPDATE hyper SET val = val + 1 WHERE id = 'a' AND val = 1 RETURNING *; }
step "s2_delete"	{ DELETE FROM hyper WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s2_delete_pk"	{ DELETE FROM hyper_pk WHERE id = 'a' AND ts = '2026-10-27 08:53:48' RETURNING *; }
step "s2_update_from"	{ UPDATE hyper SET val = hyper.val + src.val FROM src WHERE hyper.id = src.id RETURNING hyper.*; }

session "s3"
step "s3_select"	{ SELECT * FROM hyper ORDER BY id; }
step "s3_select_pk"	{ SELECT * FROM hyper_pk ORDER BY id; }

# Waiting UPDATE/DELETE must act on the updated row
permutation "s1_begin" "s1_update" "s2_update" "s1_commit" "s3_select"
permutation "s1_begin" "s1_update" "s2_delete" "s1_commit" "s3_select"
permutation "s1_begin" "s1_update_pk" "s2_update_pk" "s1_commit" "s3_select_pk"
permutation "s1_begin" "s1_update_pk" "s2_delete_pk" "s1_commit" "s3_select_pk"
permutation "s1_begin" "s1_update" "s2_update_from" "s1_commit" "s3_select"

# Waiting UPDATE must skip the row when the new version fails its quals
permutation "s1_begin" "s1_update_val" "s2_update_val1" "s1_commit" "s3_select"

# Waiting UPDATE must skip a row deleted by the other session
permutation "s1_begin" "s1_delete" "s2_update" "s1_commit" "s3_select"
