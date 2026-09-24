# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# A session that misses the invalidations of DROP EXTENSION and gets a
# full cache reset instead must not use the dropped catalog tables.

setup {
  CREATE EXTENSION IF NOT EXISTS timescaledb;
  CREATE TABLE plain_t (id int);
  INSERT INTO plain_t VALUES (1);
  CREATE TABLE ts_root (time timestamptz NOT NULL, id int);
  SELECT create_hypertable('ts_root', 'time', chunk_time_interval => interval '1 day');
  INSERT INTO ts_root SELECT '2030-01-01'::timestamptz + i * interval '1 day', i FROM generate_series(1, 20) i;
}

teardown {
  DROP TABLE plain_t;
  CREATE EXTENSION IF NOT EXISTS timescaledb;
}

session "s1"
step "s1_query_hypertable"	{ SELECT count(*) FROM ts_root; }
step "s1_wait"	{ SELECT pg_advisory_xact_lock(1); }
step "s1_query_plain"	{ SELECT count(*) FROM plain_t; }

session "s2"
setup	{ SET client_min_messages = warning; }
step "s2_drop_extension"	{ DROP EXTENSION timescaledb CASCADE; }

session "s3"
step "s3_lock"	{ SELECT pg_advisory_lock(1); }
step "s3_unlock"	{ SELECT pg_advisory_unlock(1); }

# s1 does not read the invalidation queue while it waits for the lock
permutation "s1_query_hypertable" "s3_lock" "s1_wait" "s2_drop_extension" "s3_unlock" "s1_query_plain"
