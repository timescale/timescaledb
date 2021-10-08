-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE vacuum_test(time timestamp, temp float);

-- create hypertable with three chunks
SELECT create_hypertable('vacuum_test', 'time', chunk_time_interval => 2628000000000, create_default_indexes => false);

INSERT INTO vacuum_test VALUES ('2017-01-20T16:00:01', 17.5),
                               ('2017-01-21T16:00:01', 19.1),
                               ('2017-04-20T16:00:01', 89.5),
                               ('2017-04-21T16:00:01', 17.1),
                               ('2017-06-20T16:00:01', 18.5),
                               ('2017-06-21T16:00:01', 11.0);

-- no stats
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public' AND tablename LIKE 'vacuum_test'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

VACUUM ANALYZE vacuum_test;

-- stats should exist for all three chunks
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

-- stats should exist on parent hypertable
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public' AND tablename LIKE 'vacuum_test'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

DROP TABLE vacuum_test;

--test plain analyze (no_vacuum)
CREATE TABLE analyze_test(time timestamp, temp float);

SELECT create_hypertable('analyze_test', 'time', chunk_time_interval => 2628000000000, create_default_indexes => false);

INSERT INTO analyze_test VALUES ('2017-01-20T16:00:01', 17.5),
                               ('2017-01-21T16:00:01', 19.1),
                               ('2017-04-20T16:00:01', 89.5),
                               ('2017-04-21T16:00:01', 17.1),
                               ('2017-06-20T16:00:01', 18.5),
                               ('2017-06-21T16:00:01', 11.0);

-- no stats
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public' AND tablename LIKE 'analyze_test'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

ANALYZE VERBOSE analyze_test;

-- stats should exist for all three chunks
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

-- stats should exist on parent hypertable
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public' AND tablename LIKE 'analyze_test'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');
DROP TABLE analyze_test;

-- Run vacuum on a normal (non-hypertable) table
CREATE TABLE vacuum_norm(time timestamp, temp float);

INSERT INTO vacuum_norm VALUES ('2017-01-20T09:00:01', 17.5),
                               ('2017-01-21T09:00:01', 19.1),
                               ('2017-04-20T09:00:01', 89.5),
                               ('2017-04-21T09:00:01', 17.1),
                               ('2017-06-20T09:00:01', 18.5),
                               ('2017-06-21T09:00:01', 11.0);

VACUUM ANALYZE vacuum_norm;
DROP TABLE vacuum_norm;

--Similar to normal vacuum tests, but PG11 introduced ability to vacuum multiple tables at once, we make sure that works for hypertables as well.
CREATE TABLE vacuum_test(time timestamp, temp float);

-- create hypertable with three chunks
SELECT create_hypertable('vacuum_test', 'time', chunk_time_interval => 2628000000000, create_default_indexes => false);

INSERT INTO vacuum_test VALUES ('2017-01-20T16:00:01', 17.5),
                               ('2017-01-21T16:00:01', 19.1),
                               ('2017-04-20T16:00:01', 89.5),
                               ('2017-04-21T16:00:01', 17.1),
                               ('2017-06-20T16:00:01', 18.5),
                               ('2017-06-21T16:00:01', 11.0);
CREATE TABLE analyze_test(time timestamp, temp float);

SELECT create_hypertable('analyze_test', 'time', chunk_time_interval => 2628000000000, create_default_indexes => false);

INSERT INTO analyze_test VALUES ('2017-01-20T16:00:01', 17.5),
                               ('2017-01-21T16:00:01', 19.1),
                               ('2017-04-20T16:00:01', 89.5),
                               ('2017-04-21T16:00:01', 17.1),
                               ('2017-06-20T16:00:01', 18.5),
                               ('2017-06-21T16:00:01', 11.0);

CREATE TABLE vacuum_norm(time timestamp, temp float);

INSERT INTO vacuum_norm VALUES ('2017-01-20T09:00:01', 17.5),
                               ('2017-01-21T09:00:01', 19.1),
                               ('2017-04-20T09:00:01', 89.5),
                               ('2017-04-21T09:00:01', 17.1),
                               ('2017-06-20T09:00:01', 18.5),
                               ('2017-06-21T09:00:01', 11.0);
-- no stats
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

VACUUM ANALYZE vacuum_norm, vacuum_test, analyze_test;

-- stats should exist for all 6 chunks
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

-- stats should exist on parent hypertable and normal table
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = 'public'
ORDER BY tablename, attname, array_to_string(histogram_bounds, ',');

