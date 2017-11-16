CREATE TABLE vacuum_test(time timestamp, temp float);

-- create hypertable with three chunks
SELECT create_hypertable('vacuum_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO vacuum_test VALUES ('2017-01-20T16:00:01', 17.5),
                               ('2017-01-21T16:00:01', 19.1),
                               ('2017-04-20T16:00:01', 89.5),
                               ('2017-04-21T16:00:01', 17.1),
                               ('2017-06-20T16:00:01', 18.5),
                               ('2017-06-21T16:00:01', 11.0);

-- no stats
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY schemaname, tablename;

VACUUM (VERBOSE, ANALYZE) vacuum_test;

-- stats should exist for all three chunks
SELECT tablename, attname, histogram_bounds, n_distinct FROM pg_stats
WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'
ORDER BY schemaname, tablename;

-- Run vacuum on a normal (non-hypertable) table
CREATE TABLE vacuum_norm(time timestamp, temp float);

INSERT INTO vacuum_norm VALUES ('2017-01-20T09:00:01', 17.5),
                               ('2017-01-21T09:00:01', 19.1),
                               ('2017-04-20T09:00:01', 89.5),
                               ('2017-04-21T09:00:01', 17.1),
                               ('2017-06-20T09:00:01', 18.5),
                               ('2017-06-21T09:00:01', 11.0);

VACUUM (VERBOSE, ANALYZE) vacuum_norm;
