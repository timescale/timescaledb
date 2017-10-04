CREATE TABLE reindex_test(time timestamp, temp float);
CREATE UNIQUE INDEX reindex_test_time_unique_idx ON reindex_test(time);

-- create hypertable with three chunks
SELECT create_hypertable('reindex_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO reindex_test VALUES ('2017-01-20T09:00:01', 17.5),
                                ('2017-01-21T09:00:01', 19.1),
                                ('2017-04-20T09:00:01', 89.5),
                                ('2017-04-21T09:00:01', 17.1),
                                ('2017-06-20T09:00:01', 18.5),
                                ('2017-06-21T09:00:01', 11.0);

\d+ reindex_test

-- show reindexing
REINDEX (VERBOSE) TABLE reindex_test;

\set ON_ERROR_STOP 0
-- this one currently doesn't recurse to chunks and instead gives an
-- error
REINDEX (VERBOSE) INDEX reindex_test_time_unique_idx;
\set ON_ERROR_STOP 1

-- show reindexing on a normal table
CREATE TABLE reindex_norm(time timestamp, temp float);
CREATE UNIQUE INDEX reindex_norm_time_unique_idx ON reindex_norm(time);

INSERT INTO reindex_norm VALUES ('2017-01-20T09:00:01', 17.5),
                                ('2017-01-21T09:00:01', 19.1),
                                ('2017-04-20T09:00:01', 89.5),
                                ('2017-04-21T09:00:01', 17.1),
                                ('2017-06-20T09:00:01', 18.5),
                                ('2017-06-21T09:00:01', 11.0);

REINDEX (VERBOSE) TABLE reindex_norm;
REINDEX (VERBOSE) INDEX reindex_norm_time_unique_idx;
