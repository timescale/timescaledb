CREATE TABLE cluster_test(time timestamptz, temp float, location int);

SELECT create_hypertable('cluster_test', 'time', chunk_time_interval => interval '1 day');

-- Show default indexes
SELECT * FROM test.show_indexes('cluster_test');

-- Create two chunks
INSERT INTO cluster_test VALUES ('2017-01-20T09:00:01', 23.4, 1),
       ('2017-01-21T09:00:01', 21.3, 2);

-- Run cluster
CLUSTER VERBOSE cluster_test USING cluster_test_time_idx;

-- Create a third chunk
INSERT INTO cluster_test VALUES ('2017-01-22T09:00:01', 19.5, 3);

-- Show clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster just our table
CLUSTER VERBOSE cluster_test;

-- Show clustered indexes, including new chunk
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;

-- Recluster all tables (although will only be our test table)
CLUSTER VERBOSE;

-- Change the clustered index
CREATE INDEX ON cluster_test (time, location);

CLUSTER VERBOSE cluster_test using cluster_test_time_location_idx;

-- Show updated clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true;
