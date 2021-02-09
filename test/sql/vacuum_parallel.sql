-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- PG13 introduced parallel VACUUM functionality. It gets invoked when a table
-- has two or more indexes on it. Read up more at
-- https://www.postgresql.org/docs/13/sql-vacuum.html#PARALLEL

CREATE TABLE vacuum_test(time timestamp, temp1 float, temp2 int);

-- create hypertable with chunks
SELECT create_hypertable('vacuum_test', 'time', chunk_time_interval => 2628000000000);

-- create additional indexes on the temp columns
CREATE INDEX vt_temp1 on vacuum_test (temp1);
CREATE INDEX vt_temp2 on vacuum_test (temp2);

-- parallel vacuum needs the index size to be larger than 512KB to kick in
INSERT INTO vacuum_test SELECT TIMESTAMP 'epoch' + (i * INTERVAL '100 second'),
                i, i+1 FROM generate_series(1, 100000) as T(i);

-- we should see two parallel workers for each chunk
VACUUM (PARALLEL 3, VERBOSE) vacuum_test;

DROP TABLE vacuum_test;
