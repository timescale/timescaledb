-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- PG13 introduced parallel VACUUM functionality. It gets invoked when a table
-- has two or more indexes on it. Read up more at
-- https://www.postgresql.org/docs/13/sql-vacuum.html#PARALLEL

CREATE TABLE vacuum_test(time timestamp NOT NULL, temp1 float, temp2 int);

-- create hypertable
-- we create chunks in public schema cause otherwise we would need
-- elevated privileges to create indexes directly
SELECT create_hypertable('vacuum_test', 'time', create_default_indexes => false, associated_schema_name => 'public');

-- parallel vacuum needs the index size to be larger than min_parallel_index_scan_size to kick in
SET min_parallel_index_scan_size TO 0;
INSERT INTO vacuum_test SELECT TIMESTAMP 'epoch' + (i * INTERVAL '4h'),
                i, i+1 FROM generate_series(1, 100) as T(i);

-- create indexes on the temp columns
-- we create indexes manually because otherwise vacuum verbose output
-- would be different between 13.2 and 13.3+
-- 13.2 would try to vacuum the parent table index too while 13.3+ wouldn't
CREATE INDEX ON _hyper_1_1_chunk(time);
CREATE INDEX ON _hyper_1_1_chunk(temp1);
CREATE INDEX ON _hyper_1_1_chunk(temp2);
CREATE INDEX ON _hyper_1_2_chunk(time);
CREATE INDEX ON _hyper_1_2_chunk(temp1);
CREATE INDEX ON _hyper_1_2_chunk(temp2);
CREATE INDEX ON _hyper_1_3_chunk(time);
CREATE INDEX ON _hyper_1_3_chunk(temp1);
CREATE INDEX ON _hyper_1_3_chunk(temp2);

-- INSERT only will not trigger vacuum on indexes for PG13.3+
UPDATE vacuum_test SET time = time + '1s'::interval, temp1 = random(), temp2 = random();

-- we should see two parallel workers for each chunk
VACUUM (PARALLEL 3) vacuum_test;

DROP TABLE vacuum_test;
