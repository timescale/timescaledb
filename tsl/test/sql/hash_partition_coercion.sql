-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test hash partition chunk exclusion with cross-type comparisons.
-- Regression test for bug where SELECT with name literal on text column
-- returned 0 rows due to incorrect chunk exclusion.

CREATE TABLE hash_text(time timestamptz NOT NULL, device text, v int);
SELECT table_name FROM create_hypertable('hash_text', 'time');
SELECT column_name FROM add_dimension('hash_text', 'device', number_partitions => 3);

INSERT INTO hash_text VALUES ('2000-01-01', 'dev1', 1);

-- Both queries must return 1 row
SELECT count(*) FROM hash_text WHERE device = 'dev1';
SELECT count(*) FROM hash_text WHERE device = 'dev1'::name;

DROP TABLE hash_text;
