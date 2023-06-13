-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE reloptions_test(time integer, temp float8, color integer)
WITH (fillfactor=75, autovacuum_vacuum_threshold=100);

SELECT create_hypertable('reloptions_test', 'time', chunk_time_interval => 3);

INSERT INTO reloptions_test VALUES (4, 24.3, 1), (9, 13.3, 2);

-- Show that reloptions are inherited by chunks
SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

-- Alter reloptions
ALTER TABLE reloptions_test SET (fillfactor=80, parallel_workers=8);

\set ON_ERROR_STOP 0
ALTER TABLE reloptions_test SET (fillfactor=80), SET (parallel_workers=8);
\set ON_ERROR_STOP 1

SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

ALTER TABLE reloptions_test RESET (fillfactor);

SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

-- Test reloptions on a regular table
CREATE TABLE reloptions_test2(time integer, temp float8, color integer);
ALTER TABLE reloptions_test2 SET (fillfactor=80, parallel_workers=8);
ALTER TABLE reloptions_test2 SET (fillfactor=80), SET (parallel_workers=8);
DROP TABLE reloptions_test2;

-- Test hypertable creation using storage options

-- time column
CREATE TABLE test(time timestamptz NOT NULL, device_id int);
ALTER TABLE test
SET (timescaledb.hypertable, timescaledb.time_column = 'time');

INSERT INTO test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test');

DROP TABLE test;

-- partitioning column
CREATE TABLE test(time timestamptz NOT NULL, device_id int);
ALTER TABLE test
SET (timescaledb.hypertable,
     timescaledb.time_column = 'time',
     timescaledb.partitioning_column = 'device_id',
     timescaledb.number_partitions = '4');

INSERT INTO test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test VALUES ('2004-10-20 00:00:00+00', 2);
INSERT INTO test VALUES ('2004-10-30 00:00:00+00', 3);
INSERT INTO test VALUES ('2004-11-10 00:00:00+00', 4);

SELECT show_chunks('test');

DROP TABLE test;

-- time_partitioning_func
CREATE TABLE test(time text NOT NULL);

CREATE OR REPLACE FUNCTION time_partfunc(source text)
    RETURNS TIMESTAMPTZ LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN timezone('UTC', to_timestamp(source));
END
$BODY$;

ALTER TABLE test
SET (timescaledb.hypertable,
     timescaledb.time_column = 'time',
     timescaledb.time_partitioning_func = 'time_partfunc');

DROP TABLE test;

-- chunk_time_interval

-- integer
CREATE TABLE test(time timestamptz NOT NULL, device_id int);
ALTER TABLE test
SET (timescaledb.hypertable,
     timescaledb.time_column = 'time',
     timescaledb.chunk_time_interval = '123');

INSERT INTO test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test');

SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'test'
ORDER BY d.id;

DROP TABLE test;

-- interval
CREATE TABLE test(time timestamptz NOT NULL, device_id int);
ALTER TABLE test
SET (timescaledb.hypertable,
     timescaledb.time_column = 'time',
     timescaledb.chunk_time_interval = '1 day');

INSERT INTO test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test');

SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'test'
ORDER BY d.id;

DROP TABLE test;

-- test CREATE TABLE WITH ()

-- time column
CREATE TABLE test_with(time timestamptz NOT NULL, device_id int)
WITH (timescaledb.hypertable, timescaledb.time_column = 'time');

INSERT INTO test_with VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test_with VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test_with');

DROP TABLE test_with;

-- interval
CREATE TABLE test_with(time timestamptz NOT NULL, device_id int)
WITH (timescaledb.hypertable,
      timescaledb.time_column = 'time',
      timescaledb.chunk_time_interval = '1 day');

INSERT INTO test_with VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test_with VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test_with');

SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'test_with'
ORDER BY d.id;

DROP TABLE test_with;

-- Test ALTER TABLE SET (chunk_time_interval)

CREATE TABLE test(time timestamptz NOT NULL, device_id int);
ALTER TABLE test
SET (timescaledb.hypertable,
     timescaledb.time_column = 'time',
     timescaledb.chunk_time_interval = '123');

INSERT INTO test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO test VALUES ('2004-10-20 00:00:00+00', 2);

SELECT show_chunks('test');

SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'test'
ORDER BY d.id;

ALTER TABLE test
SET (timescaledb.chunk_time_interval = '321');

SELECT interval_length
FROM _timescaledb_catalog.dimension d
LEFT JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.schema_name = 'public' AND h.table_name = 'test'
ORDER BY d.id;

DROP TABLE test;
