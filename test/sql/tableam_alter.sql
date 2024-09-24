-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test support for setting table access method on hypertables using
-- ALTER TABLE. It should propagate to the chunks.
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD testam TYPE TABLE HANDLER heap_tableam_handler;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE VIEW chunk_info AS
SELECT hypertable_name AS hypertable,
       chunk_name AS chunk,
       amname
  FROM timescaledb_information.chunks ch
  JOIN pg_class cl ON (format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass = cl.oid)
  JOIN pg_am am ON (am.oid = cl.relam);

CREATE TABLE test_table (time timestamptz not null, device int, temp float);

SELECT create_hypertable('test_table', by_range('time'));

INSERT INTO test_table
SELECT ts, 10 * random(), 100 * random()
FROM generate_series('2001-01-01'::timestamp, '2001-02-01', '1d'::interval) as x(ts);

SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';
SELECT * FROM chunk_info WHERE hypertable = 'test_table';

-- Test setting the access method together with other options. This
-- should not generate an error.
ALTER TABLE test_table
      SET ACCESS METHOD testam,
      SET (autovacuum_vacuum_threshold = 100);

-- Create more chunks. These will use the new access method, but the
-- old chunks will use the old access method.
INSERT INTO test_table
SELECT ts, 10 * random(), 100 * random()
FROM generate_series('2001-02-01'::timestamp, '2001-03-01', '1d'::interval) as x(ts);

SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';
SELECT * FROM chunk_info WHERE hypertable = 'test_table';
