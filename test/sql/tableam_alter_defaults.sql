-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test support for setting table access method on hypertables using
-- ALTER TABLE for version 17 and later. This is in addition to the
-- tests in tableam_alter.sql.

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

SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';

-- Check that setting default access method of a normal table works.
ALTER TABLE test_table SET ACCESS METHOD DEFAULT;

-- Check that changing the access method and then changing it back
-- works.
ALTER TABLE test_table SET ACCESS METHOD testam;
ALTER TABLE test_table SET ACCESS METHOD DEFAULT;

-- Check that setting default access method of a hypertable works.
SELECT create_hypertable('test_table', by_range('time'));
ALTER TABLE test_table SET ACCESS METHOD DEFAULT;

SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';

-- Test setting the access method together with other options. This
-- should not generate an error.
ALTER TABLE test_table
      SET ACCESS METHOD testam,
      SET (autovacuum_vacuum_threshold = 100);

-- Add some rows to generate a chunk. This should get the access
-- method of the hypertable.
INSERT INTO test_table
SELECT ts, 10 * random(), 100 * random()
FROM generate_series('2001-01-01'::timestamp, '2001-01-14', '1d'::interval) as x(ts);

SELECT * FROM chunk_info WHERE hypertable = 'test_table';

-- Setting it to the default method after we have set it to a test
-- access method should work fine also on a hypertable.
SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';

ALTER TABLE test_table SET ACCESS METHOD DEFAULT;

SELECT cl.relname, amname
  FROM pg_class cl JOIN pg_am am ON cl.relam = am.oid
 WHERE cl.relname = 'test_table';

SELECT chunk FROM show_chunks('test_table') t(chunk) limit 1 \gset

ALTER TABLE :chunk SET ACCESS METHOD DEFAULT;

SELECT * FROM chunk_info WHERE hypertable = 'test_table';
