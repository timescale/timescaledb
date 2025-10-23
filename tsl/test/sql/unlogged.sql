-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE UNLOGGED TABLE t1(time timestamptz) WITH (tsdb.hypertable);
SELECT oid::regclass,relpersistence from pg_class where oid = 't1'::regclass;

INSERT INTO t1 SELECT '2025-01-01';
SELECT count(compress_chunk(chunk)) FROM show_chunks('t1') chunk;

-- check that all objects related to hypertable are unlogged
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

ALTER TABLE t1 SET LOGGED;
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

SELECT show_chunks('t1') AS "CHUNK" \gset
ALTER TABLE :CHUNK SET UNLOGGED;

-- only chunk and compressed chunk should be unlogged now but not hypertable
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

ALTER TABLE t1 SET LOGGED;
-- all objects should be logged now
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

DROP TABLE t1;

CREATE TABLE t2(time timestamptz) WITH (tsdb.hypertable);
ALTER TABLE t2 SET UNLOGGED;
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

INSERT INTO t2 SELECT '2025-01-01';
-- new chunk should be unlogged
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

ALTER TABLE t2 SET LOGGED;
-- everything should be logged now
SELECT oid::regclass,relpersistence from pg_class where relpersistence = 'u' AND relnamespace <> 'pg_toast'::regnamespace ORDER BY oid::regclass;

