-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- REPACK generalizes CLUSTER and VACUUM FULL on PostgreSQL 19. Make sure it
-- rewrites every chunk of a hypertable instead of only the empty root table.

CREATE TABLE repack_test(time timestamptz, temp float, location int);
SELECT create_hypertable('repack_test', 'time', chunk_time_interval => interval '1 day');

INSERT INTO repack_test
SELECT t, 1.0, 1
FROM generate_series('2017-01-20'::timestamptz, '2017-01-24', '1 hour') t;

SELECT count(*) AS num_chunks FROM show_chunks('repack_test');

-- Remember each chunk's storage file so we can tell it was rewritten
CREATE TABLE repack_files AS
SELECT ch AS chunk, pg_relation_filenode(ch) AS filenode
FROM show_chunks('repack_test') ch;

-- Plain REPACK rewrites every chunk without ordering (like VACUUM FULL)
REPACK repack_test;

SELECT count(*) AS chunks_rewritten
FROM repack_files f
WHERE pg_relation_filenode(f.chunk) <> f.filenode;

-- REPACK ... USING INDEX orders each chunk by the index and marks it clustered
REPACK repack_test USING INDEX repack_test_time_idx;

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered
ORDER BY 1;

-- An implicit REPACK ... USING INDEX reuses the previously clustered index
REPACK repack_test USING INDEX;

-- Repacking a single chunk directly is handled by PostgreSQL
SELECT ch::regclass::text AS chunk FROM show_chunks('repack_test') ch ORDER BY 1 LIMIT 1 \gset
REPACK :chunk;

-- VACUUM FULL still reaches every chunk (it stays a VacuumStmt, not a RepackStmt)
SELECT ch AS chunk, pg_relation_filenode(ch) AS filenode
FROM show_chunks('repack_test') ch ORDER BY 1 LIMIT 1 \gset
VACUUM FULL repack_test;
SELECT pg_relation_filenode(:'chunk') <> :filenode AS chunk_rewritten;

-- REPACK on an empty hypertable is a no-op
CREATE TABLE repack_empty(time timestamptz, v int);
SELECT create_hypertable('repack_empty', 'time');
REPACK repack_empty;
REPACK repack_empty USING INDEX repack_empty_time_idx;

-- REPACK must not run inside a transaction block
\set ON_ERROR_STOP 0
BEGIN;
REPACK repack_test;
ROLLBACK;
\set ON_ERROR_STOP 1

-- Only the owner may REPACK a hypertable
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
REPACK repack_test;
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Options we do not handle per-chunk yet are rejected with a clear error
\set ON_ERROR_STOP 0
REPACK (CONCURRENTLY) repack_test;
REPACK (ANALYZE) repack_test;
\set ON_ERROR_STOP 1

DROP TABLE repack_test;
DROP TABLE repack_files;
DROP TABLE repack_empty;
