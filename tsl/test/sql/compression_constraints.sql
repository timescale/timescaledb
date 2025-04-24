-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics(time timestamptz not null, device text, value float);
SELECT create_hypertable('metrics','time');

ALTER TABLE metrics SET (tsdb.compress, tsdb.compress_segmentby='device', tsdb.compress_orderby='time');

-- test all variations without data
BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time);
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time, device);
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time);
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time, device);
ROLLBACK;

BEGIN;
CREATE UNIQUE INDEX metrics_pk ON metrics(time);
ROLLBACK;

BEGIN;
CREATE UNIQUE INDEX metrics_pk ON metrics(time,device);
ROLLBACK;

-- test all variations with data but no conflict
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

\set ON_ERROR_STOP 0

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time, device);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time, device);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

BEGIN;
CREATE UNIQUE INDEX metrics_pk ON metrics(time);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

BEGIN;
CREATE UNIQUE INDEX metrics_pk ON metrics(time,device);
INSERT INTO metrics SELECT '2025-01-02', 'd1', 0.1;
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;
ROLLBACK;

\set ON_ERROR_STOP 0

-- test with conflict across uncompressed/compressed
INSERT INTO metrics SELECT '2025-01-01', 'd1', 0.1;

\set ON_ERROR_STOP 0

ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time, device);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time, device);
CREATE UNIQUE INDEX metrics_pk ON metrics(time);
CREATE UNIQUE INDEX metrics_pk ON metrics(time,device);

\set ON_ERROR_STOP 1

-- test with conflict in compressed
SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk PRIMARY KEY(time, device);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time);
ALTER TABLE metrics ADD CONSTRAINT metrics_pk UNIQUE(time, device);
CREATE UNIQUE INDEX metrics_pk ON metrics(time);
CREATE UNIQUE INDEX metrics_pk ON metrics(time,device);
\set ON_ERROR_STOP 1

DROP TABLE metrics;

-- test NULL behaviour
CREATE TABLE dist_null(time timestamptz not null, device text, value float);
SELECT table_name FROM create_hypertable('dist_null','time',chunk_time_interval:='1 year'::interval);

ALTER TABLE dist_null SET (tsdb.compress, tsdb.compress_segmentby='device', tsdb.compress_orderby='time');

-- test behaviour without compression
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
DROP INDEX dist_null_idx;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test behaviour with conflict in compressed
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
DROP INDEX dist_null_idx;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test behaviour with conflict between compressed/uncompressed
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
DROP INDEX dist_null_idx;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples (full compressed)
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples (full compressed)
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples (partially compressed)
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test NULLs present but conflict in non-NULL tuples (partially compressed)
BEGIN;
INSERT INTO dist_null SELECT '2025-01-01', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', NULL, 0.1;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
SELECT count(compress_chunk(ch)) FROM show_chunks('dist_null') ch;
INSERT INTO dist_null SELECT '2025-01-02', 'd', 0.1;
\set ON_ERROR_STOP 0
CREATE UNIQUE INDEX dist_null_idx ON dist_null(time,device) NULLS NOT DISTINCT;
\set ON_ERROR_STOP 1
ROLLBACK;

-- test foreign keys
-- hypertable referencing plain table
CREATE TABLE metrics(time timestamptz NOT NULL, device text, value float, PRIMARY KEY(device,time));
SELECT create_hypertable('metrics','time');
INSERT INTO metrics SELECT '2025-01-01','d1',0.1;
INSERT INTO metrics SELECT '2025-01-01','d2',0.1;

CREATE TABLE device(device TEXT PRIMARY KEY);
INSERT INTO device SELECT 'd1';

CREATE TABLE events(time timestamptz, device text, data text);
INSERT INTO events SELECT '2025-02-01', 'd1', '';

\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD CONSTRAINT device_fk FOREIGN KEY(device) REFERENCES device(device);
ALTER TABLE events ADD CONSTRAINT events_fk FOREIGN KEY(device,time) REFERENCES metrics(device,time);
\set ON_ERROR_STOP 1

BEGIN;
INSERT INTO device SELECT 'd2';
ALTER TABLE metrics ADD CONSTRAINT device_fk FOREIGN KEY(device) REFERENCES device(device);
INSERT INTO metrics SELECT '2025-03-01','d2',0.3;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-03-01','d3',0.3;
\set ON_ERROR_STOP 1
ROLLBACK;

BEGIN;
INSERT INTO metrics SELECT '2025-02-01','d1',0.1;
ALTER TABLE events ADD CONSTRAINT events_fk FOREIGN KEY(device,time) REFERENCES metrics(device,time);
INSERT INTO events SELECT '2025-01-01','d1','';
\set ON_ERROR_STOP 0
INSERT INTO events SELECT '2025-03-01','d1','';
\set ON_ERROR_STOP 1
ROLLBACK;

ALTER TABLE metrics SET (tsdb.compress, tsdb.compress_orderby='device,time DESC', tsdb.compress_segmentby='');
SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD CONSTRAINT device_fk FOREIGN KEY(device) REFERENCES device(device);
ALTER TABLE events ADD CONSTRAINT events_fk FOREIGN KEY(device,time) REFERENCES metrics(device,time);
\set ON_ERROR_STOP 1

BEGIN;
INSERT INTO device SELECT 'd2';
ALTER TABLE metrics ADD CONSTRAINT device_fk FOREIGN KEY(device) REFERENCES device(device);
INSERT INTO metrics SELECT '2025-03-01','d2',0.3;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-03-01','d3',0.3;
\set ON_ERROR_STOP 1
ROLLBACK;

BEGIN;
INSERT INTO metrics SELECT '2025-02-01','d1',0.1;
ALTER TABLE events ADD CONSTRAINT events_fk FOREIGN KEY(device,time) REFERENCES metrics(device,time);
INSERT INTO events SELECT '2025-01-01','d1','';
\set ON_ERROR_STOP 0
INSERT INTO events SELECT '2025-03-01','d1','';
\set ON_ERROR_STOP 1
ROLLBACK;

DROP TABLE metrics CASCADE;
DROP TABLE device CASCADE;
DROP TABLE events CASCADE;

-- test CHECK constraints

CREATE TABLE metrics(time timestamptz not null, device text, value float);
SELECT create_hypertable('metrics','time');

ALTER TABLE metrics SET (tsdb.compress, tsdb.compress_segmentby='device', tsdb.compress_orderby='time');

INSERT INTO metrics SELECT '2025-01-01','d1',1;
INSERT INTO metrics SELECT '2025-01-01','d1',NULL;

\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (value > 9000);
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (time > '2026-01-01' AND value > 0);
\set ON_ERROR_STOP 1
BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (value > 0.9);
ALTER TABLE metrics ADD CONSTRAINT metrics_check2 CHECK (time >= '2025-01-01' AND value > 0);
INSERT INTO metrics SELECT '2025-01-01','d1',2;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-01-01','d1',0;
\set ON_ERROR_STOP 1
ROLLBACK;

SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (value > 9000);
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (time > '2026-01-01' AND value > 0);
\set ON_ERROR_STOP 1
BEGIN;
ALTER TABLE metrics ADD CONSTRAINT metrics_check CHECK (value > 0.9);
ALTER TABLE metrics ADD CONSTRAINT metrics_check2 CHECK (time >= '2025-01-01' AND value > 0);
INSERT INTO metrics SELECT '2025-01-01','d1',2;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-01-01','d1',0;
\set ON_ERROR_STOP 1
ROLLBACK;

DROP TABLE metrics CASCADE;

-- test adding columns and constraints together
CREATE TABLE metrics(time timestamptz not null, device text, value float);
SELECT create_hypertable('metrics','time');

ALTER TABLE metrics SET (tsdb.compress, tsdb.compress_segmentby='device', tsdb.compress_orderby='time');

INSERT INTO metrics SELECT '2025-01-01','d1',1;
-- should fail in validation since the new column will be NULL for existing tuples
\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD COLUMN c1 int CHECK ( c1 IS NOT NULL );
\set ON_ERROR_STOP 1

BEGIN;
-- should succeed since the CHECK constraint will succeed for NULL values
ALTER TABLE metrics ADD COLUMN c1 int CHECK (c1 <> 42);
INSERT INTO metrics SELECT '2025-01-01','d1',1,23;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-01-01','d1',1,42;
\set ON_ERROR_STOP 1
ROLLBACK;

SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

-- should fail in validation since the new column will be NULL for existing tuples
\set ON_ERROR_STOP 0
ALTER TABLE metrics ADD COLUMN c1 int CHECK ( c1 IS NOT NULL );
\set ON_ERROR_STOP 1

BEGIN;
-- should succeed since the CHECK constraint will succeed for NULL values
ALTER TABLE metrics ADD COLUMN c1 int CHECK (c1 <> 42);
INSERT INTO metrics SELECT '2025-01-01','d1',1,23;
\set ON_ERROR_STOP 0
INSERT INTO metrics SELECT '2025-01-01','d1',1,42;
\set ON_ERROR_STOP 1
ROLLBACK;


