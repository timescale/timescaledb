-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test single column fk constraint from plain table to hypertable during hypertable creation
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

CREATE TABLE event(time timestamptz references metrics(time), info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

\set ON_ERROR_STOP 0
-- should fail
DELETE FROM metrics WHERE time = '2020-01-01';
-- should fail
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

-- create new chunk and repeat the test
INSERT INTO metrics(time, device, value) VALUES ('2021-01-01', 'd1', 1.0);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2021-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2021-01-01', 'info2');

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

-- chunks referenced in fk constraints must not be dropped or truncated
\set ON_ERROR_STOP 0
TRUNCATE metrics;
TRUNCATE _timescaledb_internal._hyper_1_1_chunk;
TRUNCATE _timescaledb_internal._hyper_1_2_chunk;
DROP TABLE _timescaledb_internal._hyper_1_1_chunk;
DROP TABLE _timescaledb_internal._hyper_1_2_chunk;
SELECT drop_chunks('metrics', '1 month'::interval);
\set ON_ERROR_STOP 1

-- after removing constraint dropping should succeed
ALTER TABLE event DROP CONSTRAINT event_time_fkey;
SELECT drop_chunks('metrics', '1 month'::interval);

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable during hypertable creation with RESTRICT
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

CREATE TABLE event(time timestamptz references metrics(time) ON DELETE RESTRICT ON UPDATE RESTRICT, info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

\set ON_ERROR_STOP 0
-- should fail
DELETE FROM metrics WHERE time = '2020-01-01';
-- should fail
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

-- create new chunk and repeat the test
INSERT INTO metrics(time, device, value) VALUES ('2021-01-01', 'd1', 1.0);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2021-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2021-01-01', 'info2');

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable during hypertable creation with CASCADE
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

CREATE TABLE event(time timestamptz references metrics(time) ON DELETE CASCADE ON UPDATE CASCADE, info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

-- should cascade
BEGIN;
DELETE FROM metrics WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;
-- should cascade
BEGIN;
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable during hypertable creation with SET NULL
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

CREATE TABLE event(time timestamptz references metrics(time) ON DELETE SET NULL ON UPDATE SET NULL, info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

-- should cascade
BEGIN;
DELETE FROM metrics WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;
-- should cascade
BEGIN;
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable during hypertable creation with SET DEFAULT
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

CREATE TABLE event(time timestamptz default null references metrics(time) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT, info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

-- should cascade
BEGIN;
DELETE FROM metrics WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;
-- should cascade
BEGIN;
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable with constraint being added separately
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01', 'd1', 1.0);

CREATE TABLE event(time timestamptz, info text);

ALTER TABLE event ADD CONSTRAINT event_time_fkey FOREIGN KEY (time) REFERENCES metrics(time) ON DELETE RESTRICT;

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

-- should fail
\set ON_ERROR_STOP 0
DELETE FROM metrics WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test multi column fk constraint from plain table to hypertable
CREATE TABLE metrics(time timestamptz , device text, value float, primary key (time, device));
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01', 'd1', 1.0);

CREATE TABLE event(time timestamptz, device text, info text);

ALTER TABLE event ADD CONSTRAINT event_time_fkey FOREIGN KEY (time,device) REFERENCES metrics(time,device) ON DELETE RESTRICT;

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, device, info) VALUES ('2020-01-02', 'd1', 'info1');
INSERT INTO event(time, device, info) VALUES ('2020-01-01', 'd2', 'info2');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, device, info) VALUES ('2020-01-01', 'd1', 'info2');

-- should fail
\set ON_ERROR_STOP 0
DELETE FROM metrics WHERE time = '2020-01-01';
DELETE FROM metrics WHERE device = 'd1';
\set ON_ERROR_STOP 1

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to hypertable with constraint being added separately while data is present
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01', 'd1', 1.0);

CREATE TABLE event(time timestamptz, info text);
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');
INSERT INTO event(time, info) VALUES ('2020-02-01', 'info1');

-- should fail
\set ON_ERROR_STOP 0
ALTER TABLE event ADD CONSTRAINT event_time_fkey FOREIGN KEY (time) REFERENCES metrics(time) ON DELETE SET NULL;
\set ON_ERROR_STOP 1

INSERT INTO metrics(time, device, value) VALUES ('2020-02-01', 'd1', 1.0);
ALTER TABLE event ADD CONSTRAINT event_time_fkey FOREIGN KEY (time) REFERENCES metrics(time) ON DELETE CASCADE;

-- delete should cascade
DELETE FROM metrics WHERE time = '2020-01-01';
SELECT * FROM event;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from plain table to compressed hypertable
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 0:00:00', 'd1', 1.0);
INSERT INTO metrics(time, device, value) VALUES ('2020-01-01 1:00:00', 'd1', 1.0);

ALTER TABLE metrics SET(timescaledb.compress, timescaledb.compress_segmentby='device');

SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

CREATE TABLE event(time timestamptz references metrics(time) ON DELETE CASCADE ON UPDATE CASCADE, info text);

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO event(time, info) VALUES ('2020-01-02', 'info1');
\set ON_ERROR_STOP 1
-- should succeed
INSERT INTO event(time, info) VALUES ('2020-01-01', 'info2');

\set ON_ERROR_STOP 0
-- should fail
UPDATE event SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
\set ON_ERROR_STOP 1

-- should succeed
BEGIN;
UPDATE event SET time = '2020-01-01 01:00:00' WHERE time = '2020-01-01';
ROLLBACK;

-- should cascade
BEGIN;
DELETE FROM metrics WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;
-- should cascade
BEGIN;
UPDATE metrics SET time = '2020-01-01 00:30:00' WHERE time = '2020-01-01';
SELECT * FROM event ORDER BY event;
ROLLBACK;

SELECT conname, conrelid::regclass, confrelid::regclass, conparentid <> 0 AS parent FROM pg_constraint WHERE conrelid='event'::regclass ORDER BY oid;
SELECT tgfoid::regproc, tgparentid <> 0 AS parent, tgisinternal, tgconstrrelid::regclass FROM pg_trigger WHERE tgconstrrelid='event'::regclass ORDER BY oid;

DROP TABLE event;
DROP TABLE metrics;

-- test single column fk constraint from hypertable to hypertable
CREATE TABLE metrics(time timestamptz primary key, device text, value float);
SELECT table_name FROM create_hypertable('metrics', 'time');

CREATE TABLE event(time timestamptz, info text);
SELECT table_name FROM create_hypertable('event', 'time');

\set ON_ERROR_STOP 0
ALTER TABLE event ADD CONSTRAINT event_time_fkey FOREIGN KEY (time) REFERENCES metrics(time);
\set ON_ERROR_STOP 1

CREATE TABLE event2(time timestamptz REFERENCES metrics(time), info text);
\set ON_ERROR_STOP 0
SELECT table_name FROM create_hypertable('event2', 'time');
\set ON_ERROR_STOP 1

DROP TABLE event;
DROP TABLE event2;
DROP TABLE metrics;
