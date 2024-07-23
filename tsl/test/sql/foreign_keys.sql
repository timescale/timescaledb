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

-- test FK behavior with different cascading configurations
CREATE TABLE fk_no_action(fk_no_action text primary key);
CREATE TABLE fk_restrict(fk_restrict text primary key);
CREATE TABLE fk_cascade(fk_cascade text primary key);
CREATE TABLE fk_set_null(fk_set_null text primary key);
CREATE TABLE fk_set_default(fk_set_default text primary key);

CREATE TABLE ht(
  time timestamptz not null,
  fk_no_action text references fk_no_action(fk_no_action) ON DELETE NO ACTION ON UPDATE NO ACTION,
  fk_restrict text references fk_restrict(fk_restrict) ON DELETE RESTRICT ON UPDATE RESTRICT,
  fk_cascade text references fk_cascade(fk_cascade) ON DELETE CASCADE ON UPDATE CASCADE,
  fk_set_null text references fk_set_null(fk_set_null) ON DELETE SET NULL ON UPDATE SET NULL,
  fk_set_default text default 'default' references fk_set_default(fk_set_default) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT
);

SELECT table_name FROM create_hypertable('ht', 'time');

ALTER TABLE ht SET(timescaledb.compress, timescaledb.compress_segmentby='fk_no_action,fk_restrict,fk_cascade,fk_set_null,fk_set_default');

\set ON_ERROR_STOP 0

INSERT INTO fk_set_default(fk_set_default) VALUES ('default');
INSERT INTO ht(time) VALUES ('2020-01-01');

-- NO ACTION
-- should fail with foreign key violation
INSERT INTO ht(time, fk_no_action) VALUES ('2020-01-01', 'fk_no_action');

-- ON UPDATE NO ACTION
BEGIN;
INSERT INTO fk_no_action(fk_no_action) VALUES ('fk_no_action');
INSERT INTO ht(time, fk_no_action) VALUES ('2020-01-01', 'fk_no_action');
-- should error
UPDATE fk_no_action SET fk_no_action = 'fk_no_action_updated';
ROLLBACK;

-- ON UPDATE NO ACTION with compression
BEGIN;
INSERT INTO fk_no_action(fk_no_action) VALUES ('fk_no_action');
INSERT INTO ht(time, fk_no_action) VALUES ('2020-01-01', 'fk_no_action');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should error
UPDATE fk_no_action SET fk_no_action = 'fk_no_action_updated';
ROLLBACK;

-- ON DELETE NO ACTION
BEGIN;
INSERT INTO fk_no_action(fk_no_action) VALUES ('fk_no_action');
INSERT INTO ht(time, fk_no_action) VALUES ('2020-01-01', 'fk_no_action');
-- should error
DELETE FROM fk_no_action;
ROLLBACK;

-- ON DELETE NO ACTION with compression
BEGIN;
INSERT INTO fk_no_action(fk_no_action) VALUES ('fk_no_action');
INSERT INTO ht(time, fk_no_action) VALUES ('2020-01-01', 'fk_no_action');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should error
DELETE FROM fk_no_action;
ROLLBACK;

-- RESTRICT
-- should fail with foreign key violation
INSERT INTO ht(time, fk_restrict) VALUES ('2020-01-01', 'fk_restrict');

-- ON UPDATE RESTRICT
BEGIN;
INSERT INTO fk_restrict(fk_restrict) VALUES ('fk_restrict');
INSERT INTO ht(time, fk_restrict) VALUES ('2020-01-01', 'fk_restrict');
-- should error
UPDATE fk_restrict SET fk_restrict = 'fk_restrict_updated';
ROLLBACK;

-- ON UPDATE RESTRICT with compression
BEGIN;
INSERT INTO fk_restrict(fk_restrict) VALUES ('fk_restrict');
INSERT INTO ht(time, fk_restrict) VALUES ('2020-01-01', 'fk_restrict');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should error
UPDATE fk_restrict SET fk_restrict = 'fk_restrict_updated';
ROLLBACK;

-- ON DELETE RESTRICT
BEGIN;
INSERT INTO fk_restrict(fk_restrict) VALUES ('fk_restrict');
INSERT INTO ht(time, fk_restrict) VALUES ('2020-01-01', 'fk_restrict');
-- should error
DELETE FROM fk_restrict;
ROLLBACK;

-- ON DELETE RESTRICT with compression
BEGIN;
INSERT INTO fk_restrict(fk_restrict) VALUES ('fk_restrict');
INSERT INTO ht(time, fk_restrict) VALUES ('2020-01-01', 'fk_restrict');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should error
DELETE FROM fk_restrict;
ROLLBACK;

-- CASCADE

-- should fail with foreign key violation
INSERT INTO ht(time, fk_cascade) VALUES ('2020-01-01', 'fk_cascade');

-- ON UPDATE CASCADE
BEGIN;
INSERT INTO fk_cascade(fk_cascade) VALUES ('fk_cascade');
INSERT INTO ht(time, fk_cascade) VALUES ('2020-01-01', 'fk_cascade');
-- should cascade
UPDATE fk_cascade SET fk_cascade = 'fk_cascade_updated';
SELECT * FROM ht;
ROLLBACK;

-- ON UPDATE CASCADE with compression
BEGIN;
INSERT INTO fk_cascade(fk_cascade) VALUES ('fk_cascade');
INSERT INTO ht(time, fk_cascade) VALUES ('2020-01-01', 'fk_cascade');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should cascade
UPDATE fk_cascade SET fk_cascade = 'fk_cascade_updated';
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

-- ON DELETE CASCADE
BEGIN;
INSERT INTO fk_cascade(fk_cascade) VALUES ('fk_cascade');
INSERT INTO ht(time, fk_cascade) VALUES ('2020-01-01', 'fk_cascade');
-- should cascade
DELETE FROM fk_cascade;
SELECT * FROM ht;
ROLLBACK;

-- ON DELETE CASCADE with compression
BEGIN;
INSERT INTO fk_cascade(fk_cascade) VALUES ('fk_cascade');
INSERT INTO ht(time, fk_cascade) VALUES ('2020-01-01', 'fk_cascade');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should cascade
DELETE FROM fk_cascade;
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

-- SET NULL
-- should fail with foreign key violation
INSERT INTO ht(time, fk_set_null) VALUES ('2020-01-01', 'fk_set_null');

-- ON UPDATE SET NULL
BEGIN;
INSERT INTO fk_set_null(fk_set_null) VALUES ('fk_set_null');
INSERT INTO ht(time, fk_set_null) VALUES ('2020-01-01', 'fk_set_null');
-- should set column to null
UPDATE fk_set_null SET fk_set_null = 'fk_set_null_updated';
SELECT * FROM ht;
ROLLBACK;

-- ON UPDATE SET NULL with compression
BEGIN;
INSERT INTO fk_set_null(fk_set_null) VALUES ('fk_set_null');
INSERT INTO ht(time, fk_set_null) VALUES ('2020-01-01', 'fk_set_null');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should set column to null
UPDATE fk_set_null SET fk_set_null = 'fk_set_null_updated';
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

-- ON DELETE SET NULL
BEGIN;
INSERT INTO fk_set_null(fk_set_null) VALUES ('fk_set_null');
INSERT INTO ht(time, fk_set_null) VALUES ('2020-01-01', 'fk_set_null');
-- should set column to null
DELETE FROM fk_set_null;
SELECT * FROM ht;
ROLLBACK;

-- ON DELETE SET NULL with compression
BEGIN;
INSERT INTO fk_set_null(fk_set_null) VALUES ('fk_set_null');
INSERT INTO ht(time, fk_set_null) VALUES ('2020-01-01', 'fk_set_null');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
-- should set column to null
DELETE FROM fk_set_null;
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

-- SET DEFAULT
-- should fail with foreign key violation
INSERT INTO ht(time, fk_set_default) VALUES ('2020-01-01', 'fk_set_default');

-- ON UPDATE SET DEFAULT
BEGIN;
INSERT INTO fk_set_default(fk_set_default) VALUES ('fk_set_default');
INSERT INTO ht(time, fk_set_default) VALUES ('2020-01-01', 'fk_set_default');
SELECT * FROM ht;
UPDATE fk_set_default SET fk_set_default = 'fk_set_default_updated' WHERE fk_set_default = 'fk_set_default';
SELECT * FROM ht;
ROLLBACK;

-- ON UPDATE SET DEFAULT with compression
BEGIN;
INSERT INTO fk_set_default(fk_set_default) VALUES ('fk_set_default');
INSERT INTO ht(time, fk_set_default) VALUES ('2020-01-01', 'fk_set_default');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
SELECT * FROM ht;
UPDATE fk_set_default SET fk_set_default = 'fk_set_default_updated' WHERE fk_set_default = 'fk_set_default';
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

-- ON DELETE SET DEFAULT
BEGIN;
INSERT INTO fk_set_default(fk_set_default) VALUES ('fk_set_default');
INSERT INTO ht(time, fk_set_default) VALUES ('2020-01-01', 'fk_set_default');
SELECT * FROM ht;
DELETE FROM fk_set_default WHERE fk_set_default = 'fk_set_default';
ROLLBACK;

-- ON DELETE SET DEFAULT with compression
BEGIN;
INSERT INTO fk_set_default(fk_set_default) VALUES ('fk_set_default');
INSERT INTO ht(time, fk_set_default) VALUES ('2020-01-01', 'fk_set_default');
SELECT count(compress_chunk(ch)) FROM show_chunks('ht') ch;
SELECT * FROM ht;
DELETE FROM fk_set_default WHERE fk_set_default = 'fk_set_default';
SELECT * FROM ht;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT * FROM ht;
ROLLBACK;

