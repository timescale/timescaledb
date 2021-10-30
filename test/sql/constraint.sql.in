-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE hyper (
  time BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);


SELECT * FROM create_hypertable('hyper', 'time', chunk_time_interval => 10);

--check and not-null constraints are inherited through regular inheritance.

\set ON_ERROR_STOP 0
INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 9);

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, NULL, 11);

ALTER TABLE hyper ALTER COLUMN time DROP NOT NULL;

ALTER TABLE ONLY hyper ALTER COLUMN sensor_1 SET NOT NULL;
ALTER TABLE ONLY hyper ALTER COLUMN device_id DROP NOT NULL;

\set ON_ERROR_STOP 1

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

ALTER TABLE hyper ALTER COLUMN device_id DROP NOT NULL;

INSERT INTO hyper(time, device_id,sensor_1) VALUES
(1257987700000000000, NULL, 11);

--make sure validate works
\set ON_ERROR_STOP 0
ALTER TABLE hyper ADD CONSTRAINT bad_check_const CHECK (sensor_1 > 100);
\set ON_ERROR_STOP 1

ALTER TABLE hyper ADD CONSTRAINT bad_check_const CHECK (sensor_1 > 100) NOT VALID;

\set ON_ERROR_STOP 0
ALTER TABLE hyper VALIDATE CONSTRAINT bad_check_const;
\set ON_ERROR_STOP 1
----------------------- UNIQUE CONSTRAINTS ------------------

CREATE TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name (
  time BIGINT NOT NULL UNIQUE,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);


SELECT * FROM create_hypertable('hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name', 'time', chunk_time_interval => 10);

INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1) VALUES
(1257987800000000000, 'dev2', 11);

\set ON_ERROR_STOP 0
INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);
\set ON_ERROR_STOP 1

-- Show constraints on main tables
SELECT * FROM _timescaledb_catalog.chunk_constraint;
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM test.show_constraints('hyper');
SELECT * FROM test.show_constraints('hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name');
--should have unique constraint not just unique index
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name DROP CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key;
-- The constraint should have been removed from the chunk as well
SELECT * FROM _timescaledb_catalog.chunk_constraint;
-- The index should also have been removed
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');

--uniqueness not enforced
INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev3', 11);

--shouldn't be able to create constraint
\set ON_ERROR_STOP 0
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name ADD CONSTRAINT hyper_unique_time_key UNIQUE (time);
\set ON_ERROR_STOP 1

DELETE FROM hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name WHERE device_id = 'dev3';

-- Try multi-alter table statement with a constraint without a name
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
      ADD CHECK (time > 0),
      ADD UNIQUE (time) DEFERRABLE INITIALLY DEFERRED;

\set ON_ERROR_STOP 0
BEGIN;
--testing deferred checking. The following row has an error, which will not appear until the commit
INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev3', 11);
SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

SELECT * FROM _timescaledb_catalog.chunk_constraint;
SELECT * FROM test.show_constraints('hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');

ALTER TABLE  hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
DROP CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key,
DROP CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooo_time_check;

SELECT * FROM _timescaledb_catalog.chunk_constraint;
SELECT * FROM test.show_constraints('hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');

CREATE UNIQUE INDEX hyper_unique_with_looooooooooooooooooooooooooooooooo_time_idx
ON hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name (time);

\set ON_ERROR_STOP 0
-- Try adding constraint using existing index
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
ADD CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key UNIQUE
USING INDEX hyper_unique_with_looooooooooooooooooooooooooooooooo_time_idx;
\set ON_ERROR_STOP 1
DROP INDEX hyper_unique_with_looooooooooooooooooooooooooooooooo_time_idx;

--now can create
ALTER TABLE  hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
ADD CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key UNIQUE (time);
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');

--test adding constraint with same name to different table -- should fail
\set ON_ERROR_STOP 0
ALTER TABLE hyper
ADD CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key UNIQUE (time);
\set ON_ERROR_STOP 1

--uniquness violation fails
\set ON_ERROR_STOP 0
INSERT INTO hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name(time, device_id,sensor_1)
VALUES (1257987700000000000, 'dev2', 11);
\set ON_ERROR_STOP 1

--cannot create unique constraint on non-partition column
\set ON_ERROR_STOP 0
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
ADD CONSTRAINT hyper_unique_invalid UNIQUE (device_id);

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
ADD COLUMN new_device_id int UNIQUE;

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
DROP COLUMN device_id,
ADD COLUMN new_device_id int UNIQUE;
\set ON_ERROR_STOP 1

----------------------- RENAME CONSTRAINT  ------------------

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
RENAME CONSTRAINT hyper_unique_with_looooooooooooooooooooooooooooooooooo_time_key TO new_name;

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name *
RENAME CONSTRAINT new_name TO new_name2;

ALTER TABLE IF EXISTS hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
RENAME CONSTRAINT  hyper_unique_with_looooooooooooooooooooooooooooo_sensor_1_check TO check_2;


SELECT * FROM test.show_constraints('hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_2_4_chunk');
SELECT * FROM _timescaledb_catalog.chunk_constraint;

\set ON_ERROR_STOP 0
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
RENAME CONSTRAINT new_name TO new_name2;
ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
RENAME CONSTRAINT new_name2 TO check_2;
ALTER TABLE ONLY hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
RENAME CONSTRAINT new_name2 TO new_name;
ALTER TABLE _timescaledb_internal._hyper_2_4_chunk
RENAME CONSTRAINT "4_10_new_name2" TO new_name;
\set ON_ERROR_STOP 1

----------------------- PRIMARY KEY  ------------------

CREATE TABLE hyper_pk (
  time BIGINT NOT NULL PRIMARY KEY,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);


SELECT * FROM create_hypertable('hyper_pk', 'time', chunk_time_interval => 10);

INSERT INTO hyper_pk(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

\set ON_ERROR_STOP 0
INSERT INTO hyper_pk(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);
\set ON_ERROR_STOP 1

--should have unique constraint not just unique index
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_3_6_chunk');

ALTER TABLE hyper_pk DROP CONSTRAINT hyper_pk_pkey;
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_3_6_chunk');

--uniqueness not enforced
INSERT INTO hyper_pk(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev3', 11);


--shouldn't be able to create pk
\set ON_ERROR_STOP 0
ALTER TABLE hyper_pk ADD CONSTRAINT hyper_pk_pkey PRIMARY KEY (time);

ALTER TABLE hyper_unique_with_looooooooooooooooooooooooooooooooooooong_name
ADD COLUMN new_device_id int PRIMARY KEY;
\set ON_ERROR_STOP 1

DELETE FROM hyper_pk WHERE device_id = 'dev3';

--cannot create pk constraint on non-partition column
\set ON_ERROR_STOP 0
ALTER TABLE hyper_pk ADD CONSTRAINT hyper_pk_invalid PRIMARY KEY (device_id);
\set ON_ERROR_STOP 1

--now can create
ALTER TABLE hyper_pk ADD CONSTRAINT hyper_pk_pkey PRIMARY KEY (time) DEFERRABLE INITIALLY DEFERRED;
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_3_6_chunk');

--test adding constraint with same name to different table -- should fail
\set ON_ERROR_STOP 0
ALTER TABLE hyper ADD CONSTRAINT hyper_pk_pkey UNIQUE (time);
\set ON_ERROR_STOP 1

--uniquness violation fails
\set ON_ERROR_STOP 0
BEGIN;
  --error here deferred until commit
  INSERT INTO hyper_pk(time, device_id,sensor_1) VALUES
  (1257987700000000000, 'dev2', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1


----------------------- FOREIGN KEY  ------------------

CREATE TABLE devices(
    device_id TEXT NOT NULL,
    PRIMARY KEY (device_id)
);

CREATE TABLE hyper_fk (
  time BIGINT NOT NULL PRIMARY KEY,
  device_id TEXT NOT NULL REFERENCES devices(device_id),
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);


SELECT * FROM create_hypertable('hyper_fk', 'time', chunk_time_interval => 10);

--fail fk constraint
\set ON_ERROR_STOP 0
INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);
\set ON_ERROR_STOP 1

INSERT INTO devices VALUES ('dev2');

INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

--delete should fail
\set ON_ERROR_STOP 0
DELETE FROM devices;
\set ON_ERROR_STOP 1

ALTER TABLE hyper_fk DROP CONSTRAINT hyper_fk_device_id_fkey;

--should now be able to add non-fk rows
INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
(1257987700000000001, 'dev3', 11);

--can't add fk because of dev3 row
\set ON_ERROR_STOP 0
ALTER TABLE hyper_fk ADD CONSTRAINT hyper_fk_device_id_fkey
FOREIGN KEY (device_id) REFERENCES devices(device_id);
\set ON_ERROR_STOP 1

--but can add a NOT VALID one
ALTER TABLE hyper_fk ADD CONSTRAINT hyper_fk_device_id_fkey
FOREIGN KEY (device_id) REFERENCES devices(device_id) NOT VALID;

--which will fail when validated
\set ON_ERROR_STOP 0
ALTER TABLE hyper_fk VALIDATE CONSTRAINT hyper_fk_device_id_fkey;
\set ON_ERROR_STOP 1

ALTER TABLE hyper_fk DROP CONSTRAINT hyper_fk_device_id_fkey;

DELETE FROM hyper_fk WHERE device_id = 'dev3';

ALTER TABLE hyper_fk ADD CONSTRAINT hyper_fk_device_id_fkey
FOREIGN KEY (device_id) REFERENCES devices(device_id);

\set ON_ERROR_STOP 0
INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
(1257987700000000002, 'dev3', 11);
\set ON_ERROR_STOP 1

SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_4_8_chunk');
SELECT * FROM _timescaledb_catalog.chunk_constraint;

--test CASCADE drop behavior
DROP TABLE devices CASCADE;

SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_4_8_chunk');
SELECT * FROM _timescaledb_catalog.chunk_constraint;

--the fk went away.
INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
(1257987700000000002, 'dev3', 11);

CREATE TABLE devices(
    device_id TEXT NOT NULL,
    PRIMARY KEY (device_id)
);

INSERT INTO devices VALUES ('dev2'), ('dev3');

ALTER TABLE hyper_fk ADD CONSTRAINT hyper_fk_device_id_fkey
FOREIGN KEY (device_id) REFERENCES devices(device_id) DEFERRABLE INITIALLY DEFERRED;

\set ON_ERROR_STOP 0
BEGIN;
  --error deferred until commmit
  INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
  (1257987700000000003, 'dev4', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

ALTER TABLE hyper_fk ALTER CONSTRAINT hyper_fk_device_id_fkey NOT DEFERRABLE;

\set ON_ERROR_STOP 0
BEGIN;
  --error detected right away
  INSERT INTO hyper_fk(time, device_id,sensor_1) VALUES
  (1257987700000000003, 'dev4', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

--this tests that there are no extra chunk_constraints left on hyper_fk
TRUNCATE hyper_fk;

----------------------- FOREIGN KEY INTO A HYPERTABLE  ------------------


--FOREIGN KEY references into a hypertable are currently broken.
--The referencing table will never find the corresponding row in the hypertable
--since it will only search the parent. Thus any insert will result in an ERROR
--Block such foreign keys or fix. (Hard to block on create table so punting for now)
CREATE TABLE hyper_for_ref (
  time BIGINT NOT NULL PRIMARY KEY,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);

SELECT * FROM create_hypertable('hyper_for_ref', 'time', chunk_time_interval => 10);

\set ON_ERROR_STOP 0
CREATE TABLE referrer (
    time BIGINT NOT NULL REFERENCES hyper_for_ref(time)
);
\set ON_ERROR_STOP 1

CREATE TABLE referrer2 (
   time BIGINT NOT NULL
);

\set ON_ERROR_STOP 0
ALTER TABLE referrer2 ADD CONSTRAINT hyper_fk_device_id_fkey
FOREIGN KEY (time) REFERENCES  hyper_for_ref(time);
\set ON_ERROR_STOP 1

----------------------- EXCLUSION CONSTRAINT  ------------------

CREATE TABLE hyper_ex (
    time BIGINT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10),
    canceled boolean DEFAULT false,
    EXCLUDE USING btree (
        time WITH =, device_id WITH =
    ) WHERE (not canceled)
);

SELECT * FROM create_hypertable('hyper_ex', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

INSERT INTO hyper_ex(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 11);

\set ON_ERROR_STOP 0
INSERT INTO hyper_ex(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 12);
\set ON_ERROR_STOP 1

ALTER TABLE hyper_ex DROP CONSTRAINT hyper_ex_time_device_id_excl;

--can now add
INSERT INTO hyper_ex(time, device_id,sensor_1) VALUES
(1257987700000000000, 'dev2', 12);

--cannot add because of conflicts
\set ON_ERROR_STOP 0
ALTER TABLE hyper_ex ADD CONSTRAINT hyper_ex_time_device_id_excl
    EXCLUDE USING btree (
        time WITH =, device_id WITH =
    ) WHERE (not canceled)
;
\set ON_ERROR_STOP 1

DELETE FROM hyper_ex WHERE sensor_1 = 12;

ALTER TABLE hyper_ex ADD CONSTRAINT hyper_ex_time_device_id_excl
    EXCLUDE USING btree (
        time WITH =, device_id WITH =
    ) WHERE (not canceled) DEFERRABLE INITIALLY DEFERRED
;

\set ON_ERROR_STOP 0
BEGIN;
  --error deferred til commit
  INSERT INTO hyper_ex(time, device_id,sensor_1) VALUES
  (1257987700000000000, 'dev2', 12);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1


--cannot add exclusion constraint without partition key.
CREATE TABLE hyper_ex_invalid (
    time BIGINT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10),
    canceled boolean DEFAULT false,
    EXCLUDE USING btree (
        device_id WITH =
    ) WHERE (not canceled)
);

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('hyper_ex_invalid', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1


--- NO INHERIT constraints (not allowed) ----
CREATE TABLE hyper_noinherit (
    time BIGINT,
    sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 0) NO INHERIT
);

SELECT * FROM test.show_constraints('hyper_noinherit');

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('hyper_noinherit', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

CREATE TABLE hyper_noinherit_alter (
    time BIGINT,
    sensor_1 NUMERIC NULL DEFAULT 1
);

SELECT * FROM create_hypertable('hyper_noinherit_alter', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
ALTER TABLE hyper_noinherit_alter ADD CONSTRAINT check_noinherit CHECK (sensor_1 > 0) NO INHERIT;
--  CREATE TABLE WITH DEFERRED CONSTRAINTS --

CREATE TABLE hyper_unique_deferred (
  time BIGINT UNIQUE DEFERRABLE INITIALLY DEFERRED,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);

SELECT * FROM create_hypertable('hyper_unique_deferred', 'time', chunk_time_interval => 10);

INSERT INTO hyper_unique_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 11);

\set ON_ERROR_STOP 0
BEGIN;
  --error here deferred until commit
  INSERT INTO hyper_unique_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

--test deferred on create table
CREATE TABLE hyper_pk_deferred (
  time BIGINT NOT NULL PRIMARY KEY DEFERRABLE INITIALLY DEFERRED,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);

SELECT * FROM create_hypertable('hyper_pk_deferred', 'time', chunk_time_interval => 10);

INSERT INTO hyper_pk_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 11);

\set ON_ERROR_STOP 0
BEGIN;
  --error here deferred until commit
  INSERT INTO hyper_pk_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

--test that deferred works on create table too
CREATE TABLE hyper_fk_deferred (
  time BIGINT NOT NULL PRIMARY KEY,
  device_id TEXT NOT NULL REFERENCES devices(device_id) DEFERRABLE INITIALLY DEFERRED,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);

SELECT * FROM create_hypertable('hyper_fk_deferred', 'time', chunk_time_interval => 10);

\set ON_ERROR_STOP 0
BEGIN;
  --error deferred until commmit
  INSERT INTO hyper_fk_deferred(time, device_id,sensor_1) VALUES (1257987700000000003, 'dev4', 11);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

CREATE TABLE hyper_ex_deferred (
    time BIGINT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10),
    canceled boolean DEFAULT false,
    EXCLUDE USING btree (
        time WITH =, device_id WITH =
    ) WHERE (not canceled) DEFERRABLE INITIALLY DEFERRED
);

SELECT * FROM create_hypertable('hyper_ex_deferred', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

INSERT INTO hyper_ex_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 12);

\set ON_ERROR_STOP 0
BEGIN;
  --error deferred til commit
  INSERT INTO hyper_ex_deferred(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 12);
  SELECT 1;
COMMIT;
\set ON_ERROR_STOP 1

-- Make sure renaming schemas won't break dropping constraints
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLE hyper_unique (
  time BIGINT NOT NULL UNIQUE,
  device_id TEXT NOT NULL,
  sensor_1 NUMERIC NULL DEFAULT 1 CHECK (sensor_1 > 10)
);

SELECT * FROM create_hypertable('hyper_unique', 'time', chunk_time_interval => 10, associated_schema_name => 'my_associated_schema');

INSERT INTO hyper_unique(time, device_id,sensor_1) VALUES (1257987700000000000, 'dev2', 11);

ALTER SCHEMA my_associated_schema RENAME TO new_associated_schema;

ALTER TABLE hyper_unique DROP CONSTRAINT hyper_unique_time_key;

-- test for constraint validation crash, see #1183
CREATE TABLE test_validate(time timestamp NOT NULL, a TEXT, b TEXT);
SELECT * FROM create_hypertable('test_validate', 'time');
INSERT INTO test_validate values(now(), 'a', 'b');

ALTER TABLE test_validate
ADD COLUMN c TEXT,
ADD CONSTRAINT c_not_null CHECK (c IS NOT NULL) NOT VALID;

UPDATE test_validate SET c = '';

ALTER TABLE test_validate
VALIDATE CONSTRAINT c_not_null;

DROP TABLE test_validate;

-- test for hypertables constraints both using index tablespaces and not See #2604
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
SET client_min_messages = NOTICE;
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;

CREATE TABLE fk_tbl (
id int,
CONSTRAINT pkfk PRIMARY KEY (id) USING INDEX TABLESPACE tablespace1);

CREATE TABLE tbl (
fk_id int,
id int,
time timestamp,
CONSTRAINT pk PRIMARY KEY (time, id) USING INDEX TABLESPACE tablespace1);

SELECT create_hypertable('tbl', 'time');

ALTER TABLE tbl
ADD CONSTRAINT fk_con
FOREIGN KEY (fk_id) REFERENCES fk_tbl(id)
ON UPDATE SET NULL
ON DELETE SET NULL;

INSERT INTO fk_tbl VALUES(1);

INSERT INTO tbl VALUES (
1, 1, now()
);

DROP TABLE tbl;
DROP TABLE fk_tbl;

DROP TABLESPACE IF EXISTS tablespace1;
