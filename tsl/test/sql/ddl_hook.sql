-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_test_ddl_command_hook_reg() RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_ddl_command_hook_reg'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION ts_test_ddl_command_hook_unreg() RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_ddl_command_hook_unreg'
LANGUAGE C VOLATILE STRICT;

SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS htable_schema CASCADE;
DROP TABLE IF EXISTS htable;
DROP TABLE IF EXISTS non_htable;
SET client_min_messages TO NOTICE;

CREATE SCHEMA htable_schema;

-- Install test hooks
SELECT ts_test_ddl_command_hook_reg();

CREATE TABLE htable(time timestamptz, device int, color int CONSTRAINT color_check CHECK (color > 0), temp float);
CREATE UNIQUE INDEX htable_pk ON htable(time);

-- CREATE TABLE
SELECT * FROM create_hypertable('htable', 'time');
SELECT * FROM test.show_columns('htable');
SELECT * FROM test.show_constraints('htable');
SELECT * FROM test.show_indexes('htable');

-- ADD CONSTRAINT
ALTER TABLE htable ADD CONSTRAINT device_check CHECK (device > 0);
SELECT * FROM test.show_constraints('htable');

-- DROP CONSTRAINT
ALTER TABLE htable DROP CONSTRAINT device_check;
SELECT * FROM test.show_constraints('htable');

-- DROP COLUMN
ALTER TABLE htable DROP COLUMN color;
SELECT * FROM test.show_columns('htable');

-- ADD COLUMN
ALTER TABLE htable ADD COLUMN description text;
SELECT * FROM test.show_columns('htable');

-- CREATE INDEX
CREATE INDEX htable_description_idx ON htable (description);
SELECT * FROM test.show_indexes('htable');

-- CREATE/DROP TRIGGER
CREATE OR REPLACE FUNCTION test_trigger()
RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
RETURN OLD;
END
$BODY$;

CREATE TRIGGER htable_trigger_test
BEFORE INSERT ON htable
FOR EACH ROW EXECUTE FUNCTION test_trigger();

DROP TRIGGER htable_trigger_test on htable;
DROP FUNCTION test_trigger();

-- CLUSTER, TRUNCATE, REINDEX, VACUUM (should not call event hooks)
CREATE TABLE non_htable (id int);

CLUSTER htable USING htable_description_idx;
TRUNCATE non_htable, htable;
REINDEX TABLE htable;
VACUUM htable;

-- ALTER TABLE
ALTER TABLE htable ADD CONSTRAINT temp_check CHECK (temp > 0.0);
SELECT * FROM test.show_constraints('htable');

ALTER TABLE htable RENAME CONSTRAINT temp_check TO temp_chk;
ALTER TABLE htable RENAME COLUMN description TO descr;
ALTER INDEX htable_description_idx RENAME to htable_descr_idx;
ALTER TABLE htable SET SCHEMA htable_schema;
ALTER TABLE htable_schema.htable SET SCHEMA public;
ALTER TABLE public.htable RENAME TO htable2;
ALTER TABLE htable2 RENAME TO htable;

-- DROP INDEX, TABLE
\set ON_ERROR_STOP 0
DROP INDEX htable_description_idx, htable_pk;
DROP TABLE htable, non_htable;
\set ON_ERROR_STOP 1
DROP INDEX htable_descr_idx;
DROP TABLE htable;
DROP TABLE non_htable;

-- DROP TABLE within procedure
CREATE TABLE test (time timestamp, v int);
SELECT create_hypertable('test','time');
CREATE PROCEDURE test_drop() LANGUAGE PLPGSQL AS $$
BEGIN
    DROP TABLE test;
END
$$;
CALL test_drop();

-- DROP CASCADE cases

-- DROP schema
CREATE TABLE htable_schema.non_htable (id int);
CREATE TABLE htable_schema.htable(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('htable_schema.htable', 'time');
DROP SCHEMA htable_schema CASCADE;

-- DROP column cascades to index drop
CREATE TABLE htable(time timestamptz, device int, color int, temp float);
SELECT * FROM create_hypertable('htable', 'time');
CREATE INDEX htable_device_idx ON htable (device);
ALTER TABLE htable DROP COLUMN device;
DROP TABLE htable;

-- DROP foreign key
CREATE TABLE non_htable (id int PRIMARY KEY);
CREATE TABLE htable(time timestamptz, device int REFERENCES non_htable(id));
SELECT * FROM create_hypertable('htable', 'time');
DROP TABLE non_htable CASCADE;

-- cleanup
SELECT ts_test_ddl_command_hook_unreg();
