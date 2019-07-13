-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE cursor_test(time timestamptz, device_id int, temp float);
SELECT create_hypertable('cursor_test','time');

INSERT INTO cursor_test SELECT '2000-01-01',1,0.5;
INSERT INTO cursor_test SELECT '2001-01-01',1,0.5;
INSERT INTO cursor_test SELECT '2002-01-01',1,0.5;

\set ON_ERROR_STOP 0
BEGIN;
DECLARE c1 SCROLL CURSOR FOR SELECT * FROM cursor_test;
FETCH NEXT FROM c1;
-- this will produce an error because PostgreSQL checks
-- for the existence of a scan node with the relation id for every relation
-- used in the update plan in the plan of the cursor.
--
-- constraint exclusion will make this easier to hit
-- because constraint exclusion only works for SELECT
-- and UPDATE statements will still have parent tables in
-- the generated plans
UPDATE cursor_test SET temp = 0.7 WHERE CURRENT OF c1;
COMMIT;

-- test cursor with no chunks left after runtime exclusion
BEGIN;
DECLARE c1 SCROLL CURSOR FOR SELECT * FROM cursor_test WHERE time > now();
UPDATE cursor_test SET temp = 0.7 WHERE CURRENT OF c1;
COMMIT;

-- test cursor with no chunks left after planning exclusion
BEGIN;
DECLARE c1 SCROLL CURSOR FOR SELECT * FROM cursor_test WHERE time > '2010-01-01';
UPDATE cursor_test SET temp = 0.7 WHERE CURRENT OF c1;
COMMIT;
\set ON_ERROR_STOP 1

SET timescaledb.enable_constraint_exclusion TO off;
BEGIN;
DECLARE c1 SCROLL CURSOR FOR SELECT * FROM cursor_test;
FETCH NEXT FROM c1;
UPDATE cursor_test SET temp = 0.7 WHERE CURRENT OF c1;
COMMIT;

