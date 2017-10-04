\set ON_ERROR_STOP 0

\c single :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
SET client_min_messages = NOTICE;

--test hypertable with tables space
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
\c single :ROLE_DEFAULT_PERM_USER

--assigning a tablespace via the main table should work
CREATE TABLE tspace_2dim(time timestamp, temp float, device text) TABLESPACE tablespace1;
SELECT create_hypertable('tspace_2dim', 'time', 'device', 2);

INSERT INTO tspace_2dim VALUES ('2017-01-20T09:00:01', 24.3, 'blue');

--verify that the table chunk has the correct tablespace
SELECT relname, spcname FROM pg_class c
INNER JOIN pg_tablespace t ON (c.reltablespace = t.oid)
INNER JOIN _timescaledb_catalog.chunk ch ON (ch.table_name = c.relname);

--attach another tablespace without first creating it --> should generate error
SELECT attach_tablespace('tspace_2dim', 'tablespace2');
--attach the same tablespace twice to same table should also generate error
SELECT attach_tablespace('tspace_2dim', 'tablespace1');

\c single :ROLE_SUPERUSER
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;
\c single :ROLE_DEFAULT_PERM_USER

--attach after creating --> should work
SELECT attach_tablespace('tspace_2dim', 'tablespace2');

SELECT * FROM _timescaledb_catalog.tablespace;

--insert into another chunk
INSERT INTO tspace_2dim VALUES ('2017-01-20T09:00:01', 24.3, 'brown');

SELECT relname, spcname FROM pg_class c
INNER JOIN pg_tablespace t ON (c.reltablespace = t.oid)
INNER JOIN _timescaledb_catalog.chunk ch ON (ch.table_name = c.relname);

--
CREATE TABLE tspace_1dim(time timestamp, temp float, device text) TABLESPACE tablespace1;
SELECT create_hypertable('tspace_1dim', 'time');
SELECT attach_tablespace('tspace_1dim', 'tablespace2');

INSERT INTO tspace_1dim VALUES ('2017-01-20T09:00:01', 24.3, 'blue');
INSERT INTO tspace_1dim VALUES ('2017-03-20T09:00:01', 24.3, 'brown');

SELECT relname, spcname FROM pg_class c
INNER JOIN pg_tablespace t ON (c.reltablespace = t.oid)
INNER JOIN _timescaledb_catalog.chunk ch ON (ch.table_name = c.relname);

--cleanup
DROP TABLE tspace_1dim CASCADE;
DROP TABLE tspace_2dim CASCADE;

DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;

