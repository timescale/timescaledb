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

--check some error conditions
SELECT attach_tablespace('tablespace2', NULL);
SELECT attach_tablespace(NULL, 'tspace_2dim');
SELECT attach_tablespace('none_existing_tablespace', 'tspace_2dim');
SELECT attach_tablespace('tablespace2', 'none_existing_table');

--attach another tablespace without first creating it --> should generate error
SELECT attach_tablespace('tablespace2', 'tspace_2dim');
--attach the same tablespace twice to same table should also generate error
SELECT attach_tablespace('tablespace1', 'tspace_2dim');

\c single :ROLE_SUPERUSER
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER_2 LOCATION :TEST_TABLESPACE2_PATH;
\c single :ROLE_DEFAULT_PERM_USER_2

--attach without permissions on the table should fail
SELECT attach_tablespace('tablespace2', 'tspace_2dim');

\c single :ROLE_DEFAULT_PERM_USER

--attach without permissions on the tablespace should also fail
SELECT attach_tablespace('tablespace2', 'tspace_2dim');

\c single :ROLE_SUPERUSER
GRANT :ROLE_DEFAULT_PERM_USER_2 TO :ROLE_DEFAULT_PERM_USER;
\c single :ROLE_DEFAULT_PERM_USER

--should work with permissions on both the table and the tablespace
SELECT attach_tablespace('tablespace2', 'tspace_2dim');

SELECT * FROM _timescaledb_catalog.tablespace;
SELECT * FROM show_tablespaces('tspace_2dim');

--insert into another chunk
INSERT INTO tspace_2dim VALUES ('2017-01-20T09:00:01', 24.3, 'brown');

SELECT * FROM test.show_subtables('tspace_2dim');

--indexes should inherit the tablespace of their chunk
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

--
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
-- User doesn't have permission on tablespace1 --> error
CREATE TABLE tspace_1dim(time timestamp, temp float, device text);

-- Grant permission to tablespace1
SET ROLE :ROLE_DEFAULT_PERM_USER;
GRANT CREATE ON TABLESPACE tablespace1 TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
CREATE TABLE tspace_1dim(time timestamp, temp float, device text);

SELECT create_hypertable('tspace_1dim', 'time');
SELECT attach_tablespace('tablespace1', 'tspace_1dim');
SELECT attach_tablespace('tablespace2', 'tspace_1dim');

SELECT * FROM _timescaledb_catalog.tablespace;

INSERT INTO tspace_1dim VALUES ('2017-01-20T09:00:01', 24.3, 'blue');
INSERT INTO tspace_1dim VALUES ('2017-03-20T09:00:01', 24.3, 'brown');

SELECT * FROM test.show_subtablesp('tspace_%');
--indexes should inherit the tablespace of their chunk, unless the
--parent index has a tablespace set, in which case the chunks'
--corresponding indexes are pinned to the parent index's
--tablespace. The parent index can have a tablespace set in two cases:
--(1) if explicitly set in CREATE INDEX, or (2) if the main table was
--created with a tablespace, because then default indexes will be
--created in that tablespace too.
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

--detach tablespace1 from all tables. Due to lack of permissions,
--should only detach from 'tspace_1dim' (1 tablespace)
SELECT detach_tablespace('tablespace1');
SELECT * FROM _timescaledb_catalog.tablespace;
SELECT * FROM show_tablespaces('tspace_1dim');
SELECT * FROM show_tablespaces('tspace_2dim');

--detach the other tablespace
SELECT detach_tablespace('tablespace2', 'tspace_1dim');
SELECT * FROM _timescaledb_catalog.tablespace;
SELECT * FROM show_tablespaces('tspace_1dim');
SELECT * FROM show_tablespaces('tspace_2dim');

--detaching a tablespace from table without permissions should fail
SELECT detach_tablespace('tablespace2', 'tspace_2dim');
SELECT detach_tablespaces('tspace_2dim');

--set other user should make detach work
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT detach_tablespaces('tspace_2dim');
SELECT * FROM _timescaledb_catalog.tablespace;
SELECT * FROM show_tablespaces('tspace_1dim');
SELECT * FROM show_tablespaces('tspace_2dim');

--cleanup
DROP TABLE tspace_1dim CASCADE;
DROP TABLE tspace_2dim CASCADE;

DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;

-- revoke grants
\c single :ROLE_SUPERUSER
REVOKE :ROLE_DEFAULT_PERM_USER_2 FROM :ROLE_DEFAULT_PERM_USER;
