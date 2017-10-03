\ir include/insert_single.sql

\c single postgres
DO $$
BEGIN
    CREATE ROLE alt_usr2 LOGIN;
EXCEPTION
    WHEN duplicate_object THEN
        --mute error
END$$;

--needed for ddl ops:
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION alt_usr2;

--test creating and using schema as non-superuser
\c single alt_usr2
\dt

\set ON_ERROR_STOP 0
SELECT * FROM "one_Partition";
SELECT set_chunk_time_interval('"one_Partition"', 1::bigint);
select add_dimension('"one_Partition"', 'device_id', 2);
select attach_tablespace('"one_Partition"', 'tablespace1');
\set ON_ERROR_STOP 1

CREATE TABLE "1dim"(time timestamp, temp float);
SELECT create_hypertable('"1dim"', 'time');
INSERT INTO "1dim" VALUES('2017-01-20T09:00:01', 22.5);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:21', 21.2);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:47', 25.1);
SELECT * FROM "1dim";


\ir include/ddl_ops_1.sql
\ir include/ddl_ops_2.sql

--test proper denials for all security definer functions:
\c single postgres 
CREATE TABLE plain_table_su (time timestamp, temp float);
CREATE TABLE hypertable_su (time timestamp, temp float);
SELECT create_hypertable('hypertable_su', 'time');
CREATE INDEX "ind_1" ON hypertable_su (time);
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);

\c single alt_usr
--all of the following should produce errors
\set ON_ERROR_STOP 0
SELECT create_hypertable('plain_table_su', 'time');
CREATE INDEX ON plain_table_su (time, temp);
CREATE INDEX ON hypertable_su (time, temp);
DROP INDEX "ind_1";
ALTER INDEX "ind_1" RENAME TO "ind_2";
\set ON_ERROR_STOP 1

--test that I can't do anything to a non-owned hypertable.
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
SELECT * FROM hypertable_su;
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--grant read permissions
\c single postgres
GRANT SELECT ON hypertable_su TO alt_usr;

\c single alt_usr
SELECT * FROM hypertable_su;
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--grant read, insert permissions
\c single postgres
GRANT SELECT, INSERT ON hypertable_su TO alt_usr;

\c single alt_usr
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
SELECT * FROM hypertable_su;
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--change owner 
\c single postgres
ALTER TABLE hypertable_su OWNER TO alt_usr;

\c single alt_usr
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
SELECT * FROM hypertable_su;
CREATE INDEX ON hypertable_su (time, temp);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
