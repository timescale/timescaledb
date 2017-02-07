\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/create_clustered_db.sql

\o
\set ECHO ALL
\c meta

-- Expect error when adding user again
\set ON_ERROR_STOP 0
SELECT add_cluster_user();
\set ON_ERROR_STOP 1

\c meta
-- Expect error when adding node again
\set ON_ERROR_STOP 0
SELECT add_node('test2' :: NAME, 'localhost', 5432);
SELECT add_node('test2' :: NAME, 'otherhost', 5432);
\set ON_ERROR_STOP 1

\c Test1

CREATE TABLE PUBLIC."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);
CREATE INDEX ON PUBLIC."Hypertable_1" (time, "Device_id");

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1_mispelled"', 'time', 'Device_id');
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time_mispelled', 'Device_id');
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'Device_id', 'Device_id');
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id_mispelled');

INSERT INTO PUBLIC."Hypertable_1" VALUES(1,'dev_1', 3); 

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id');

DELETE FROM  PUBLIC."Hypertable_1" ;
\set ON_ERROR_STOP 1

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id');

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id');
