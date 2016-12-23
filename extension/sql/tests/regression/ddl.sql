\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

\c Test1


CREATE TABLE PUBLIC."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1,
  humidity numeric NULL DEFAULT 0,
  sensor_1 NUMERIC NULL DEFAULT 1,
  sensor_2 NUMERIC NOT NULL DEFAULT 1,
  sensor_3 NUMERIC NOT NULL DEFAULT 1,
  sensor_4 NUMERIC NOT NULL DEFAULT 1
);
CREATE INDEX ON PUBLIC."Hypertable_1" (time, "Device_id");

SELECT * FROM add_hypertable('"public"."Hypertable_1"', 'time', 'Device_id');
SELECT * FROM hypertable;
SELECT * FROM hypertable_index;

CREATE INDEX ON PUBLIC."Hypertable_1" (time, "temp_c");
CREATE INDEX "ind_humidity" ON PUBLIC."Hypertable_1" (time, "humidity");
CREATE INDEX "ind_sensor_1" ON PUBLIC."Hypertable_1" (time, "sensor_1");
SELECT set_is_distinct_flag('"public"."Hypertable_1"', 'sensor_1', TRUE);
SELECT set_is_distinct_flag('"public"."Hypertable_1"', 'sensor_2', TRUE);

BEGIN;
SELECT *
FROM create_temp_copy_table('public."Hypertable_1"', 'copy_t');
\COPY copy_t FROM 'data/ds2_ddl_1.tsv' NULL AS '';
SELECT * FROM copy_t;
SELECT *
FROM insert_data('public."Hypertable_1"', 'copy_t');
COMMIT;

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN SELECT * FROM PUBLIC."Hypertable_1";

\d+ PUBLIC."Hypertable_1"
\d+ "_sys_1_"."_hyper_1_root"
\d+ _sys_1_._hyper_1_1_0_1_data
SELECT * FROM PUBLIC.default_replica_node;


\c test2

\d+ PUBLIC."Hypertable_1"
\d+ "_sys_1_"."_hyper_1_root"

SELECT set_is_distinct_flag('"public"."Hypertable_1"', 'sensor_2', FALSE);
SELECT set_is_distinct_flag('"public"."Hypertable_1"', 'Device_id', TRUE);
ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN temp_f INTEGER NOT NULL DEFAULT 31;
ALTER TABLE PUBLIC."Hypertable_1" DROP COLUMN temp_c;
ALTER TABLE PUBLIC."Hypertable_1" DROP COLUMN sensor_4;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN humidity SET DEFAULT 100;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_1 DROP DEFAULT;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2 SET DEFAULT NULL;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_1 SET NOT NULL;
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2 DROP NOT NULL;
ALTER TABLE PUBLIC."Hypertable_1" RENAME COLUMN sensor_2 TO sensor_2_renamed;
ALTER TABLE PUBLIC."Hypertable_1" RENAME COLUMN sensor_3 TO sensor_3_renamed;
DROP INDEX "ind_sensor_1";

--expect error cases
\set ON_ERROR_STOP 0
ALTER TABLE PUBLIC."Hypertable_1" ALTER COLUMN sensor_2_renamed SET DATA TYPE int;
ALTER INDEX "ind_humidity" RENAME TO "ind_humdity2";
\set ON_ERROR_STOP 1

--create column with same name as previously renamed one
ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN sensor_3 BIGINT NOT NULL DEFAULT 131;
--create column with same name as previously dropped one
ALTER TABLE PUBLIC."Hypertable_1" ADD COLUMN sensor_4 BIGINT NOT NULL DEFAULT 131;


\d+ PUBLIC."Hypertable_1"
\d+ "_sys_1_"."_hyper_1_root"
SELECT * FROM _sys_1_._hyper_1_0_1_distinct_data;


\c Test1
\d+ PUBLIC."Hypertable_1"
\d+ "_sys_1_"."_hyper_1_root"
\d+ _sys_1_._hyper_1_1_0_1_data

SELECT * FROM PUBLIC."Hypertable_1";

