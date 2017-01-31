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

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id');
SELECT * FROM _iobeamdb_catalog.hypertable;
SELECT * FROM _iobeamdb_catalog.hypertable_index;

CREATE INDEX ON PUBLIC."Hypertable_1" (time, "temp_c");
CREATE INDEX "ind_humidity" ON PUBLIC."Hypertable_1" (time, "humidity");
CREATE INDEX "ind_sensor_1" ON PUBLIC."Hypertable_1" (time, "sensor_1");

INSERT INTO PUBLIC."Hypertable_1"(time, "Device_id", temp_c, humidity, sensor_1, sensor_2, sensor_3, sensor_4)
VALUES(1257894000000000000, 'dev1', 30, 70, 1, 2, 3, 100);
--expect error cases
\set ON_ERROR_STOP 0
UPDATE ONLY PUBLIC."Hypertable_1" SET time = 0 WHERE TRUE;
DELETE FROM ONLY PUBLIC."Hypertable_1" WHERE "Device_id" = 'dev1';
\set ON_ERROR_STOP 1
