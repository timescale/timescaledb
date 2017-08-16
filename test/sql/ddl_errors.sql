\o /dev/null
\ir include/create_single_db.sql
\o

-- Expect error when adding node again

CREATE TABLE PUBLIC."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);
CREATE INDEX ON PUBLIC."Hypertable_1" (time, "Device_id");

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1_mispelled"', 'time', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time_mispelled', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'Device_id', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id_mispelled', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

INSERT INTO PUBLIC."Hypertable_1" VALUES(1,'dev_1', 3);

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

DELETE FROM  PUBLIC."Hypertable_1" ;
\set ON_ERROR_STOP 1

SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
