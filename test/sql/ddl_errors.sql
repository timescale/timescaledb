-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE PUBLIC."Hypertable_1" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);
CREATE INDEX ON PUBLIC."Hypertable_1" (time, "Device_id");

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable(NULL, NULL);
SELECT * FROM create_hypertable('"public"."Hypertable_1"', NULL);
-- integer time dimensions require an explicit interval
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time');
-- space dimensions require explicit number of partitions
SELECT * FROM create_hypertable('"public"."Hypertable_1"', 'time', 'Device_id', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
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
\set ON_ERROR_STOP 1

INSERT INTO "Hypertable_1" VALUES (0, 1, 0);

\set ON_ERROR_STOP 0
ALTER TABLE _timescaledb_internal._hyper_1_1_chunk ALTER COLUMN temp_c DROP NOT NULL;
\set ON_ERROR_STOP 1

CREATE TABLE PUBLIC."Parent" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);

\set ON_ERROR_STOP 0
ALTER TABLE "Hypertable_1" INHERIT "Parent";
ALTER TABLE _timescaledb_internal._hyper_1_1_chunk INHERIT "Parent";
ALTER TABLE _timescaledb_internal._hyper_1_1_chunk NO INHERIT "Parent";
\set ON_ERROR_STOP 1

CREATE TABLE PUBLIC."Child" () INHERITS ("Parent");

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Parent"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
SELECT * FROM create_hypertable('"public"."Child"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

CREATE UNLOGGED TABLE PUBLIC."Hypertable_unlogged" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_unlogged"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

ALTER TABLE PUBLIC."Hypertable_unlogged" SET LOGGED;
SELECT * FROM create_hypertable('"public"."Hypertable_unlogged"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

CREATE TEMP TABLE "Hypertable_temp" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"Hypertable_temp"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

ALTER TABLE "Hypertable_1" SET UNLOGGED;
\set ON_ERROR_STOP 1

ALTER TABLE "Hypertable_1" SET LOGGED;
CREATE TABLE PUBLIC."Hypertable_1_replica_ident" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);
ALTER TABLE "Hypertable_1_replica_ident" REPLICA IDENTITY FULL;

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1_replica_ident"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
ALTER TABLE "Hypertable_1" REPLICA IDENTITY FULL;
\set ON_ERROR_STOP 1


CREATE TABLE PUBLIC."Hypertable_1_rule" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp_c int NOT NULL DEFAULT -1
);
CREATE RULE notify_me AS ON UPDATE TO "Hypertable_1_rule" DO ALSO NOTIFY "Hypertable_1_rule";

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1_rule"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

ALTER TABLE "Hypertable_1_rule" DISABLE RULE notify_me;

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."Hypertable_1_rule"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

DROP RULE notify_me ON "Hypertable_1_rule";
SELECT * FROM create_hypertable('"public"."Hypertable_1_rule"', 'time', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
CREATE RULE notify_me AS ON UPDATE TO "Hypertable_1_rule" DO ALSO NOTIFY "Hypertable_1_rule";
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
SELECT add_dimension(NULL,NULL);
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
SELECT attach_tablespace(NULL,NULL);
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
select set_number_partitions(NULL,NULL);
\set ON_ERROR_STOP 1
