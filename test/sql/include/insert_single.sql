-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE PUBLIC."one_Partition" (
  "timeCustom" BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);
CREATE INDEX ON PUBLIC."one_Partition" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL;
CREATE INDEX ON PUBLIC."one_Partition" ("timeCustom" DESC NULLS LAST, series_0) WHERE series_0 IS NOT NULL;
CREATE INDEX ON PUBLIC."one_Partition" ("timeCustom" DESC NULLS LAST, series_1)  WHERE series_1 IS NOT NULL;
CREATE INDEX ON PUBLIC."one_Partition" ("timeCustom" DESC NULLS LAST, series_2) WHERE series_2 IS NOT NULL;
CREATE INDEX ON PUBLIC."one_Partition" ("timeCustom" DESC NULLS LAST, series_bool) WHERE series_bool IS NOT NULL;

\c :DBNAME :ROLE_SUPERUSER
CREATE SCHEMA "one_Partition" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :DBNAME :ROLE_DEFAULT_PERM_USER;

SELECT * FROM create_hypertable('"public"."one_Partition"', 'timeCustom', associated_schema_name=>'one_Partition', chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

--output command tags
\set QUIET off
BEGIN;
\COPY "one_Partition" FROM 'data/ds1_dev1_1.tsv' NULL AS '';
COMMIT;

INSERT INTO "one_Partition"("timeCustom", device_id, series_0, series_1) VALUES
(1257987600000000000, 'dev1', 1.5, 1),
(1257987600000000000, 'dev1', 1.5, 2),
(1257894000000000000, 'dev2', 1.5, 1),
(1257894002000000000, 'dev1', 2.5, 3);

INSERT INTO "one_Partition"("timeCustom", device_id, series_0, series_1) VALUES
(1257894000000000000, 'dev2', 1.5, 2);
\set QUIET on
