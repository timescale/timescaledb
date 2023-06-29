-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE conditions (
    "time" TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    temperature INTEGER NOT NULL,
    device_id INTEGER NOT NULL
);

SELECT table_name FROM create_hypertable('conditions', 'time');

INSERT INTO
    conditions ("time", city, temperature, device_id)
VALUES
  ('2021-06-14 00:00:00-00', 'Moscow', 26,1),
  ('2021-06-15 00:00:00-00', 'Berlin', 22,2),
  ('2021-06-16 00:00:00-00', 'Stockholm', 24,3),
  ('2021-06-17 00:00:00-00', 'London', 24,4),
  ('2021-06-18 00:00:00-00', 'London', 27,4),
  ('2021-06-19 00:00:00-00', 'Moscow', 28,4);

ALTER TABLE conditions SET (timescaledb.compress);

SELECT * FROM _timescaledb_catalog.chunk ORDER BY 1,2;

UPDATE _timescaledb_catalog.chunk SET compressed_chunk_id = NULL WHERE id = 1;

SELECT * FROM _timescaledb_catalog.chunk ORDER BY 1,2;

SELECT _timescaledb_functions.create_compressed_chunks_for_hypertable('conditions');

SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable;

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunks_for_hypertable('_timescaledb_internal._compressed_hypertable_2');
SELECT _timescaledb_functions.create_compressed_chunks_for_hypertable('nonexistant');
\set ON_ERROR_STOP 1
