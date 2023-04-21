-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE PROCEDURE _timescaledb_internal.cagg_try_repair (
    cagg_view REGCLASS,
    force_rebuild BOOLEAN
) AS :MODULE_PATHNAME, 'ts_cagg_try_repair' LANGUAGE C SET client_min_messages TO DEBUG1;

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
  ('2021-06-19 00:00:00-00', 'Moscow', 28,4),
  ('2021-06-20 00:00:00-00', 'Moscow', 30,1),
  ('2021-06-21 00:00:00-00', 'Berlin', 31,1),
  ('2021-06-22 00:00:00-00', 'Stockholm', 34,1),
  ('2021-06-23 00:00:00-00', 'Stockholm', 34,2),
  ('2021-06-24 00:00:00-00', 'Moscow', 34,2),
  ('2021-06-25 00:00:00-00', 'London', 32,3),
  ('2021-06-26 00:00:00-00', 'Moscow', 32,3),
  ('2021-06-27 00:00:00-00', 'Moscow', 31,3);

CREATE TABLE devices (
    id INTEGER NOT NULL,
    name TEXT,
    location TEXT
);

INSERT INTO
    devices (id, name, location)
VALUES
    (1, 'thermo_1', 'Moscow'),
    (2, 'thermo_2', 'Berlin'),
    (3, 'thermo_3', 'London'),
    (4, 'thermo_4', 'Stockholm');

CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
    time_bucket(INTERVAL '1 week', "time") AS bucket,
    devices.name AS device_name,
    MIN(temperature),
    MAX(temperature),
    SUM(temperature)
FROM
    conditions
    JOIN devices ON devices.id = conditions.device_id
GROUP BY
    1, 2
WITH NO DATA;

\d+ conditions_summary
CALL refresh_continuous_aggregate('conditions_summary', NULL, '2021-06-22 00:00:00-00');
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

-- Execute repair for materialized only cagg
CALL _timescaledb_internal.cagg_try_repair('conditions_summary', FALSE);
\d+ conditions_summary
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

CALL _timescaledb_internal.cagg_try_repair('conditions_summary', TRUE);
\d+ conditions_summary
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

-- Switch to realtime cagg
ALTER MATERIALIZED VIEW conditions_summary SET (timescaledb.materialized_only=false);
\d+ conditions_summary
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

-- Execute repair for realtime cagg
CALL _timescaledb_internal.cagg_try_repair('conditions_summary', FALSE);
\d+ conditions_summary
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

CALL _timescaledb_internal.cagg_try_repair('conditions_summary', TRUE);
\d+ conditions_summary
SELECT * FROM conditions_summary ORDER BY bucket, device_name;

-- Tests without join
CREATE MATERIALIZED VIEW conditions_summary_nojoin
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 week', "time") AS bucket,
    MIN(temperature),
    MAX(temperature),
    SUM(temperature)
FROM
    conditions
GROUP BY
    1
WITH NO DATA;

CALL _timescaledb_internal.cagg_try_repair('conditions_summary_nojoin', TRUE);
\d+ conditions_summary_nojoin

DROP PROCEDURE _timescaledb_internal.cagg_try_repair (REGCLASS, BOOLEAN);
