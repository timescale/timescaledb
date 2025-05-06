-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SET timezone TO CET;
SET datestyle TO ISO;

CREATE VIEW continuous_aggregates AS
SELECT mat_hypertable_id AS materialization_id,
       format('%I.%I', user_view_schema, user_view_name)::regclass AS continuous_aggregate
  FROM _timescaledb_catalog.hypertable
  JOIN _timescaledb_catalog.continuous_agg
    ON hypertable.id = continuous_agg.mat_hypertable_id;

CREATE TABLE hyper_ts (time timestamp NOT NULL, value float);
CREATE TABLE hyper_tstz (time timestamptz NOT NULL, value float);
CREATE TABLE hyper_multi (time timestamptz NOT NULL, device int, value float);
CREATE TABLE hyper_no_cagg (time timestamptz NOT NULL, device int, value float);
CREATE TABLE normal_ts(time timestamp NOT NULL, value float);

SELECT * FROM create_hypertable('hyper_ts', 'time');
SELECT * FROM create_hypertable('hyper_tstz', 'time');
SELECT * FROM create_hypertable('hyper_no_cagg', 'time');
SELECT * FROM create_hypertable('hyper_multi', 'time', 'device', 4);

INSERT INTO hyper_ts
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-01'::timestamp,
                         '2025-01-06', '1m') time;

INSERT INTO hyper_tstz
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-01'::timestamptz,
                         '2025-01-06', '1m') time;

CREATE MATERIALIZED VIEW ts_temperature_1h
  WITH  (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time), avg(value)
  FROM hyper_ts
GROUP BY 1;

CREATE MATERIALIZED VIEW ts_temperature_15m
  WITH  (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', time), avg(value)
  FROM hyper_ts
GROUP BY 1;

CREATE MATERIALIZED VIEW tstz_temperature_1h
  WITH  (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time), avg(value)
  FROM hyper_tstz
GROUP BY 1;

CREATE MATERIALIZED VIEW tstz_temperature_15m
  WITH  (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', time), avg(value)
  FROM hyper_tstz
GROUP BY 1;

CREATE MATERIALIZED VIEW multi_temperature_15m
  WITH  (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', time), avg(value)
  FROM hyper_multi
GROUP BY 1;

SET search_path TO _timescaledb_functions, public;

-- These are not part of the API, but we test them here just to make
-- sure they work as expected.
SELECT table_name, get_materialization_info(table_name)
  FROM (
      VALUES ('tstz_temperature_15m'), ('multi_temperature_15m')
  ) t(table_name);

\set ON_ERROR_STOP 0
SELECT get_materialization_info('hyper_no_cagg');
\set ON_ERROR_STOP 1

-- This is not part of the API either, but added a test here to make
-- sure that it works as expected.
SELECT materialization_id,
       to_timestamp(lowest_modified_value),
       to_timestamp(greatest_modified_value)
  FROM get_raw_materialization_ranges('timestamptz');

-- Here are tests of the API
SELECT *
  INTO before
  FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

CALL _timescaledb_functions.add_materialization_invalidations(
     'tstz_temperature_15m'::regclass,
     '["2025-04-25 11:10:00+02","2025-04-26 11:14:00+02"]'::tstzrange
);

CALL _timescaledb_functions.add_materialization_invalidations(
     'ts_temperature_15m'::regclass,
     '["2025-04-25 11:10:00+02","2025-04-26 11:14:00+02"]'::tsrange
);

-- Custom refresh function that iterate over the ranges inside the
-- restriction and refresh them. This is to check that the refresh
-- function does the right thing with the ranges returned by the API
-- function.
CREATE PROCEDURE custom_refresh(cagg REGCLASS, restriction ANYRANGE) AS
$body$
DECLARE
  inval restriction%TYPE;
BEGIN
   FOR inval IN
   SELECT UNNEST(invalidations)
     FROM _timescaledb_functions.get_materialization_invalidations(cagg, restriction)
   LOOP
       RAISE NOTICE 'Updating range %', inval;
       CALL refresh_continuous_aggregate(cagg, lower(inval), upper(inval));
       COMMIT;
   END LOOP;
END
$body$
LANGUAGE plpgsql;

SELECT continuous_aggregate,
       to_timestamp(lhs.lowest_modified_value),
       to_timestamp(lhs.greatest_modified_value)
  FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log lhs
  JOIN continuous_aggregates USING (materialization_id)
LEFT JOIN before rhs ON row(lhs.*) = row(rhs.*)
 WHERE lhs.materialization_id IS NULL OR rhs.materialization_id IS NULL;

SELECT materialization_id,
       to_timestamp(lowest_modified_value),
       to_timestamp(greatest_modified_value)
  FROM get_raw_materialization_ranges('timestamptz');

SELECT * FROM _timescaledb_functions.get_materialization_invalidations(
       'ts_temperature_15m'::regclass,
       '["2025-04-25","2025-04-26"]'::tsrange
);

CALL custom_refresh('ts_temperature_15m', '["2025-04-25","2025-04-26"]'::tsrange);

SELECT * FROM _timescaledb_functions.get_materialization_invalidations(
       'ts_temperature_15m'::regclass,
       '["2025-04-25","2025-04-26"]'::tsrange
);

SELECT * FROM _timescaledb_functions.get_materialization_invalidations(
       'tstz_temperature_15m'::regclass,
       '["2025-04-25","2025-04-26"]'::tstzrange
);

CALL custom_refresh('tstz_temperature_15m', '["2025-04-25","2025-04-26"]'::tsrange);

SELECT * FROM _timescaledb_functions.get_materialization_invalidations(
       'tstz_temperature_15m'::regclass,
       '["2025-04-25","2025-04-26"]'::tstzrange
);
