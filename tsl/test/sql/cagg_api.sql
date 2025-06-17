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

-- Generate some invalidations. These new values need to be before the
-- invalidation threshold, which is set to end time of the insertion
-- above.
INSERT INTO hyper_ts
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-04 10:00:00'::timestamp,
                         '2025-01-04 11:12:00', '9 minutes') time;

INSERT INTO hyper_tstz
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-04 12:00:00'::timestamptz,
                         '2025-01-04 13:12:00', '8 minutes') time;

-- Check the invalidation threshold. If that is not after the
-- insertions above, nothing will show up in the invalidation log.
SELECT hypertable_id,
       _timescaledb_functions.to_timestamp_without_timezone(watermark) AS watermark
  FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
  ORDER BY 1;

-- Check that there indeed is something in the hypertable invalidation
-- log. If not, this will fail anyway. We show the "raw" timestamps,
-- which is in UTC if it was originally a timestamp with timezone.
--
-- We ignore duplicates since those will be merged when moving
-- invalidations and hence does not affect correctness and can cause
-- test flakiness otherwise.
SELECT DISTINCT
       hypertable_id,
       _timescaledb_functions.to_timestamp_without_timezone(lowest_modified_value) AS start,
       _timescaledb_functions.to_timestamp_without_timezone(greatest_modified_value) AS finish
  FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
  ORDER BY 1;

\set VERBOSITY default

-- Check that we can handle hypertables with single and multiple
-- dimensions. This function is not part of the official API but we
-- add tests for it here.
SELECT _timescaledb_functions.get_hypertable_id('hyper_ts', 'timestamp');
SELECT _timescaledb_functions.get_hypertable_id('hyper_tstz', 'timestamptz');
SELECT _timescaledb_functions.get_hypertable_id('hyper_multi', 'timestamptz');

\set ON_ERROR_STOP 0
-- Not a hypertable
SELECT _timescaledb_functions.get_hypertable_id('normal_ts', 'timestamp');
SELECT _timescaledb_functions.get_hypertable_invalidations('normal_ts', null::timestamp, array['15 minutes', '1 hour']::interval[]);
-- No continuous aggregate connected
SELECT _timescaledb_functions.get_hypertable_id('hyper_no_cagg', 'timestamp');
SELECT _timescaledb_functions.get_hypertable_invalidations('hyper_no_cagg', null::timestamp, array['15 minutes', '1 hour']::interval[]);
-- Wrong type used
SELECT _timescaledb_functions.get_hypertable_id('hyper_ts', 'timestamptz');
SELECT _timescaledb_functions.get_hypertable_invalidations('hyper_ts', null::timestamptz, array['15 minutes', '1 hour']::interval[]);
\set ON_ERROR_STOP 1

SELECT * INTO saved_invalidations_1
  FROM _timescaledb_functions.get_hypertable_invalidations('hyper_ts', null::timestamp, array['15 minutes', '1 hour']::interval[]);

SELECT bucket_width, invalidations FROM saved_invalidations_1;

-- Calling it twice should return same invalidations since we haven't
-- inserted anything between.
SELECT * INTO saved_invalidations_2
  FROM _timescaledb_functions.get_hypertable_invalidations('hyper_ts', null::timestamp, array['15 minutes', '1 hour']::interval[]);

SELECT * FROM saved_invalidations_2 s1
    FULL JOIN saved_invalidations_2 s2 ON row(s1.*) = row(s2.*)
        WHERE s1.token IS NULL OR s2.token IS NULL;

SELECT token FROM saved_invalidations_1 LIMIT 1 \gset

-- Test some error cases
\set ON_ERROR_STOP 0
CALL _timescaledb_functions.accept_hypertable_invalidations('normal_ts', :'token');
CALL _timescaledb_functions.accept_hypertable_invalidations('hyper_no_cagg', :'token');
CALL _timescaledb_functions.accept_hypertable_invalidations('hyper_ts', 'garbage');
\set ON_ERROR_STOP 1

-- Accept the invalidations as processed by passing in the token.
CALL _timescaledb_functions.accept_hypertable_invalidations('hyper_ts', :'token');

-- This should now show no validations.
SELECT bucket_width, invalidations
  FROM _timescaledb_functions.get_hypertable_invalidations(
       'hyper_ts',
       null::timestamp,
       array['15 minutes', '1 hour']::interval[]
  );

-- Check that the invalidations associated with the token are removed
-- when there are inserts in between.

-- First insert some rows. All invalidations were removed above, so we
-- should only see these.
INSERT INTO hyper_ts
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-03 10:00:00'::timestamp,
                         '2025-01-03 11:12:00', '10 minutes') time;

-- Get invalidations from the hypertable, with a token.
SELECT * INTO saved_3
  FROM _timescaledb_functions.get_hypertable_invalidations(
	'hyper_ts',
	null::timestamp,
	array['15 minutes', '1 hour']::interval[]
  );

SELECT token FROM saved_3 LIMIT 1 \gset

-- Insert some more rows into the hypertable that are disjoint with
-- the invalidations associated with the token above. These should be
-- fully removed.
INSERT INTO hyper_ts
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-02 10:00:00'::timestamp,
                         '2025-01-02 11:12:00', '10 minutes') time;

-- Insert some more rows into the hypertable that are inside the range
-- associated with the token above. These should remain after
-- accepting the token.
INSERT INTO hyper_ts
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2025-01-03 10:30:00'::timestamp,
                         '2025-01-03 10:50:00', '5 minutes') time;

-- Check that we have them in the invalidation log
SELECT bucket_width, unnest(invalidations) AS inval
  FROM _timescaledb_functions.get_hypertable_invalidations(
	'hyper_ts',
	null::timestamp,
	array['15 minutes', '1 hour']::interval[]
  )
ORDER BY 1, 2;

-- Accept the original invalidations
CALL _timescaledb_functions.accept_hypertable_invalidations('hyper_ts', :'token');

-- Check that we only removed the ones associated with the token.
SELECT bucket_width, unnest(invalidations) AS inval
  FROM _timescaledb_functions.get_hypertable_invalidations(
	'hyper_ts',
	null::timestamp,
	array['15 minutes', '1 hour']::interval[]
  )
ORDER BY 1, 2;


