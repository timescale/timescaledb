-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Get information about the materialization table and bucket width.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_info(
    continuous_aggregate REGCLASS
) RETURNS RECORD AS
$body$
DECLARE
  info RECORD;
BEGIN
    SELECT mat_hypertable_id AS materialization_id,
           bucket_width::interval AS bucket_width
      INTO info
      FROM _timescaledb_catalog.continuous_agg,
   LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id)
     WHERE format('%I.%I', user_view_schema, user_view_name)::regclass = continuous_aggregate;

    IF NOT FOUND THEN
        RAISE '"%" is not a continuous aggregate', continuous_aggregate
        USING ERRCODE = 'wrong_object_type';
    END IF;

    RETURN info;
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

-- Add new invalidations to the materialization invalidation log.
--
-- This will add the range to the materialization invalidations for
-- the continuous aggregate. The range will automatically be "aligned"
-- to the bucket width to ensure that it covers all buckets that it
-- touches.
CREATE OR REPLACE PROCEDURE _timescaledb_functions.add_materialization_invalidations(
    continuous_aggregate regclass,
    invalidation tsrange
) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, invalidation);
BEGIN
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    VALUES (info.materialization_id,
            _timescaledb_functions.to_unix_microseconds(lower(aligned)),
            _timescaledb_functions.to_unix_microseconds(upper(aligned)));
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.add_materialization_invalidations(
    continuous_aggregate REGCLASS,
    invalidation TSTZRANGE
) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSTZRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, invalidation);
BEGIN
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    VALUES (info.materialization_id,
            _timescaledb_functions.to_unix_microseconds(lower(aligned)),
            _timescaledb_functions.to_unix_microseconds(upper(aligned)));
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

-- Get raw ranges from the materialization invalidation log
--
-- This is a cleaned-up version of the timestamps, still in Unix
-- microseconds, with nulls for '-infinity' and '+infinity' and
-- invalid entries removed.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_raw_materialization_ranges(typ regtype)
RETURNS TABLE (materialization_id integer,
               lowest_modified_value bigint,
               greatest_modified_value bigint)
AS $$
   WITH
     min_max_values AS MATERIALIZED (
       SELECT _timescaledb_functions.get_internal_time_min(typ) AS min,
       _timescaledb_functions.get_internal_time_max(typ) AS max
   )
   SELECT materialization_id,
          NULLIF(lowest_modified_value, min_max_values.min),
          NULLIF(greatest_modified_value, min_max_values.max)
     FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log, min_max_values
    WHERE lowest_modified_value
          BETWEEN min_max_values.min
              AND min_max_values.max
      AND greatest_modified_value
          BETWEEN min_max_values.min
              AND min_max_values.max
$$
LANGUAGE SQL
SET search_path TO pg_catalog, pg_temp;

-- Get materialization invalidations for a continuous aggregate.
--
-- Note that this will modify the materialization invalidation table
-- to be able to extract the restricted range of invalidations.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_invalidations(
    continuous_aggregate REGCLASS,
    restriction TSTZRANGE
) RETURNS TABLE (invalidations TSTZMULTIRANGE) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSTZRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, restriction);
BEGIN
    -- Compute the multirange for the invalidations inside the
    -- restriction passed down to the function and return the ranges.
    RETURN QUERY
    WITH
      ranges AS (
          SELECT materialization_id,
                 range_agg(_timescaledb_functions.make_multirange_from_internal_time(
			null::tstzrange,
			lowest_modified_value,
                        greatest_modified_value)) AS invals
            FROM _timescaledb_functions.get_raw_materialization_ranges('timestamptz'::regtype)
          GROUP BY materialization_id
      )
    SELECT range_agg(invals * multirange(aligned))
      FROM ranges
     WHERE invals && aligned
       AND materialization_id = info.materialization_id;
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_invalidations(
    continuous_aggregate REGCLASS,
    restriction TSRANGE
) RETURNS TABLE (invalidations TSMULTIRANGE) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, restriction);
BEGIN
    -- Compute the multirange for the invalidations inside the
    -- restriction passed down to the function and return the ranges.
    RETURN QUERY
    WITH
      ranges AS (
          SELECT materialization_id,
                 range_agg(_timescaledb_functions.make_multirange_from_internal_time(
			null::tsrange,
			lowest_modified_value,
                        greatest_modified_value)) AS invals
            FROM _timescaledb_functions.get_raw_materialization_ranges('timestamp'::regtype)
          GROUP BY materialization_id
      )
    SELECT range_agg(invals * multirange(aligned))
      FROM ranges
     WHERE invals && aligned
       AND materialization_id = info.materialization_id;
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;
