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
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Get hypertable id for a hypertable and execute common checks to
-- avoid duplicating them in the overloaded functions below.
--
-- This function is part of the internal API and not intended for
-- public consumption.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_id(
       hypertable REGCLASS,
       column_type REGTYPE
) RETURNS integer AS
$body$
DECLARE
   info RECORD;
BEGIN
   SELECT ht.id AS hypertable_id,
          di.column_type::regtype,
          EXISTS(SELECT FROM _timescaledb_catalog.continuous_agg where raw_hypertable_id = ht.id) AS has_cagg
     INTO info
     FROM _timescaledb_catalog.hypertable ht
     JOIN _timescaledb_catalog.dimension di ON ht.id = di.hypertable_id
    WHERE format('%I.%I', schema_name, table_name)::regclass = hypertable
      AND di.interval_length IS NOT NULL;

   IF info IS NULL THEN
      RAISE EXCEPTION 'table "%" is not a hypertable', hypertable
      USING ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF NOT info.has_cagg THEN
      RAISE EXCEPTION 'hypertable "%" has no continuous aggregate', hypertable
      USING HINT = 'Define a continuous aggregate for the hypertable to read invalidations.',
            ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF info.column_type <> get_hypertable_id.column_type THEN
      RAISE EXCEPTION 'wrong column type for hypertable %', hypertable
      USING HINT = format('hypertable type was "%s", but caller expected "%s"',
                          info.column_type, get_hypertable_id.column_type),
	    ERRCODE = 'datatype_mismatch';
   END IF;

   RETURN info.hypertable_id;
END
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Get hypertable invalidations ranges based on bucket size.
--
-- This will return a multirange for each bucket size passed in and a
-- token that can be used to accept the multirange.
--
-- Note that the token returned is not unique for each bucket size and
-- represents either the LSN or a Snapshot of what data was read to
-- produce the bucket ranges.
--
-- Currently, we only have support for timestamp with and without
-- timezone, but it is straightforward to add similar implementations
-- for integer types.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_invalidations(
   hypertable REGCLASS,
   base TIMESTAMPTZ,
   bucket_widths INTERVAL[]
) RETURNS TABLE (bucket_width INTERVAL, token TEXT, invalidations TSTZMULTIRANGE) AS
$body$
DECLARE
   l_hypertable_id INTEGER := _timescaledb_functions.get_hypertable_id(hypertable, 'timestamptz'::regtype);
BEGIN
   RETURN QUERY (
      WITH
         -- Collect ranges from the invalidation log and convert them
         -- to correct type.
         timestamps AS MATERIALIZED (
            SELECT _timescaledb_functions.to_timestamp(lowest_modified_value) AS start,
                   _timescaledb_functions.to_timestamp(greatest_modified_value) AS finish
              FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
             WHERE hypertable_id = l_hypertable_id
         ),
         -- Since a range can end at the end of a bucket and this
         -- range should not include the next bucket, we need to
         -- subtract one microsecond before computing the bucket and
         -- then add the width again.
         ranges AS MATERIALIZED (
           SELECT width,
                  tstzrange(@extschema@.time_bucket(width, start),
                            @extschema@.time_bucket(width, finish - '1 microsecond'::interval) + width) AS bucket
             FROM timestamps CROSS JOIN UNNEST(bucket_widths) w(width)
         )
      SELECT width, pg_current_snapshot()::text, range_agg(bucket) ranges
        FROM ranges GROUP BY width
   );
END
$body$
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_invalidations(
   hypertable REGCLASS,
   base TIMESTAMP,
   bucket_widths INTERVAL[]
) RETURNS TABLE (bucket_width INTERVAL, token TEXT, invalidations TSMULTIRANGE) AS
$body$
DECLARE
   l_hypertable_id INTEGER := _timescaledb_functions.get_hypertable_id(hypertable, 'timestamp'::regtype);
BEGIN
   RETURN QUERY (
      WITH
         -- Collect ranges from the invalidation log and convert them
         -- to correct type.
         timestamps AS MATERIALIZED (
            SELECT _timescaledb_functions.to_timestamp_without_timezone(lowest_modified_value) AS start,
                   _timescaledb_functions.to_timestamp_without_timezone(greatest_modified_value) AS finish
              FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
             WHERE hypertable_id = l_hypertable_id
         ),
         -- Compute bucket-aligned ranges from the ranges above. Since
         -- a range can end at the end of a bucket and this range
         -- should not include the next bucket, we need to subtract
         -- one microsecond before computing the bucket and then add
         -- the width again.
         ranges AS MATERIALIZED (
           SELECT width,
                  tsrange(@extschema@.time_bucket(width, start),
                          @extschema@.time_bucket(width, finish - '1 microsecond'::interval) + width) AS bucket
             FROM timestamps CROSS JOIN UNNEST(bucket_widths) w(width)
         )
      SELECT width, pg_current_snapshot()::text, range_agg(bucket)
        FROM ranges GROUP BY width
   );
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
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Accept a set of hypertable invalidations for a hypertable.
--
-- This procedure is used to accept all invalidations for a hypertable
-- using the token returned by a previous call of
-- get_hypertable_invalidations().
CREATE OR REPLACE PROCEDURE _timescaledb_functions.accept_hypertable_invalidations(
   hypertable REGCLASS,
   token TEXT
) AS
$body$
DECLARE
   info RECORD;
   errmsg TEXT;
BEGIN
   SELECT ht.id AS hypertable_id,
          (cagg.raw_hypertable_id IS NOT NULL) AS has_cagg
     INTO info
     FROM _timescaledb_catalog.hypertable ht
     LEFT JOIN _timescaledb_catalog.continuous_agg cagg ON cagg.raw_hypertable_id = ht.id
    WHERE format('%I.%I', schema_name, table_name)::regclass = hypertable;

   IF info IS NULL THEN
      RAISE EXCEPTION 'table "%" is not a hypertable', hypertable
      USING ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF NOT info.has_cagg THEN
      RAISE EXCEPTION 'hypertable "%" has no continuous aggregate', hypertable
      USING HINT = 'Define a continuous aggregate for the hypertable to handle invalidations.',
            ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   DELETE FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
    WHERE hypertable_id = info.hypertable_id
      AND pg_visible_in_snapshot(xmin::text::xid8, token::pg_snapshot);
EXCEPTION
    WHEN invalid_text_representation THEN
       RAISE EXCEPTION '%', SQLERRM
       USING HINT = 'Use the token from the get_hypertable_invalidations() call.',
             ERRCODE = 'invalid_text_representation';
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;
