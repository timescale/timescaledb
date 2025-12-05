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

