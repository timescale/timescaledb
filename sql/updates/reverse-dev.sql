DROP FUNCTION IF EXISTS _timescaledb_functions.remove_dropped_chunk_metadata(INTEGER);

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_origin = '' WHERE bucket_origin IS NULL;
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_timezone = '' WHERE bucket_timezone IS NULL;

CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      CASE WHEN bucket_func::text like 'timescaledb_experimental%' THEN true ELSE false END,
      split_part(bucket_func::regproc::text, '.', 2),
      bucket_width,
      bucket_origin,
      bucket_timezone
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The schema of the function. Equals TRUE for "timescaledb_experimental", FALSE otherwise.
  experimental bool NOT NULL,
  -- Name of the bucketing function, e.g. "time_bucket" or "time_bucket_ng"
  name text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- `origin` argument of the function provided by the user
  origin text NOT NULL,
  -- `timezone` argument of the function provided by the user
  timezone text NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;

--
-- End rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

-- Convert _timescaledb_catalog.continuous_aggs_bucket_function.origin back to Timestamp
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function
   SET origin = origin::timestamptz::timestamp::text
   WHERE length(origin) > 1;

-- only create stub
CREATE FUNCTION _timescaledb_functions.get_chunk_relstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, num_pages INTEGER, num_tuples REAL, num_allvisible INTEGER)
AS $$BEGIN END$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.get_chunk_colstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, att_num INTEGER, nullfrac REAL, width INTEGER, distinctval REAL, slotkind INTEGER[], slotopstrings CSTRING[], slotcollations OID[], slot1numbers FLOAT4[], slot2numbers FLOAT4[], slot3numbers FLOAT4[], slot4numbers FLOAT4[], slot5numbers FLOAT4[], slotvaluetypetrings CSTRING[], slot1values CSTRING[], slot2values CSTRING[], slot3values CSTRING[], slot4values CSTRING[], slot5values CSTRING[])
AS $$BEGIN END$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;


