-- Type for bloom filters used by the sparse indexes on compressed hypertables.
CREATE TYPE _timescaledb_internal.bloom1;

CREATE FUNCTION _timescaledb_functions.bloom1in(cstring) RETURNS _timescaledb_internal.bloom1 AS 'byteain' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_functions.bloom1out(_timescaledb_internal.bloom1) RETURNS cstring AS 'byteaout' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;

CREATE TYPE _timescaledb_internal.bloom1 (
    INPUT = _timescaledb_functions.bloom1in,
    OUTPUT = _timescaledb_functions.bloom1out,
    LIKE = bytea
);

CREATE FUNCTION _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_table;


-- New option `refresh_newest_first` for incremental cagg refresh policy
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL,
    buckets_per_batch INTEGER,
    max_batches_per_execution INTEGER
);

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    include_tiered_data BOOL = NULL,
    buckets_per_batch INTEGER = NULL,
    max_batches_per_execution INTEGER = NULL,
    refresh_newest_first BOOL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_functions' WHERE chunk_sizing_func_schema = '_timescaledb_internal' AND chunk_sizing_func_name = 'calculate_chunk_interval';

DROP VIEW IF EXISTS timescaledb_information.hypertables;

-- Rename Columnstore Policy jobs to Compression Policy
UPDATE _timescaledb_config.bgw_job SET application_name = replace(application_name, 'Compression Policy', 'Columnstore Policy') WHERE application_name LIKE '%Compression Policy%';

-- Split chunk
CREATE PROCEDURE @extschema@.split_chunk(
    chunk REGCLASS,
    split_at "any" = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE FUNCTION _timescaledb_functions.align_to_bucket(width INTERVAL, rng ANYRANGE)
RETURNS ANYRANGE AS
$body$
BEGIN
  RETURN _timescaledb_functions.make_range_from_internal_time(
         rng,
         @extschema@.time_bucket(width, lower(rng)),
         @extschema@.time_bucket(width, upper(rng) - '1 microsecond'::interval) + width
  );
END
$body$
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSTZRANGE, low_usec BIGINT, high_usec BIGINT
) RETURNS TSTZMULTIRANGE AS
$body$
  select multirange(tstzrange(_timescaledb_functions.to_timestamp(low_usec),
			      _timescaledb_functions.to_timestamp(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSRANGE, low_usec BIGINT, high_usec BIGINT
) RETURNS TSMULTIRANGE AS
$body$
  select multirange(tsrange(_timescaledb_functions.to_timestamp_without_timezone(low_usec),
			    _timescaledb_functions.to_timestamp_without_timezone(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_range_from_internal_time(
    base ANYRANGE, low_usec ANYELEMENT, high_usec ANYELEMENT
) RETURNS anyrange
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_min(REGTYPE) RETURNS BIGINT
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_max(REGTYPE) RETURNS BIGINT
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

DROP FUNCTION IF EXISTS @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB,
  initial_start TIMESTAMPTZ,
  scheduled BOOL,
  check_config REGPROC,
  fixed_schedule BOOL,
  timezone TEXT
);

CREATE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL,
  fixed_schedule BOOL DEFAULT TRUE,
  timezone TEXT DEFAULT NULL,
  job_name TEXT DEFAULT NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL,
    max_runtime INTERVAL,
    max_retries INTEGER,
    retry_period INTERVAL,
    scheduled BOOL,
    config JSONB,
    next_start TIMESTAMPTZ,
    if_exists BOOL,
    check_config REGPROC,
    fixed_schedule BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT
);

CREATE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE,
    check_config REGPROC = NULL,
    fixed_schedule BOOL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT DEFAULT NULL,
    job_name TEXT DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, application_name name)
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;