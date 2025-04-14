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
  job_name name DEFAULT NULL
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
    job_name name DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, job_name name)
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;
