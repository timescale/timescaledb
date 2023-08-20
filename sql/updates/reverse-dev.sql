DROP FUNCTION IF EXISTS @extschema@.alter_job(
    INTEGER,
    INTERVAL,
    INTERVAL,
    INTEGER,
    INTERVAL,
    BOOL,
    JSONB,
    TIMESTAMPTZ,
    BOOL,
    REGPROC,
    BOOL,
    TIMESTAMPTZ,
    TEXT
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
    check_config REGPROC = NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT)
AS '@MODULE_PATHNAME@', 'ts_job_alter'
LANGUAGE C VOLATILE;

ALTER FUNCTION _timescaledb_functions.insert_blocker() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.drop_dist_ht_invalidation_trigger(integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_create_command(name) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.to_unix_microseconds(timestamptz) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_timestamp(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_timestamp_without_timezone(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_date(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_interval(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.interval_to_usec(interval) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.time_to_internal(anyelement) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.subtract_integer_from_now(regclass, bigint) SET SCHEMA _timescaledb_internal;

