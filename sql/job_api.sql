-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL,
  fixed_schedule BOOL DEFAULT TRUE,
  timezone TEXT DEFAULT NULL
) RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.delete_job(job_id INTEGER) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_job_delete' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE PROCEDURE @extschema@.run_job(job_id INTEGER) AS '@MODULE_PATHNAME@', 'ts_job_run' LANGUAGE C;

-- Returns the updated job schedule values
CREATE OR REPLACE FUNCTION @extschema@.alter_job(
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

CREATE OR REPLACE FUNCTION _timescaledb_internal.alter_job_set_hypertable_id(
    job_id INTEGER,
    hypertable REGCLASS )
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_alter_set_hypertable_id'
LANGUAGE C VOLATILE;
