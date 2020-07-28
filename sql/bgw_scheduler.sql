-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_stop'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_start'
LANGUAGE C VOLATILE;

INSERT INTO _timescaledb_config.bgw_job (id, application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
(1, 'Telemetry Reporter', 'telemetry_and_version_check_if_enabled', INTERVAL '24h', INTERVAL '100s', -1, INTERVAL '1h')
ON CONFLICT (id) DO NOTHING;

-- Add a retention policy to a hypertable or continuous aggregate.
-- The retention_window (typically an INTERVAL) determines the 
-- window beyond which data is dropped at the time
-- of execution of the policy (e.g., '1 week'). Note that the retention 
-- window will always align with chunk boundaries, thus the window 
-- might be larger than the given one, but never smaller. In other
-- words, some data beyond the retention window 
-- might be kept, but data within the window will never be deleted.
CREATE OR REPLACE FUNCTION add_retention_policy(
       hypertable REGCLASS,
       retention_window "any",
       if_not_exists BOOL = false,
       cascade_to_materializations BOOL = false
)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_add_retention_policy'
LANGUAGE C VOLATILE STRICT;

-- Remove the retention policy from a hypertable
CREATE OR REPLACE FUNCTION remove_retention_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_remove_retention_policy'
LANGUAGE C VOLATILE STRICT;

/* reorder policy */
CREATE OR REPLACE FUNCTION add_reorder_policy(hypertable REGCLASS, index_name NAME, if_not_exists BOOL = false) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_add'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_reorder_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_remove'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_reorder(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_proc'
LANGUAGE C;

/* compression policy */
CREATE OR REPLACE FUNCTION add_compression_policy(hypertable REGCLASS, older_than "any", if_not_exists BOOL = false)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_compression_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policy_compression_remove'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_compression(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_compression_proc'
LANGUAGE C;

-- Returns the updated job schedule values
CREATE OR REPLACE FUNCTION alter_job_schedule(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    if_exists BOOL = FALSE,
    next_start TIMESTAMPTZ = NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, next_start TIMESTAMPTZ)
AS '@MODULE_PATHNAME@', 'ts_alter_job_schedule'
LANGUAGE C VOLATILE;
