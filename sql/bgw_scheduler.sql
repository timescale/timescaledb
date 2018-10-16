-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

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

CREATE OR REPLACE FUNCTION add_drop_chunks_policy(main_table REGCLASS, older_than INTERVAL, cascade BOOL = FALSE, if_not_exists BOOL = false) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_add_drop_chunks_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION add_recluster_policy(main_table REGCLASS, index_name NAME, if_not_exists BOOL = false) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_add_recluster_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_drop_chunks_policy(main_table REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_remove_drop_chunks_policy'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION remove_recluster_policy(main_table REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_remove_recluster_policy'
LANGUAGE C VOLATILE STRICT;
