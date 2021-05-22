-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_retention(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_retention_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_reorder(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_compression(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_compression_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_recompression(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_recompression_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_refresh_continuous_aggregate(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_proc'
LANGUAGE C;
