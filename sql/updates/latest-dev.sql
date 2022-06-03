DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL);
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL);
DROP FUNCTION IF EXISTS @extschema@.detach_data_node;

DROP FUNCTION IF EXISTS @extschema@.add_job;
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_retention_check(INTEGER, JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_compression_check(INTEGER, JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_reorder_check(INTEGER, JSONB);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_refresh_continuous_aggregate_check(INTEGER, JSONB);

-- add fields for check function
ALTER TABLE _timescaledb_config.bgw_job
      ADD COLUMN check_schema NAME,
      ADD COLUMN check_name NAME;

-- no need to touch the telemetry jobs since they do not have a check
-- function.

-- add check function to reorder jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_reorder_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_reorder';

-- add check function to compression jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_compression_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_compression';

-- add check function to retention jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_retention_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_retention';

-- add check function to continuous aggregate refresh jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_refresh_continuous_aggregate_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_refresh_continuous_aggregate';
