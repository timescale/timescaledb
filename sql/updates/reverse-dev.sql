DROP FUNCTION IF EXISTS _timescaledb_functions.move_to_columnstore(REGCLASS, BOOLEAN);

-- Remove move to columnstore policy jobs since the policy does not exist in the older version.
DELETE FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_functions' AND proc_name = 'policy_move_to_columnstore';
DROP FUNCTION IF EXISTS @extschema@.add_move_to_columnstore_policy(REGCLASS, BOOL, INTERVAL, TIMESTAMPTZ, TEXT, INTEGER, BOOL);
DROP FUNCTION IF EXISTS @extschema@.remove_move_to_columnstore_policy(REGCLASS, BOOL);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_move_to_columnstore(INTEGER, JSONB);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_move_to_columnstore_check(JSONB);

-- Clear the concurrent_compress status bit
UPDATE _timescaledb_catalog.hypertable SET status = status & ~16 WHERE status & 16 <> 0;
