ALTER EXTENSION timescaledb DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS);
ALTER EXTENSION timescaledb DROP PROCEDURE @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
ALTER EXTENSION timescaledb DROP PROCEDURE @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);
ALTER EXTENSION timescaledb DROP PROCEDURE _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);

DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS);
DROP PROCEDURE @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP PROCEDURE @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);
DROP PROCEDURE _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
DROP FUNCTION _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);
