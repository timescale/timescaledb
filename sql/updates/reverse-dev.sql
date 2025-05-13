DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS);
DROP PROCEDURE @extschema@.add_move_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT, BOOL);
DROP PROCEDURE @extschema@.remove_move_hypertable_invalidations_policy(REGCLASS, BOOL);
DROP PROCEDURE _timescaledb_functions.policy_move_hypertable_invalidations(INTEGER, JSONB);
DROP FUNCTION _timescaledb_functions.policy_move_hypertable_invalidations_check(JSONB);
