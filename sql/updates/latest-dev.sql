CREATE PROCEDURE _timescaledb_functions.process_hypertable_invalidations(
    hypertable REGCLASS
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE PROCEDURE @extschema@.add_process_hypertable_invalidations_policy(
    hypertable REGCLASS,
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE PROCEDURE @extschema@.remove_process_hypertable_invalidations_policy(
       hypertable REGCLASS,
       if_exists BOOL = false
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';
