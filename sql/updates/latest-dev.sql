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

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);

DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_execute(
INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN, BOOLEAN
);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(
  INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN
);

CREATE PROCEDURE _timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  reindex_enabled     BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$
BEGIN
  -- empty body
END;
$$ LANGUAGE PLPGSQL;

DROP PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate REGCLASS,
    window_start "any",
    window_end "any",
    force BOOLEAN
);

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE,
    options                  JSONB = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 7, 1, 'COMPRESSION_ALGORITHM_UUID', 'uuid');

