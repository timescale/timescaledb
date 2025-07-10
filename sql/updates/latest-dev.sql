
-- block upgrade if hypercore access method is still in use
DO $$
BEGIN
  IF EXISTS(SELECT from pg_class c join pg_am am ON c.relam=am.oid AND am.amname='hypercore') THEN
    RAISE EXCEPTION 'TimescaleDB does no longer support the hypercore table access method. Convert all tables to heap access method before upgrading.';
  END IF;
END
$$;

DROP OPERATOR CLASS IF EXISTS int4_ops USING hypercore_proxy;
DROP ACCESS METHOD IF EXISTS hypercore_proxy;
DROP ACCESS METHOD IF EXISTS hypercore;

DROP FUNCTION IF EXISTS ts_hypercore_proxy_handler;
DROP FUNCTION IF EXISTS ts_hypercore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute;
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy;
DROP PROCEDURE IF EXISTS @extschema@.add_columnstore_policy;
DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies;
DROP FUNCTION IF EXISTS @extschema@.compress_chunk;
DROP PROCEDURE IF EXISTS @extschema@.convert_to_columnstore;

CREATE FUNCTION @extschema@.compress_chunk(
  uncompressed_chunk REGCLASS,
  if_not_compressed BOOLEAN = true,
  recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

CREATE PROCEDURE @extschema@.convert_to_columnstore(
    chunk REGCLASS,
    if_not_columnstore BOOLEAN = true,
    recompress BOOLEAN = false
) AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

CREATE PROCEDURE @extschema@.add_columnstore_policy(
    hypertable REGCLASS,
    after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    created_before INTERVAL = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL
) RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

CREATE PROCEDURE _timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  reindex_enabled     BOOLEAN,
  use_creation_time   BOOLEAN
)
AS $$ BEGIN END $$ LANGUAGE PLPGSQL;

