ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.continuous_aggregates;

DROP VIEW timescaledb_information.continuous_aggregates;

DROP FUNCTION _timescaledb_functions.cagg_parse_invalidation_record(BYTEA);
DROP FUNCTION _timescaledb_functions.has_invalidation_trigger(regclass);

CREATE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_handler' LANGUAGE C;

CREATE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_proxy_handler' LANGUAGE C;

CREATE ACCESS METHOD hypercore TYPE TABLE HANDLER ts_hypercore_handler;
COMMENT ON ACCESS METHOD hypercore IS 'Storage engine using hybrid row/columnar compression';

CREATE ACCESS METHOD hypercore_proxy TYPE INDEX HANDLER ts_hypercore_proxy_handler;
COMMENT ON ACCESS METHOD hypercore_proxy IS 'Hypercore proxy index access method';

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING hypercore_proxy AS
       OPERATOR 1 = (int4, int4),
       FUNCTION 1 hashint4(int4);

CREATE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STRICT;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute;
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy;
DROP PROCEDURE IF EXISTS @extschema@.add_columnstore_policy;
DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies;
DROP FUNCTION IF EXISTS @extschema@.compress_chunk;
DROP PROCEDURE IF EXISTS @extschema@.convert_to_columnstore;

CREATE FUNCTION @extschema@.compress_chunk(
  uncompressed_chunk REGCLASS,
  if_not_compressed BOOLEAN = true,
  recompress BOOLEAN = false,
  hypercore_use_access_method BOOL = NULL
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

CREATE PROCEDURE @extschema@.convert_to_columnstore(
  chunk REGCLASS,
  if_not_columnstore BOOLEAN = true,
  recompress BOOLEAN = false,
  hypercore_use_access_method BOOL = NULL
) AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
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
    created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE OR REPLACE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL,
    hypercore_use_access_method BOOL = NULL)
RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

CREATE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  reindex_enabled     BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$ BEGIN END $$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS _timescaledb_functions.generate_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_v7_from_timestamptz;
DROP FUNCTION IF EXISTS _timescaledb_functions.timestamptz_from_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_version;
