-- remove obsolete job
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;

-- Hypercore updates
CREATE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL, initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL);

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

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies(relation REGCLASS, if_not_exists BOOL, refresh_start_offset "any", refresh_end_offset "any", compress_after "any", drop_after "any");

CREATE FUNCTION timescaledb_experimental.add_policies(
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

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(job_id INTEGER, htid INTEGER, lag ANYELEMENT, maxchunks INTEGER, verbose_log BOOLEAN, recompress_enabled  BOOLEAN, use_creation_time BOOLEAN);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);

CREATE PROCEDURE @extschema@.convert_to_columnstore(
    chunk REGCLASS,
    if_not_columnstore BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL)
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C;

CREATE PROCEDURE @extschema@.convert_to_rowstore(
    chunk REGCLASS,
    if_columnstore BOOLEAN = true)
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C;

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

CREATE PROCEDURE @extschema@.remove_columnstore_policy(
       hypertable REGCLASS,
       if_exists BOOL = false
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE FUNCTION @extschema@.chunk_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.chunk_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.hypertable_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.hypertable_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

-- Recreate `refresh_continuous_aggregate` procedure to add `force` argument
DROP PROCEDURE IF EXISTS @extschema@.refresh_continuous_aggregate (continuous_aggregate REGCLASS, window_start "any", window_end "any");

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

-- Add `include_tiered_data` argument to `add_continuous_aggregate_policy`
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT
);
CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
	include_tiered_data BOOL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

-- Merge chunks
CREATE PROCEDURE @extschema@.merge_chunks(
    chunk1 REGCLASS, chunk2 REGCLASS
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE PROCEDURE @extschema@.merge_chunks(
    chunks REGCLASS[]
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

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
