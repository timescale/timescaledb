-- Hypercore AM
DROP ACCESS METHOD IF EXISTS hypercore_proxy;
DROP FUNCTION IF EXISTS ts_hypercore_proxy_handler;
DROP ACCESS METHOD IF EXISTS hypercore;
DROP FUNCTION IF EXISTS ts_hypercore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN, hypercore_use_access_method BOOL);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL, initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL, hypercore_use_access_method BOOL);

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
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies(relation REGCLASS, if_not_exists BOOL, refresh_start_offset "any", refresh_end_offset "any", compress_after "any", drop_after "any", hypercore_use_access_method BOOL);

CREATE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL)
RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policies_add'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(job_id INTEGER, htid INTEGER, lag ANYELEMENT, maxchunks INTEGER, verbose_log BOOLEAN, recompress_enabled  BOOLEAN, use_creation_time BOOLEAN, useam BOOLEAN);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);
DROP PROCEDURE IF EXISTS @extschema@.convert_to_columnstore(REGCLASS, BOOLEAN, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS @extschema@.convert_to_rowstore(REGCLASS, BOOLEAN);
DROP PROCEDURE IF EXISTS @extschema@.add_columnstore_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT, INTERVAL, BOOL);
DROP PROCEDURE IF EXISTS @extschema@.remove_columnstore_policy(REGCLASS, BOOL);
DROP FUNCTION IF EXISTS @extschema@.hypertable_columnstore_stats(REGCLASS);
DROP FUNCTION IF EXISTS @extschema@.chunk_columnstore_stats(REGCLASS);

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.hypertable_columnstore_settings;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.chunk_columnstore_settings;

DROP VIEW timescaledb_information.hypertable_columnstore_settings;
DROP VIEW timescaledb_information.chunk_columnstore_settings;

DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_update_watermark(INTEGER);

-- Recreate `refresh_continuous_aggregate` procedure to remove the `force` argument
DROP PROCEDURE IF EXISTS @extschema@.refresh_continuous_aggregate (continuous_aggregate REGCLASS, window_start "any", window_end "any", force BOOLEAN);

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any"
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh';

-- Remove `include_tiered_data` argument from `add_continuous_aggregate_policy`
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL
);
CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_add'
LANGUAGE C VOLATILE;

-- Merge chunks
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(chunk1 REGCLASS, chunk2 REGCLASS);
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(chunks REGCLASS[]);
