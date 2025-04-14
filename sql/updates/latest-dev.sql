-- Type for bloom filters used by the sparse indexes on compressed hypertables.
CREATE TYPE _timescaledb_internal.bloom1;

CREATE FUNCTION _timescaledb_functions.bloom1in(cstring) RETURNS _timescaledb_internal.bloom1 AS 'byteain' LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_functions.bloom1out(_timescaledb_internal.bloom1) RETURNS cstring AS 'byteaout' LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE;

CREATE TYPE _timescaledb_internal.bloom1 (
    INPUT = _timescaledb_functions.bloom1in,
    OUTPUT = _timescaledb_functions.bloom1out,
    LIKE = bytea
);

CREATE FUNCTION _timescaledb_functions.ts_bloom1_matches(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE OPERATOR _timescaledb_internal.@> (
    FUNCTION = _timescaledb_functions.ts_bloom1_matches,
    LEFTARG = _timescaledb_internal.bloom1,
    RIGHTARG = anyelement
);


DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_table;

-- New option `refresh_newest_first` for incremental cagg refresh policy
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL,
    buckets_per_batch INTEGER,
    max_batches_per_execution INTEGER
);

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    include_tiered_data BOOL = NULL,
    buckets_per_batch INTEGER = NULL,
    max_batches_per_execution INTEGER = NULL,
    refresh_newest_first BOOL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_functions' WHERE chunk_sizing_func_schema = '_timescaledb_internal' AND chunk_sizing_func_name = 'calculate_chunk_interval';

