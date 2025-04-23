CREATE FUNCTION _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data)
    RETURNS BOOL
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 5, 1, 'COMPRESSION_ALGORITHM_BOOL', 'bool'),
( 6, 1, 'COMPRESSION_ALGORITHM_NULL', 'null')
;

-------------------------------
-- Update compression settings
-------------------------------
CREATE TABLE _timescaledb_catalog.tempsettings (LIKE _timescaledb_catalog.compression_settings);
INSERT INTO _timescaledb_catalog.tempsettings SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings CASCADE;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  compress_relid regclass NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

-- Insert updated settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
    CASE
        WHEN h.schema_name IS NOT NULL THEN
            cs.relid
        ELSE
            format('%I.%I', ch.schema_name, ch.table_name)::regclass
    END AS relid,
    CASE
        WHEN h.schema_name IS NOT NULL THEN
            NULL
        ELSE
            cs.relid
    END AS compress_relid,
    cs.segmentby,
    cs.orderby,
    cs.orderby_desc,
    cs.orderby_nullsfirst
FROM
    _timescaledb_catalog.tempsettings cs
INNER JOIN
    pg_class c ON (cs.relid = c.oid)
INNER JOIN
    pg_namespace ns ON (ns.oid = c.relnamespace)
LEFT JOIN
    _timescaledb_catalog.hypertable h ON (h.schema_name = ns.nspname AND h.table_name = c.relname)
LEFT JOIN
    _timescaledb_catalog.chunk cch ON (cch.schema_name = ns.nspname AND cch.table_name = c.relname)
LEFT JOIN
    _timescaledb_catalog.chunk ch ON (cch.id = ch.compressed_chunk_id);

-- Add index on secondary compressed relid key
CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

DROP TABLE _timescaledb_catalog.tempsettings CASCADE;
GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');


-- New add_continuous_aggregate_policy API for incremental refresh policy
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL
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
    max_batches_per_execution INTEGER = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;