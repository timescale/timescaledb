
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

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 7, 1, 'COMPRESSION_ALGORITHM_UUID', 'uuid');

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_index_clone(oid);
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_index_clone(oid);
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_index_replace(oid,oid);
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_index_replace(oid,oid);

-- Update compression settings
CREATE TABLE _timescaledb_catalog.tempsettings (LIKE _timescaledb_catalog.compression_settings);
INSERT INTO _timescaledb_catalog.tempsettings SELECT * FROM _timescaledb_catalog.compression_settings;
DROP VIEW IF EXISTS timescaledb_information.hypertable_columnstore_settings;
DROP VIEW IF EXISTS timescaledb_information.chunk_columnstore_settings;
DROP VIEW IF EXISTS timescaledb_information.hypertable_compression_settings;
DROP VIEW IF EXISTS timescaledb_information.chunk_compression_settings;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  compress_relid regclass NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  index jsonb,
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

-- Insert updated settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
    cs.relid,
    cs.compress_relid,
    cs.segmentby,
    cs.orderby,
    cs.orderby_desc,
    cs.orderby_nullsfirst
FROM
    _timescaledb_catalog.tempsettings cs;

-- Add index on secondary compressed relid key
CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

DROP TABLE _timescaledb_catalog.tempsettings;
GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');

CREATE FUNCTION _timescaledb_functions.jsonb_get_matching_index_entry(
    config      jsonb,
    attr_name   text,
    target_type text
)
RETURNS jsonb
AS $$
BEGIN
  --empty body
  RETURN NULL;
END;
$$ LANGUAGE PLPGSQL
SET search_path TO pg_catalog, pg_temp;

--migration script
CREATE FUNCTION process_single_table_index_migrate(
    IN target_schema TEXT,
    IN target_table TEXT
)
RETURNS jsonb
AS $$
DECLARE
    col_name TEXT;
    json_entries jsonb := '[]';
    type_part TEXT;
    sparse_type TEXT;
    col_part TEXT;
BEGIN
    -- Step 1: Loop over relevant column names
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = target_table
          AND table_schema = target_schema
          AND column_name LIKE '_ts_meta_v2_%'
    LOOP
        -- Step 2: Parse column name: _ts_meta_v2_<type>_<col>
        col_name := regexp_replace(col_name, '^_ts_meta_v2_', '');

        type_part := split_part(col_name, '_', 1);
        col_part := substring(col_name FROM length(type_part) + 2);  -- skip type and underscore

        IF type_part = 'bloom1' THEN
            sparse_type := 'bloom';
        ELSIF type_part = 'min' THEN
            sparse_type := 'minmax';
        ELSE
            CONTINUE;
        END IF;

        json_entries := json_entries || jsonb_build_object('type', sparse_type, 'source', 'default', 'column', col_part);
    END LOOP;

    RETURN json_entries;
END;
$$ LANGUAGE PLPGSQL
SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
    rec RECORD;
    schema_name TEXT;
    table_name TEXT;
    json_entries JSONB;
    rows_updated INT;
BEGIN
    FOR rec IN
        SELECT relid, compress_relid
        FROM _timescaledb_catalog.compression_settings
        WHERE index IS NULL
    LOOP
        IF rec.compress_relid IS NULL THEN
            CONTINUE;
        ELSE
            -- Get schema and table name from compress_relid
            SELECT n.nspname, c.relname
            INTO schema_name, table_name
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = rec.compress_relid;
        END IF;

        -- Call procedure for that table
        json_entries := process_single_table_index_migrate(schema_name, table_name);

        IF jsonb_array_length(json_entries) > 0 THEN
            UPDATE _timescaledb_catalog.compression_settings
            SET index = json_entries
            WHERE relid = rec.relid AND index IS NULL;
        END IF;
    END LOOP;
END $$;

DROP FUNCTION process_single_table_index_migrate(text, text);

-- add orderby minmax columns to sparse index settings
UPDATE _timescaledb_catalog.compression_settings cs
SET index = COALESCE(index, '[]'::jsonb) ||
            (
            SELECT jsonb_agg(jsonb_build_object(
                                'type', 'minmax',
                                'source', 'orderby',
                                'column', elem))
            FROM unnest(cs.orderby) AS elem
            )
WHERE cs.orderby IS NOT NULL AND cardinality(cs.orderby) > 0;

DROP FUNCTION IF EXISTS _timescaledb_internal.indexes_local_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.indexes_local_size;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_index;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_index;

