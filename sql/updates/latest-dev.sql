-- Add the missing NOT NULL constraint, which also propagates it to the chunks.
DO $$
DECLARE
  mat_ht regclass;
  colname name;
BEGIN
  FOR mat_ht, colname IN
    SELECT a.attrelid::regclass, a.attname
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid = pg_catalog.format('%I.%I', h.schema_name, h.table_name)::regclass
    WHERE pg_catalog.current_setting('server_version_num')::int >= 180000
      AND a.attnum > 0
      AND a.attnotnull
      AND NOT EXISTS (
        SELECT FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = a.attrelid AND c.contype = 'n' AND c.conkey = ARRAY[a.attnum])
  LOOP
    -- The column cannot contain NULLs, so add the constraint as NOT VALID
    -- to avoid scanning the chunks and mark it as valid in the catalog.
    EXECUTE pg_catalog.format('ALTER TABLE %s ADD NOT NULL %I NOT VALID', mat_ht, colname);

    UPDATE pg_catalog.pg_constraint SET convalidated = true
    WHERE contype = 'n'
      AND NOT convalidated
      AND (conrelid = mat_ht OR conrelid IN (
        SELECT inhrelid FROM pg_catalog.pg_inherits WHERE inhparent = mat_ht));
  END LOOP;
END
$$;

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 9, 1, 'COMPRESSION_ALGORITHM_EXTERNAL', 'external');

-- Registry access method for EXTERNAL compression codec operator classes.
-- Placeholder is replaced when the function definitions run later in the update.
CREATE FUNCTION _timescaledb_functions.compression_codec_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;
CREATE ACCESS METHOD ts_compression_codec TYPE INDEX HANDLER _timescaledb_functions.compression_codec_handler;
COMMENT ON ACCESS METHOD ts_compression_codec IS 'Registry for compression codec operator classes';

-- Rebuild compression settings to add the per-column setting for the
-- EXTERNAL compression algorithm (timescaledb.compress_column_codec)
CREATE TABLE _timescaledb_catalog.tempsettings AS SELECT * FROM _timescaledb_catalog.compression_settings;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
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
  codec_column text[],
  codec_opclass text[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst)),
  CONSTRAINT compression_settings_check_codec_null CHECK ((codec_column IS NULL AND codec_opclass IS NULL) OR (codec_column IS NOT NULL AND codec_opclass IS NOT NULL)),
  CONSTRAINT compression_settings_check_codec_cardinality CHECK (array_ndims(codec_column) = 1 AND array_ndims(codec_opclass) = 1 AND cardinality(codec_column) = cardinality(codec_opclass))
);

INSERT INTO _timescaledb_catalog.compression_settings
SELECT
    cs.relid,
    cs.compress_relid,
    cs.segmentby,
    cs.orderby,
    cs.orderby_desc,
    cs.orderby_nullsfirst,
    cs.index
FROM
    _timescaledb_catalog.tempsettings cs;

DROP TABLE _timescaledb_catalog.tempsettings;

CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');
