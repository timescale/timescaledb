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

-- Per-column setting for EXTERNAL compression algorithm. Set via the
-- timescaledb.compress_column_codec option. codec_column[i] is compressed
-- with the codec operator class named by codec_opclass[i].
ALTER TABLE _timescaledb_catalog.compression_settings
  ADD COLUMN codec_column text[],
  ADD COLUMN codec_opclass text[],
  ADD CONSTRAINT compression_settings_check_codec_null
    CHECK ((codec_column IS NULL AND codec_opclass IS NULL) OR (codec_column IS NOT NULL AND codec_opclass IS NOT NULL)),
  ADD CONSTRAINT compression_settings_check_codec_cardinality
    CHECK (array_ndims(codec_column) = 1 AND array_ndims(codec_opclass) = 1 AND cardinality(codec_column) = cardinality(codec_opclass));
