DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 9 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_EXTERNAL';

-- Can't downgrade while external compression codecs are registered. Chunks may
-- still hold batches only their codec can read, and the older version has no
-- EXTERNAL algorithm support.
-- First, clear the timescaledb.compress_column_codec settings that reference
-- the operator class so new compression uses the built-in algorithms. Then
-- rewrite every chunk using the codec with decompress_chunk() followed
-- by compress_chunk(). Then drop the operator class.
DO $$
DECLARE
  reg record;
  clear_cmds text;
  rewrite_cmds text;
  decommission_cmds text := NULL;
  codec_opclasses text := NULL;
BEGIN
  FOR reg IN
    SELECT format('%I.%I', n.nspname, c.opcname) AS opclass,
           c.opcintype::regtype AS type
    FROM pg_catalog.pg_opclass c
    JOIN pg_catalog.pg_am a ON a.oid = c.opcmethod
    JOIN pg_catalog.pg_namespace n ON n.oid = c.opcnamespace
    WHERE a.amname = 'ts_compression_codec'
  LOOP
    codec_opclasses := concat_ws(', ', codec_opclasses, reg.opclass);

    -- Clear the codec settings of every hypertable that references this
    -- operator class. The settings store the name the same way it is quoted
    -- here, so string comparison finds the references.
    SELECT string_agg(format('ALTER TABLE %s SET (timescaledb.compress_column_codec = '''');',
                             format('%I.%I', h.schema_name, h.table_name)), E'\n')
      INTO clear_cmds
    FROM _timescaledb_catalog.hypertable h
    JOIN _timescaledb_catalog.compression_settings cs
      ON cs.relid = format('%I.%I', h.schema_name, h.table_name)::regclass
    WHERE reg.opclass = ANY (cs.codec_opclass);

    -- For (comparative) simplicity, just show instructions for rewriting every
    -- possibly-affected chunk having a compressed column with this registered
    -- codec.
    SELECT string_agg(format(E'SELECT decompress_chunk(ch, true) FROM show_chunks(%L) ch;\n'
                             'SELECT compress_chunk(ch) FROM show_chunks(%L) ch;', ht, ht), E'\n')
      INTO rewrite_cmds
    FROM (
      SELECT DISTINCT format('%I.%I', h.schema_name, h.table_name)::regclass AS ht
      FROM _timescaledb_catalog.hypertable h
      JOIN pg_catalog.pg_attribute att
        ON att.attrelid = format('%I.%I', h.schema_name, h.table_name)::regclass
      WHERE att.atttypid = reg.type AND att.attnum > 0 AND NOT att.attisdropped
    ) affected;

    decommission_cmds := concat_ws(E'\n', decommission_cmds, format(
      E'-- decommission codec %1$s for type %2$s\n'
      '%3$s'
      '%4$s'
      'DROP OPERATOR CLASS %1$s USING ts_compression_codec;',
      reg.opclass,
      reg.type,
      coalesce(clear_cmds || E'\n', ''),
      coalesce(rewrite_cmds || E'\n', '')));
  END LOOP;

  IF codec_opclasses IS NOT NULL THEN
    RAISE EXCEPTION 'cannot downgrade while compression codec operator classes exist: %', codec_opclasses
      USING DETAIL = 'Chunks compressed while a codec is registered may hold EXTERNAL batches that the older version cannot read.',
            HINT = format(E'Run the following, then retry the downgrade:\n%s', decommission_cmds);
  END IF;
END
$$;
DROP ACCESS METHOD IF EXISTS ts_compression_codec;
DROP FUNCTION IF EXISTS _timescaledb_functions.compression_codec_handler(internal);

-- Rebuild compression settings without the per-column external codec setting
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
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
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
