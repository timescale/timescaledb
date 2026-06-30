
DROP PROCEDURE IF EXISTS _timescaledb_functions.repair_relation_acls();
DROP FUNCTION IF EXISTS _timescaledb_functions.makeaclitem(regrole, regrole, text, bool);

-- Create watermark record when required. This uses pure SQL to avoid calling
-- C functions that need catalog access during ALTER EXTENSION UPDATE.
-- this is only needed for users upgrading from before 2.11.0, as the watermark
-- was added in that version.
DO
$$
DECLARE
  ts_version TEXT;
  cagg_rec RECORD;
  max_val BIGINT;
  watermark_val BIGINT;
  bucket_width_val BIGINT;
BEGIN
    SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';
    IF ts_version < '2.11.0' THEN
      RETURN;
    END IF;

    FOR cagg_rec IN
      SELECT a.mat_hypertable_id,
             h.schema_name, h.table_name,
             d.column_name, d.column_type,
             bf.bucket_width, bf.bucket_fixed_width
      FROM _timescaledb_catalog.continuous_agg a
      LEFT JOIN _timescaledb_catalog.continuous_aggs_watermark w ON w.mat_hypertable_id = a.mat_hypertable_id
      JOIN _timescaledb_catalog.hypertable h ON h.id = a.mat_hypertable_id
      JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = a.mat_hypertable_id AND d.num_slices IS NULL
      LEFT JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf ON bf.mat_hypertable_id = a.mat_hypertable_id
      WHERE w.mat_hypertable_id IS NULL
      ORDER BY a.mat_hypertable_id
    LOOP
      -- Get max value from materialization hypertable converted to internal representation
      IF cagg_rec.column_type IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
        EXECUTE format(
          'SELECT (pg_catalog.date_part(''epoch'', pg_catalog.max(%I)) * 1000000)::bigint FROM %I.%I',
          cagg_rec.column_name, cagg_rec.schema_name, cagg_rec.table_name
        ) INTO max_val;
      ELSE
        EXECUTE format(
          'SELECT pg_catalog.max(%I)::bigint FROM %I.%I',
          cagg_rec.column_name, cagg_rec.schema_name, cagg_rec.table_name
        ) INTO max_val;
      END IF;

      IF max_val IS NULL OR cagg_rec.bucket_width IS NULL OR NOT cagg_rec.bucket_fixed_width THEN
        -- No data, no bucket function info, or variable-width bucket: use minimum value.
        -- The next cagg refresh will compute the correct watermark.
        watermark_val := '-9223372036854775808'::bigint;
      ELSE
        -- Fixed-width bucket: watermark is max value + bucket width
        IF cagg_rec.column_type IN ('timestamptz'::regtype, 'timestamp'::regtype, 'date'::regtype) THEN
          bucket_width_val := (pg_catalog.date_part('epoch', cagg_rec.bucket_width::interval) * 1000000)::bigint;
        ELSE
          bucket_width_val := cagg_rec.bucket_width::bigint;
        END IF;
        watermark_val := max_val + bucket_width_val;
      END IF;

      INSERT INTO _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark)
      VALUES (cagg_rec.mat_hypertable_id, watermark_val);
    END LOOP;
END;
$$;

-- Cleanup orphaned compression settings
WITH orphaned_settings AS (
     SELECT cs.relid, cl.relname
     FROM _timescaledb_catalog.compression_settings cs
     LEFT JOIN pg_class cl ON (cs.relid = cl.oid)
     WHERE cl.relname IS NULL
)
DELETE FROM _timescaledb_catalog.compression_settings AS cs
USING orphaned_settings AS os WHERE cs.relid = os.relid;

-- Remove self-referential foreign keys to eliminate pg_dump circular dependency warnings
ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT IF EXISTS hypertable_compressed_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk DROP CONSTRAINT IF EXISTS chunk_compressed_chunk_id_fkey;


-- Bloom filters on smallint columns are incompatible with this version because
-- the int2 hash changed, so the bloom data must be dropped. Drop it only when
-- every such bloom was created automatically (source is not 'config'); if any
-- was configured explicitly, block the upgrade with manual instructions instead.
DO $$
DECLARE
  drop_commands text;
  has_explicit boolean;
BEGIN
  -- Find the bloom metadata columns of int2 blooms on compressed chunks. The
  -- column is named after the bloom's columns joined by '_'; the prefix depends
  -- on the build that wrote it (bloom1 for old builds, bloomh/bloomg for current
  -- ones), and names longer than 39 chars are truncated with a 4 char md5 hash.
  WITH int2_blooms AS (
    SELECT cs.compress_relid,
           COALESCE(elem->>'source', 'default') AS source,
           (SELECT string_agg(col, '_' ORDER BY ord)
            FROM jsonb_array_elements_text(cols) WITH ORDINALITY AS t(col, ord)) AS suffix
    FROM _timescaledb_catalog.compression_settings cs,
         jsonb_array_elements(cs.index) AS elem,
         LATERAL (SELECT CASE jsonb_typeof(elem->'column')
                           WHEN 'array' THEN elem->'column'
                           ELSE jsonb_build_array(elem->'column')
                         END) AS n(cols)
    WHERE elem->>'type' = 'bloom'
      AND cs.compress_relid IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(cols) AS colname
        JOIN pg_attribute a ON a.attrelid = cs.relid AND a.attname = colname
         AND a.atttypid = 'int2'::regtype AND a.attnum > 0
      )
  ),
  drop_cmds AS (
    SELECT format('ALTER TABLE %s DROP COLUMN %I;', b.compress_relid::regclass, a.attname) AS cmd,
           b.source
    FROM int2_blooms b
    CROSS JOIN LATERAL (
      SELECT CASE WHEN length(b.suffix) > 39
                  THEN substr(md5(b.suffix), 1, 4) || '_' || substr(b.suffix, 1, 39)
                  ELSE b.suffix END AS body
    ) m
    JOIN pg_attribute a ON a.attrelid = b.compress_relid AND a.attnum > 0
     AND a.attname IN ('_ts_meta_v2_bloom1_' || m.body,
                       '_ts_meta_v2_bloomh_' || m.body,
                       '_ts_meta_v2_bloomg_' || m.body)
  )
  SELECT string_agg(DISTINCT cmd, E'\n' ORDER BY cmd), bool_or(source = 'config')
  INTO drop_commands, has_explicit
  FROM drop_cmds;

  IF drop_commands IS NULL THEN
    -- Nothing to migrate.
  ELSIF has_explicit THEN
    -- Not all can be dropped automatically, so drop none and block the upgrade.
    RAISE EXCEPTION
      'existing bloom filter sparse indexes on smallint columns are incompatible '
      'with this version of TimescaleDB'
      USING
        DETAIL = E'These indexes must be dropped before upgrading. To do so, run the following commands:\n\n'
                 || E'SET timescaledb.restoring = on;\n'
                 || drop_commands || E'\n'
                 || 'SET timescaledb.restoring = off;',
        HINT = 'To rebuild the bloom filter indexes after upgrading, decompress and compress the affected chunks.';
  ELSE
    EXECUTE drop_commands;
  END IF;
END
$$;

-- Drop the same blooms from the compression settings so they no longer
-- reference the removed columns, keeping explicitly configured entries.
WITH rebuilt AS (
  SELECT cs.relid,
         jsonb_agg(elem ORDER BY ord) FILTER (WHERE keep) AS new_index,
         bool_or(NOT keep) AS changed
  FROM _timescaledb_catalog.compression_settings cs,
       jsonb_array_elements(cs.index) WITH ORDINALITY AS arr(elem, ord),
       LATERAL (SELECT NOT (
         elem->>'type' = 'bloom'
         AND COALESCE(elem->>'source', 'default') <> 'config'
         AND EXISTS (
           SELECT 1
           FROM jsonb_array_elements_text(CASE jsonb_typeof(elem->'column')
                                            WHEN 'array' THEN elem->'column'
                                            ELSE jsonb_build_array(elem->'column')
                                          END) AS colname
           JOIN pg_attribute a ON a.attrelid = cs.relid AND a.attname = colname
            AND a.atttypid = 'int2'::regtype AND a.attnum > 0
         ))) AS k(keep)
  GROUP BY cs.relid
)
UPDATE _timescaledb_catalog.compression_settings cs
SET index = rebuilt.new_index
FROM rebuilt
WHERE cs.relid = rebuilt.relid
  AND rebuilt.changed;


DROP FUNCTION IF EXISTS _timescaledb_functions.job_history_bsearch;

DROP FUNCTION IF EXISTS _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
DROP PROCEDURE IF EXISTS @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP PROCEDURE IF EXISTS @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);

-- Return type widened from INTEGER to BIGINT; per-batch byte count can
-- exceed INT32_MAX for wide varlena columns and was silently wrapping.
DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_column_size(_timescaledb_internal.compressed_data, ANYELEMENT);

-- Migration: refresh orderby sparse index entries in compression_settings
UPDATE _timescaledb_catalog.compression_settings
SET index = (
    SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
    FROM jsonb_array_elements(index) AS elem
    WHERE elem->>'source' != 'orderby'
)
WHERE index IS NOT NULL
AND index @> '[{"source": "orderby"}]';

UPDATE _timescaledb_catalog.compression_settings cs
SET index = COALESCE(index, '[]'::jsonb) ||
            (
            SELECT jsonb_agg(jsonb_build_object(
                                'type', 'minmax',
                                'source', 'orderby',
                                'column', elem))
            FROM unnest(cs.orderby) AS elem
            )
WHERE cs.orderby IS NOT NULL;

