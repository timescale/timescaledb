
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


-- Block upgrade if bloom filter sparse indexes exist on smallint (int2)
-- columns. These bloom filters used PostgreSQL's hashint2extended while
-- the new code uses bloom1_hash_2. Existing bloom data must be dropped
-- before upgrading; recompress afterwards to rebuild with the new hash.
DO $$
DECLARE
  drop_commands text;
BEGIN
  WITH bloom_entries AS (
    SELECT relid AS chunk_oid,
           compress_relid,
           columns,
           (SELECT string_agg(col, '_' ORDER BY ordinality)
            FROM jsonb_array_elements_text(columns)
                 WITH ORDINALITY AS t(col, ordinality)) AS col_suffix
    FROM _timescaledb_catalog.compression_settings,
         jsonb_array_elements(index) AS elem,
         LATERAL (SELECT
           CASE jsonb_typeof(elem->'column')
             WHEN 'array' THEN elem->'column'
             ELSE jsonb_build_array(elem->'column')
           END AS columns
         ) AS normalized
    WHERE elem->>'type' = 'bloom'
      AND compress_relid IS NOT NULL
  ),
  bloom_column_names AS (
    SELECT chunk_oid, compress_relid, col_suffix, colname
    FROM bloom_entries,
         jsonb_array_elements_text(columns) AS bloom_column(colname)
  ),
  int2_bloom_suffixes AS (
    SELECT DISTINCT compress_relid, col_suffix
    FROM bloom_column_names
    JOIN pg_attribute ON attrelid = chunk_oid
     AND attname = colname
     AND atttypid = 'int2'::regtype
     AND attnum > 0
  ),
  bloom_cols_to_drop AS (
    SELECT compress_relid,
           attname AS bloom_attname
    FROM int2_bloom_suffixes
    JOIN pg_attribute ON attrelid = compress_relid
     AND attname IN (
       '_ts_meta_v2_bloom1_' || col_suffix,
       '_ts_meta_v2_bloomh_' || col_suffix,
       '_ts_meta_v2_bloomg_' || col_suffix
     )
     AND attnum > 0
  )
  SELECT string_agg(DISTINCT
           format('ALTER TABLE %s DROP COLUMN %I;',
                  compress_relid::regclass, bloom_attname),
           E'\n' ORDER BY
           format('ALTER TABLE %s DROP COLUMN %I;',
                  compress_relid::regclass, bloom_attname))
  INTO drop_commands
  FROM bloom_cols_to_drop;

  IF drop_commands IS NOT NULL THEN
    RAISE EXCEPTION
      'existing bloom filter sparse indexes on smallint columns are incompatible '
      'with this version of TimescaleDB'
      USING
        DETAIL = E'These indexes must be dropped before upgrading. To do so, run the following commands:\n\n'
                 || E'SET timescaledb.restoring = on;\n'
                 || drop_commands || E'\n'
                 || 'SET timescaledb.restoring = off;',
        HINT = 'To rebuild the bloom filter indexes after upgrading, decompress and compress the affected chunks.';
  END IF;
END
$$;


DROP FUNCTION IF EXISTS _timescaledb_functions.job_history_bsearch;

DROP FUNCTION IF EXISTS _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
DROP PROCEDURE IF EXISTS @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP PROCEDURE IF EXISTS @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);

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

