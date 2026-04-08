
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
-- Add continuous_aggs_jobs_refresh_ranges table
CREATE TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges (
  materialization_id integer NOT NULL,
  start_range bigint NOT NULL,
  end_range bigint NOT NULL,
  pid integer NOT NULL,
  job_id integer NOT NULL,
  created_at timestamptz NOT NULL,
  CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_jobs_refresh_ranges', '');

CREATE INDEX continuous_aggs_jobs_refresh_ranges_idx ON _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges (materialization_id);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges TO PUBLIC;

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

