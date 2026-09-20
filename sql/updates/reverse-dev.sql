DROP FUNCTION IF EXISTS _timescaledb_functions.move_to_columnstore(REGCLASS, BOOLEAN);

DROP VIEW IF EXISTS timescaledb_information.hypertable_granular_refresh_settings;

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

-- Block the downgrade if any compressed chunk holds a batch compressed with
-- the adaptive integer compression (AIC, algorithm id 8): the previous
-- version cannot read such batches.
--
-- compressed_data_info() cannot be used here. It lives in the current
-- version's shared library, which is not loaded while ALTER EXTENSION runs,
-- and loading it now would fail. Instead we read the algorithm id directly
-- through the 'compressed_data_prefix' helper function.
--
-- AIC cannot be enabled per column, so all integer-like columns of a batch
-- share one algorithm. Per batch we therefore look at the first integer-like
-- column that holds values.

CREATE FUNCTION _timescaledb_functions.compressed_data_prefix(_timescaledb_internal.compressed_data, int4, int4)
  RETURNS bytea AS 'bytea_substr' LANGUAGE INTERNAL STRICT IMMUTABLE;

DO $$
DECLARE
  chunk_rec RECORD;
  dim_column NAME;
  probe_expr TEXT;
  found BOOLEAN;
BEGIN
  FOR chunk_rec IN
    SELECT ht.id AS hypertable_id,
           ch.relid AS chunk,
           cs.compress_relid AS compressed_chunk
      FROM _timescaledb_catalog.hypertable ht
      JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
      JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = ch.relid
     WHERE cs.compress_relid IS NOT NULL
       AND NOT ch.osm_chunk
     ORDER BY ht.id, ch.id
  LOOP
    SELECT d.column_name INTO dim_column
      FROM _timescaledb_catalog.dimension d
     WHERE d.hypertable_id = chunk_rec.hypertable_id
     ORDER BY d.id
     LIMIT 1;

    SELECT string_agg(
             format($f$nullif(_timescaledb_functions.compressed_data_prefix(x.%I, 1, 1), decode('06', 'hex'))$f$, c.attname),
             ', ' ORDER BY (c.attname = dim_column) DESC, u.attnotnull DESC, c.attnum)
      INTO probe_expr
      FROM pg_catalog.pg_attribute c
      JOIN pg_catalog.pg_attribute u
        ON u.attrelid = chunk_rec.chunk AND u.attname = c.attname AND NOT u.attisdropped
     WHERE c.attrelid = chunk_rec.compressed_chunk
       AND c.attnum > 0
       AND NOT c.attisdropped
       AND c.atttypid = '_timescaledb_internal.compressed_data'::regtype
       AND u.atttypid = ANY (ARRAY['int2', 'int4', 'int8', 'date', 'timestamp', 'timestamptz']::regtype[]);

    IF probe_expr IS NULL THEN
      CONTINUE; -- no integer-like compressed column, the chunk cannot hold AIC
    END IF;

    EXECUTE format($f$SELECT EXISTS (SELECT 1 FROM %s x WHERE coalesce(%s) = decode('08', 'hex'))$f$,
                   chunk_rec.compressed_chunk, probe_expr)
       INTO found;

    IF found THEN
      RAISE EXCEPTION 'cannot downgrade because chunk % holds data compressed with adaptive integer compression (AIC)', chunk_rec.chunk
        USING
          ERRCODE = 'object_not_in_prerequisite_state',
          DETAIL = 'The previous TimescaleDB version cannot read such data. Other chunks may be affected as well.',
          HINT = 'Run https://github.com/timescale/timescaledb-extras/blob/main/utils/2.31.0-downgrade_aic_compression.sql on the current version to find and recompress all affected chunks, then retry the downgrade.';
    END IF;
  END LOOP;
END
$$;

DROP FUNCTION _timescaledb_functions.compressed_data_prefix(_timescaledb_internal.compressed_data, int4, int4);

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 8 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_AIC';

