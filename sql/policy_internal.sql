-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_retention(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_retention_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_reorder(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_recompression(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_recompression_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_refresh_continuous_aggregate(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_proc'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE
_timescaledb_internal.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN)
AS $$
DECLARE
  htoid       REGCLASS;
  chunk_rec   RECORD;
  numchunks   INTEGER := 1;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog;

  SELECT format('%I.%I', schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  -- for the integer cases, we have to compute the lag w.r.t
  -- the integer_now function and then pass on to show_chunks
  IF pg_typeof(lag) IN ('BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype) THEN
    lag := _timescaledb_internal.subtract_integer_from_now(htoid, lag::BIGINT);
  END IF;

  FOR chunk_rec IN
    SELECT
      show.oid, ch.schema_name, ch.table_name, ch.status
    FROM
      @extschema@.show_chunks(htoid, older_than => lag) AS show(oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname AND ch.schema_name = pgns.nspname AND ch.hypertable_id = htid
    WHERE
      ch.dropped IS FALSE
      AND (ch.status = 0 OR ch.status = 3)
  LOOP
    IF chunk_rec.status = 0 THEN
       PERFORM @extschema@.compress_chunk( chunk_rec.oid );
    ELSIF chunk_rec.status = 3 AND recompress_enabled IS TRUE THEN
       PERFORM @extschema@.decompress_chunk(chunk_rec.oid, if_compressed => true);
       COMMIT;
       -- SET LOCAL is only active until end of transaction.
       -- While we could use SET at the start of the function we do not
       -- want to bleed out search_path to caller, so we do SET LOCAL
       -- again after COMMIT
       PERFORM @extschema@.compress_chunk(chunk_rec.oid);
    END IF;
    COMMIT;
    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog;
    IF verbose_log THEN
       RAISE LOG 'job % completed processing chunk %.%', job_id, chunk_rec.schema_name, chunk_rec.table_name;
    END IF;
    numchunks := numchunks + 1;
    IF maxchunks > 0 AND numchunks >= maxchunks THEN
         EXIT;
    END IF;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE
_timescaledb_internal.policy_compression(job_id INTEGER, config JSONB)
AS $$
DECLARE
  dimtype             REGTYPE;
  dimtypeinput        REGPROC;
  compress_after      TEXT;
  lag_value           TEXT;
  htid                INTEGER;
  htoid               REGCLASS;
  chunk_rec           RECORD;
  verbose_log         BOOL;
  maxchunks           INTEGER := 0;
  numchunks           INTEGER := 1;
  recompress_enabled  BOOL;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog;

  IF config IS NULL THEN
    RAISE EXCEPTION 'job % has null config', job_id;
  END IF;

  htid := jsonb_object_field_text(config, 'hypertable_id')::INTEGER;
  IF htid is NULL THEN
    RAISE EXCEPTION 'job % config must have hypertable_id', job_id;
  END IF;

  verbose_log         := COALESCE(jsonb_object_field_text(config, 'verbose_log')::BOOLEAN, FALSE);
  maxchunks           := COALESCE(jsonb_object_field_text(config, 'maxchunks_to_compress')::INTEGER, 0);
  recompress_enabled  := COALESCE(jsonb_object_field_text(config, 'recompress')::BOOLEAN, TRUE);
  compress_after      := jsonb_object_field_text(config, 'compress_after');

  IF compress_after IS NULL THEN
    RAISE EXCEPTION 'job % config must have compress_after', job_id;
  END IF;

  -- find primary dimension type --
  SELECT dim.column_type INTO dimtype
  FROM  _timescaledb_catalog.hypertable ht
        JOIN _timescaledb_catalog.dimension dim ON ht.id = dim.hypertable_id
  WHERE ht.id = htid
  ORDER BY dim.id
  LIMIT 1;

  lag_value := jsonb_object_field_text(config, 'compress_after');

  -- execute the properly type casts for the lag value
  CASE dimtype
    WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype, 'DATE'::regtype THEN
      CALL _timescaledb_internal.policy_compression_execute(
        job_id, htid, lag_value::INTERVAL,
        maxchunks, verbose_log, recompress_enabled
      );
    WHEN 'BIGINT'::regtype THEN
      CALL _timescaledb_internal.policy_compression_execute(
        job_id, htid, lag_value::BIGINT,
        maxchunks, verbose_log, recompress_enabled
      );
    WHEN 'INTEGER'::regtype THEN
      CALL _timescaledb_internal.policy_compression_execute(
        job_id, htid, lag_value::INTEGER,
        maxchunks, verbose_log, recompress_enabled
      );
    WHEN 'SMALLINT'::regtype THEN
      CALL _timescaledb_internal.policy_compression_execute(
        job_id, htid, lag_value::SMALLINT,
        maxchunks, verbose_log, recompress_enabled
      );
  END CASE;
END;
$$ LANGUAGE PLPGSQL;
