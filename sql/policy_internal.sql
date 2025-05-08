-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_retention(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_retention_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_retention_check(config JSONB)
RETURNS void AS '@MODULE_PATHNAME@', 'ts_policy_retention_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_reorder(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_reorder_check(config JSONB)
RETURNS void AS '@MODULE_PATHNAME@', 'ts_policy_reorder_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_recompression(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_recompression_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_compression_check(config JSONB)
RETURNS void AS '@MODULE_PATHNAME@', 'ts_policy_compression_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_refresh_continuous_aggregate(job_id INTEGER, config JSONB)
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_refresh_continuous_aggregate_check(config JSONB)
RETURNS void AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$
DECLARE
  htoid       REGCLASS;
  chunk_rec   RECORD;
  numchunks_compressed   INTEGER := 0;
  _message     text;
  _detail      text;
  _sqlstate    text;
  -- fully compressed chunk status
  status_fully_compressed int := 1;
  -- chunk status bits:
  bit_compressed int := 1;
  bit_compressed_unordered int := 2;
  bit_frozen int := 4;
  bit_compressed_partial int := 8;
  creation_lag INTERVAL := NULL;
  chunks_failure INTEGER := 0;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog, pg_temp;

  SELECT format('%I.%I', schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  -- for the integer cases, we have to compute the lag w.r.t
  -- the integer_now function and then pass on to show_chunks
  IF pg_typeof(lag) IN ('BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype) THEN
    -- cannot have use_creation_time set with this
    IF use_creation_time IS TRUE THEN
        RAISE EXCEPTION 'job % cannot use creation time with integer_now function', job_id;
    END IF;
    lag := _timescaledb_functions.subtract_integer_from_now(htoid, lag::BIGINT);
  END IF;

  -- if use_creation_time has been specified then the lag needs to be used with the
  -- "compress_created_before" argument. Otherwise the usual "older_than" argument
  -- is good enough
  IF use_creation_time IS TRUE THEN
    creation_lag := lag;
    lag := NULL;
  END IF;

  FOR chunk_rec IN
    SELECT
      show.oid, ch.schema_name, ch.table_name, ch.status
    FROM
      @extschema@.show_chunks(htoid, older_than => lag, created_before => creation_lag) AS show(oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname AND ch.schema_name = pgns.nspname AND ch.hypertable_id = htid
    WHERE NOT ch.dropped
    AND NOT ch.osm_chunk
    -- Checking for chunks which are not fully compressed and not frozen
    AND ch.status != status_fully_compressed
    AND ch.status & bit_frozen = 0
  LOOP
    BEGIN
      IF chunk_rec.status = bit_compressed OR recompress_enabled IS TRUE THEN
        PERFORM @extschema@.compress_chunk(chunk_rec.oid, hypercore_use_access_method => useam);
        numchunks_compressed := numchunks_compressed + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
          _message = MESSAGE_TEXT,
          _detail = PG_EXCEPTION_DETAIL,
          _sqlstate = RETURNED_SQLSTATE;
      RAISE WARNING 'converting chunk "%" to columnstore failed when columnstore policy is executed', chunk_rec.oid::regclass::text
          USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                ERRCODE = _sqlstate;
      chunks_failure := chunks_failure + 1;
    END;
    COMMIT;
    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog, pg_temp;
    IF verbose_log THEN
       RAISE LOG 'job % completed processing chunk %.%', job_id, chunk_rec.schema_name, chunk_rec.table_name;
    END IF;
    IF maxchunks > 0 AND numchunks_compressed >= maxchunks THEN
         EXIT;
    END IF;
  END LOOP;

  IF chunks_failure > 0 THEN
    RAISE EXCEPTION 'columnstore policy failure'
      USING
        DETAIL = format('Failed to convert %L chunks to columnstore. Successfully converted %L chunks.', chunks_failure, numchunks_compressed),
        ERRCODE = 'data_exception';
  END IF;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE
_timescaledb_functions.policy_compression(job_id INTEGER, config JSONB)
AS $$
DECLARE
  dimtype             REGTYPE;
  dimtypeinput        REGPROC;
  compress_after      TEXT;
  compress_created_before TEXT;
  lag_value           TEXT;
  lag_bigint_value    BIGINT;
  htid                INTEGER;
  htoid               REGCLASS;
  chunk_rec           RECORD;
  verbose_log         BOOL;
  maxchunks           INTEGER := 0;
  numchunks           INTEGER := 1;
  recompress_enabled  BOOL;
  use_creation_time   BOOL := FALSE;
  hypercore_use_access_method   BOOL;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog, pg_temp;

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

  -- find primary dimension type --
  SELECT dim.column_type INTO dimtype
  FROM  _timescaledb_catalog.hypertable ht
        JOIN _timescaledb_catalog.dimension dim ON ht.id = dim.hypertable_id
  WHERE ht.id = htid
  ORDER BY dim.id
  LIMIT 1;

  compress_after      := jsonb_object_field_text(config, 'compress_after');
  IF compress_after IS NULL THEN
    compress_created_before := jsonb_object_field_text(config, 'compress_created_before');
    IF compress_created_before IS NULL THEN
        RAISE EXCEPTION 'job % config must have compress_after or compress_created_before', job_id;
    END IF;
    lag_value := compress_created_before;
    use_creation_time := true;
    dimtype := 'INTERVAL' ::regtype;
  ELSE
    lag_value := compress_after;
  END IF;

  hypercore_use_access_method := jsonb_object_field_text(config, 'hypercore_use_access_method')::bool;

  -- execute the properly type casts for the lag value
  CASE dimtype
    WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype, 'DATE'::regtype, 'INTERVAL' ::regtype  THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::INTERVAL, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'BIGINT'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::BIGINT, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'INTEGER'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::INTEGER, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'SMALLINT'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::SMALLINT, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
  END CASE;
  COMMIT;
END;
$$ LANGUAGE PLPGSQL;
