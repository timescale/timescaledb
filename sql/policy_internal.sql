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
_timescaledb_internal.policy_compression_interval( job_id INTEGER, 
   htid INTEGER,
   lag INTERVAL,
   maxchunks INTEGER,
   verbose_log BOOLEAN,
   recompress_enabled BOOLEAN)
AS $$
DECLARE
  htoid regclass;
  chunk_rec record;
  numchunks integer := 1;
BEGIN

  SELECT format('%I.%I',schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  FOR chunk_rec IN
    SELECT show.oid, ch.schema_name, ch.table_name, ch.status
    FROM show_chunks( htoid, older_than => lag) as show(oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname and ch.schema_name = pgns.nspname and ch.hypertable_id = htid
    WHERE ch.dropped is false and  (ch.status = 0 OR ch.status = 3)
  LOOP
    IF chunk_rec.status = 0 THEN
       PERFORM compress_chunk( chunk_rec.oid );
    ELSIF chunk_rec.status = 3 AND recompress_enabled = 'true' THEN
       PERFORM recompress_chunk( chunk_rec.oid );
    END IF;
    COMMIT;
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
_timescaledb_internal.policy_compression_integer( job_id INTEGER, 
   htid INTEGER,
   lag BIGINT,
   maxchunks INTEGER,
   verbose_log BOOLEAN,
   recompress_enabled BOOLEAN)
AS $$
DECLARE
  htoid regclass;
  chunk_rec record;
  numchunks integer := 0;
  lag_integer BIGINT;
BEGIN

  SELECT format('%I.%I',schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  --for the integer case , we have to compute the lag w.r.t 
  -- the integer_now function and then pass on to show_chunks
  lag_integer := _timescaledb_internal.subtract_integer_from_now( htoid, lag);

  FOR chunk_rec IN
    SELECT show.oid, ch.schema_name, ch.table_name, ch.status
    FROM show_chunks( htoid, older_than => lag_integer) SHOW (oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname and ch.schema_name = pgns.nspname and ch.hypertable_id = htid
    WHERE ch.dropped is false and  (ch.status = 0 OR ch.status = 3)
  LOOP
    IF chunk_rec.status = 0 THEN
       PERFORM compress_chunk( chunk_rec.oid );
    ELSIF chunk_rec.status = 3 AND recompress_enabled = 'true' THEN
       PERFORM recompress_chunk( chunk_rec.oid );
    END IF;
    COMMIT;
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
_timescaledb_internal.policy_compression( job_id INTEGER, config JSONB)
AS $$
DECLARE
  dimtype regtype;
  compress_after text;
  lag_interval interval;
  lag_integer bigint;
  htid integer;
  htoid regclass;
  chunk_rec record;
  verbose_log bool;
  maxchunks integer := 0;
  numchunks integer := 1;
  recompress_enabled bool;
BEGIN
  IF config IS NULL THEN
    RAISE EXCEPTION 'job % has null config', job_id;
  END IF;
 
  htid := jsonb_object_field_text (config, 'hypertable_id')::integer;
  IF htid is NULL THEN
    RAISE EXCEPTION 'job % config must have hypertable_id', job_id;
  END IF;
  
  verbose_log := jsonb_object_field_text (config, 'verbose_log')::boolean;
  IF verbose_log is NULL THEN
     verbose_log = 'false';
  END IF;
  
  maxchunks := jsonb_object_field_text (config, 'maxchunks_to_compress')::integer;
  IF maxchunks IS NULL THEN
    maxchunks = 0;
  END IF;
  
  recompress_enabled := jsonb_object_field_text (config, 'recompress')::boolean;
  IF recompress_enabled IS NULL THEN
    recompress_enabled = 'true';
  END IF;
  
  compress_after := jsonb_object_field_text(config, 'compress_after');
  IF compress_after IS NULL THEN
    RAISE EXCEPTION 'job % config must have compress_after', job_id;
  END IF;

  -- find primary dimension type --
  SELECT column_type INTO STRICT dimtype
  FROM ( SELECT ht.schema_name, ht.table_name, dim.column_name, dim.column_type,
         row_number() over(partition by ht.id order by dim.id) as rn
         FROM  _timescaledb_catalog.hypertable ht , 
               _timescaledb_catalog.dimension dim 
         WHERE ht.id = dim.hypertable_id and ht.id = htid ) q 
  WHERE rn = 1; 
 
  CASE WHEN (dimtype = 'TIMESTAMP'::regtype
      OR dimtype = 'TIMESTAMPTZ'::regtype
      OR dimtype = 'DATE'::regtype) THEN
      lag_interval := jsonb_object_field_text(config, 'compress_after')::interval ;
      CALL _timescaledb_internal.policy_compression_interval( 
           job_id, htid, lag_interval, 
           maxchunks, verbose_log, recompress_enabled);
  ELSE
      lag_integer := jsonb_object_field_text(config, 'compress_after')::bigint;
      CALL _timescaledb_internal.policy_compression_integer( 
            job_id, htid, lag_integer, 
            maxchunks, verbose_log, recompress_enabled );
  END CASE;
END;
$$ LANGUAGE PLPGSQL;
