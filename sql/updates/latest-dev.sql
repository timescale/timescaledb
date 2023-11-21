CREATE TYPE _timescaledb_internal.dimension_info;

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_in(cstring)
    RETURNS _timescaledb_internal.dimension_info
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_in';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_out(_timescaledb_internal.dimension_info)
    RETURNS cstring
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_dimension_info_out';

CREATE TYPE _timescaledb_internal.dimension_info (
    INPUT = _timescaledb_functions.dimension_info_in,
    OUTPUT = _timescaledb_functions.dimension_info_out,
    INTERNALLENGTH = VARIABLE
);

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.by_hash(column_name NAME, number_partitions INTEGER,
                                    partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_hash_dimension';

CREATE FUNCTION @extschema@.by_range(column_name NAME,
                                     partition_interval ANYELEMENT = NULL::bigint,
                                     partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_range_dimension';

--
-- Rebuild the catalog table `_timescaledb_catalog.chunk` to
-- add new column `creation_time`
--
CREATE TABLE _timescaledb_internal.chunk_tmp
AS SELECT * from _timescaledb_catalog.chunk;

CREATE TABLE _timescaledb_internal.tmp_chunk_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.chunk_id_seq;

--drop foreign keys on chunk table
ALTER TABLE _timescaledb_catalog.chunk_constraint DROP CONSTRAINT
chunk_constraint_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index DROP CONSTRAINT
chunk_index_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_data_node DROP CONSTRAINT
chunk_data_node_chunk_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation DROP CONSTRAINT
chunk_copy_operation_chunk_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;
DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk
( id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk)
SELECT id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk
FROM _timescaledb_internal.chunk_tmp;

-- update creation_time for chunks
UPDATE
    _timescaledb_catalog.chunk c
SET
    creation_time = (pg_catalog.pg_stat_file(pg_catalog.pg_relation_filepath(r.oid))).modification
FROM
    pg_class r, pg_namespace n
WHERE
    r.relnamespace = n.oid
    AND r.relname = c.table_name
    AND n.nspname = c.schema_name
    AND r.relkind = 'r'
    AND c.dropped IS FALSE;

-- Make sure that there are no record with empty creation time
UPDATE _timescaledb_catalog.chunk SET creation_time = now() WHERE creation_time IS NULL;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk(hypertable_id, creation_time);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY ( compressed_chunk_id )
 REFERENCES _timescaledb_catalog.chunk( id );

--add foreign key constraint
ALTER TABLE _timescaledb_catalog.chunk
      ADD CONSTRAINT chunk_hypertable_id_fkey
      FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

-- Add non-null constraint
ALTER TABLE _timescaledb_catalog.chunk
  ALTER COLUMN creation_time SET NOT NULL;

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.chunk_constraint ADD CONSTRAINT
chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_catalog.chunk_index ADD CONSTRAINT
chunk_index_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_data_node ADD CONSTRAINT
chunk_data_node_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation ADD CONSTRAINT
chunk_copy_operation_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal.chunk_tmp;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;
-- end recreate _timescaledb_catalog.chunk table --

--
-- Rebuild the catalog table `_timescaledb_catalog.compression_chunk_size` to
-- add new column `numrows_frozen_immediately`
--
CREATE TABLE _timescaledb_internal.compression_chunk_size_tmp
    AS SELECT * from _timescaledb_catalog.compression_chunk_size;

-- Drop depended views
-- We assume that '_timescaledb_internal.compressed_chunk_stats' was already dropped in this update
-- (see above)

-- Drop table
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_chunk_size;
DROP TABLE _timescaledb_catalog.compression_chunk_size;

CREATE TABLE _timescaledb_catalog.compression_chunk_size (
  chunk_id integer NOT NULL,
  compressed_chunk_id integer NOT NULL,
  uncompressed_heap_size bigint NOT NULL,
  uncompressed_toast_size bigint NOT NULL,
  uncompressed_index_size bigint NOT NULL,
  compressed_heap_size bigint NOT NULL,
  compressed_toast_size bigint NOT NULL,
  compressed_index_size bigint NOT NULL,
  numrows_pre_compression bigint,
  numrows_post_compression bigint,
  numrows_frozen_immediately bigint,
  -- table constraints
  CONSTRAINT compression_chunk_size_pkey PRIMARY KEY (chunk_id),
  CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.compression_chunk_size
(chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression, numrows_frozen_immediately)
SELECT chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression, 0
FROM _timescaledb_internal.compression_chunk_size_tmp;

DROP TABLE _timescaledb_internal.compression_chunk_size_tmp;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

GRANT SELECT ON _timescaledb_catalog.compression_chunk_size TO PUBLIC;

-- End modify `_timescaledb_catalog.compression_chunk_size`

DROP FUNCTION @extschema@.drop_chunks(REGCLASS, "any", "any", BOOL);
CREATE FUNCTION @extschema@.drop_chunks(
     relation               REGCLASS,
     older_than             "any" = NULL,
     newer_than             "any" = NULL,
     verbose                BOOLEAN = FALSE,
     created_before         "any" = NULL,
     created_after          "any" = NULL
 ) RETURNS SETOF TEXT AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
 LANGUAGE C VOLATILE PARALLEL UNSAFE;

DROP FUNCTION @extschema@.show_chunks(REGCLASS, "any", "any");
CREATE FUNCTION @extschema@.show_chunks(
     relation               REGCLASS,
     older_than             "any" = NULL,
     newer_than             "any" = NULL,
     created_before         "any" = NULL,
     created_after          "any" = NULL
 ) RETURNS SETOF REGCLASS AS '@MODULE_PATHNAME@', 'ts_chunk_show_chunks'
 LANGUAGE C STABLE PARALLEL SAFE;

DROP FUNCTION @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
CREATE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any" = NULL,
       if_not_exists BOOL = false,
       schedule_interval INTERVAL = NULL,
       initial_start TIMESTAMPTZ = NULL,
       timezone TEXT = NULL,
       drop_created_before INTERVAL = NULL
)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add'
LANGUAGE C VOLATILE;

DROP FUNCTION @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN);
CREATE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN)
AS $$
DECLARE
  htoid       REGCLASS;
  chunk_rec   RECORD;
  numchunks   INTEGER := 1;
  _message     text;
  _detail      text;
  -- chunk status bits:
  bit_compressed int := 1;
  bit_compressed_unordered int := 2;
  bit_frozen int := 4;
  bit_compressed_partial int := 8;
  creation_lag INTERVAL := NULL;
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
    WHERE
      ch.dropped IS FALSE
      AND (
        ch.status = 0 OR
        (
          ch.status & bit_compressed > 0 AND (
            ch.status & bit_compressed_unordered > 0 OR
            ch.status & bit_compressed_partial > 0
          )
        )
      )
  LOOP
    IF chunk_rec.status = 0 THEN
      BEGIN
        PERFORM @extschema@.compress_chunk( chunk_rec.oid );
      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _message = MESSAGE_TEXT,
            _detail = PG_EXCEPTION_DETAIL;
        RAISE WARNING 'compressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
    ELSIF
      (
        chunk_rec.status & bit_compressed > 0 AND (
          chunk_rec.status & bit_compressed_unordered > 0 OR
          chunk_rec.status & bit_compressed_partial > 0
        )
      ) AND recompress_enabled IS TRUE THEN
      BEGIN
        PERFORM @extschema@.decompress_chunk(chunk_rec.oid, if_compressed => true);
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'decompressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
      -- SET LOCAL is only active until end of transaction.
      -- While we could use SET at the start of the function we do not
      -- want to bleed out search_path to caller, so we do SET LOCAL
      -- again after COMMIT
      BEGIN
        PERFORM @extschema@.compress_chunk(chunk_rec.oid);
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'compressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
    END IF;
    COMMIT;
    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog, pg_temp;
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

-- fix atttypmod and attcollation for segmentby columns
DO $$
DECLARE
  htc_id INTEGER;
  htc REGCLASS;
  _attname NAME;
  _atttypmod INTEGER;
  _attcollation OID;
BEGIN
  -- find any segmentby columns where typmod and collation in
  -- the compressed hypertable does not match the uncompressed
  -- hypertable values
  FOR htc_id, htc, _attname, _atttypmod, _attcollation IN
    SELECT cat.htc_id, cat.htc, pga.attname, ht_mod, ht_coll
    FROM pg_attribute pga
    INNER JOIN
    (
      SELECT
        htc.id AS htc_id,
        format('%I.%I',htc.schema_name,htc.table_name) AS htc,
        att_ht.atttypmod AS ht_mod,
        att_ht.attcollation AS ht_coll,
        c.attname
      FROM _timescaledb_catalog.hypertable_compression c
      INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id=c.hypertable_id
      INNER JOIN pg_attribute att_ht ON att_ht.attname = c.attname AND att_ht.attrelid = format('%I.%I',ht.schema_name,ht.table_name)::regclass
      INNER JOIN _timescaledb_catalog.hypertable htc ON htc.id=ht.compressed_hypertable_id
      WHERE c.segmentby_column_index > 0
    ) cat ON cat.htc::regclass = pga.attrelid AND cat.attname = pga.attname
    WHERE pga.atttypmod <> ht_mod OR pga.attcollation <> ht_coll
  LOOP
    -- fix typmod and collation for the compressed hypertable and all compressed chunks
    UPDATE pg_attribute SET atttypmod = _atttypmod, attcollation = _attcollation WHERE attname = _attname AND attrelid IN (
      SELECT format('%I.%I',schema_name,table_name)::regclass from _timescaledb_catalog.chunk WHERE hypertable_id = htc_id AND NOT dropped UNION ALL SELECT htc
    );
  END LOOP;
END
$$;
