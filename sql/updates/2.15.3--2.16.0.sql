-- Enable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to which the column belongs to
-- column_name - The column to track statistics for
-- if_not_exists - If set, and the entry already exists, generate a notice instead of an error
CREATE FUNCTION @extschema@.enable_column_stats(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(column_stats_id INT, enabled BOOL)
AS 'SELECT NULL,NULL' LANGUAGE SQL VOLATILE SET search_path = pg_catalog, pg_temp;

-- Disable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to remove from
-- column_name - NAME of the column on which the stats are tracked
-- if_not_exists - If set, and the entry does not exist,
-- generate a notice instead of an error
CREATE FUNCTION @extschema@.disable_column_stats(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, column_name NAME, disabled BOOL)
AS 'SELECT NULL,NULL,NULL' LANGUAGE SQL VOLATILE SET search_path = pg_catalog, pg_temp;

-- Track statistics for columns of chunks from a hypertable.
-- Currently, we track the min/max range for a given column across chunks.
-- More statistics (like bloom filters) will be added in the future.
--
-- A "special" entry for a column with invalid chunk_id, PG_INT64_MAX,
-- PG_INT64_MIN indicates that min/max ranges could be computed for this column
-- for chunks.
--
-- The ranges can overlap across chunks. The values could be out-of-date if
-- modifications/changes occur in the corresponding chunk and such entries
-- should be marked as "invalid" to ensure that the chunk is in
-- appropriate state to be able to use these values. Thus these entries
-- are different from dimension_slice which is used for tracking partitioning
-- column ranges which have different characteristics.
--
-- Currently this catalog supports datatypes like INT, SERIAL, BIGSERIAL,
-- DATE, TIMESTAMP etc. by storing the ranges in bigint columns. In the
-- future, we could support additional datatypes (which support btree style
-- >, <, = comparators) by storing their textual representation.
--
CREATE TABLE _timescaledb_catalog.chunk_column_stats (
  id serial NOT NULL,
  hypertable_id integer NOT NULL,
  chunk_id integer NOT NULL,
  column_name name NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  valid boolean NOT NULL,
  -- table constraints
  CONSTRAINT chunk_column_stats_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_column_stats_ht_id_chunk_id_colname_key UNIQUE (hypertable_id, chunk_id, column_name),
  CONSTRAINT chunk_column_stats_range_check CHECK (range_start <= range_end),
  CONSTRAINT chunk_column_stats_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id),
  CONSTRAINT chunk_column_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_column_stats', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk_column_stats', 'id'), '');

GRANT SELECT ON _timescaledb_catalog.chunk_column_stats TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk_column_stats_id_seq TO PUBLIC;

-- Remove foreign key constraints from compressed chunks
DO $$
DECLARE
  conrelid regclass;
  conname name;
BEGIN
  FOR conrelid, conname IN
  SELECT
    con.conrelid::regclass,
    con.conname
  FROM _timescaledb_catalog.chunk ch
  JOIN pg_constraint con ON con.conrelid = format('%I.%I',schema_name,table_name)::regclass AND con.contype='f'
  WHERE NOT ch.dropped AND EXISTS(SELECT FROM _timescaledb_catalog.chunk ch2 WHERE NOT ch2.dropped AND ch2.compressed_chunk_id=ch.id)
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', conrelid, conname);
  END LOOP;
END $$;

