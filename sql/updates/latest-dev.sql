CREATE FUNCTION @extschema@.by_correlation(column_name NAME)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_correlated_dimension';

-- Remove a dimension on a hypertable. Currently only correlated constraint
-- dimensions can be removed since the other dimensions will need a data
-- rewrite which is not implemented
--
-- hypertable - OID of the table to remove dimension from
-- column_name - NAME of the column on which the dimension exists
-- if_not_exists - If set, and the dimension does not exist, generate a notice instead of an error
CREATE FUNCTION @extschema@.remove_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, dropped BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_remove' LANGUAGE C VOLATILE;

--
-- Rebuild the catalog table `_timescaledb_catalog.dimension with type column
--

CREATE TABLE _timescaledb_internal._tmp_dimension
AS SELECT * from _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_internal.tmp_dimension_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.dimension_id_seq;

--drop foreign keys on dimension table
ALTER TABLE _timescaledb_catalog.dimension_slice DROP CONSTRAINT
dimension_slice_dimension_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.hypertable_compression_settings;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_id_seq;
DROP TABLE _timescaledb_catalog.dimension;

CREATE TABLE _timescaledb_catalog.dimension (
  id serial NOT NULL ,
  hypertable_id integer NOT NULL,
  column_name name NOT NULL,
  column_type REGTYPE NOT NULL,
  aligned boolean NOT NULL,
  -- closed dimensions
  num_slices smallint NULL,
  partitioning_func_schema name NULL,
  partitioning_func name NULL,
  -- open dimensions (e.g., time)
  interval_length bigint NULL,
  -- compress interval is used by rollup procedure during compression
  -- in order to merge multiple chunks into a single one
  compress_interval_length bigint NULL,
  integer_now_func_schema name NULL,
  integer_now_func name NULL,
  type "char",
  -- table constraints
  CONSTRAINT dimension_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
  CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
  CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
  CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
  CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0 OR type = 'C'),
  CONSTRAINT dimension_compress_interval_length_check CHECK (compress_interval_length IS NULL OR compress_interval_length > 0),
  CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.dimension
( id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  compress_interval_length,
  integer_now_func_schema, integer_now_func,
  type)
SELECT id, hypertable_id, column_name, column_type,
  aligned, num_slices, partitioning_func_schema,
  partitioning_func, interval_length,
  compress_interval_length,
  integer_now_func_schema, integer_now_func,
  CASE WHEN interval_length IS NULL AND num_slices IS NOT NULL THEN
        'c' -- closed dimension
       WHEN interval_length IS NOT NULL AND num_slices is NULL THEN
        'o' -- open dimension
       ELSE
        'a' -- any.. This should never happen
  END as type
FROM _timescaledb_internal._tmp_dimension;

-- Check that there's no entry with type == 'a'
DO $$
DECLARE
  count_var INTEGER;
BEGIN
    SELECT count(*) FROM _timescaledb_catalog.dimension INTO count_var WHERE
        type = 'a';
    IF count_var != 0 THEN
        RAISE EXCEPTION 'invalid dimension entry found!';
    END IF;
END
$$;

ALTER SEQUENCE _timescaledb_catalog.dimension_id_seq OWNED BY _timescaledb_catalog.dimension.id;
SELECT setval('_timescaledb_catalog.dimension_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_dimension_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.dimension_slice ADD CONSTRAINT
dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id)
REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal._tmp_dimension;
DROP TABLE _timescaledb_internal.tmp_dimension_seq_value;

GRANT SELECT ON _timescaledb_catalog.dimension_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension TO PUBLIC;

-- end recreate _timescaledb_catalog.dimension table --
