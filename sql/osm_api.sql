-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This function updates the dimension slice range stored in the catalog with the min and max
-- values that the OSM chunk contains. Since there is only one OSM chunk per hypertable with
-- only a time dimension, the hypertable is used to determine the corresponding slice
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_osm_range_update(
    hypertable REGCLASS,
    range_start ANYELEMENT = NULL::bigint,
    range_end ANYELEMENT = NULL,
    empty BOOL = false
) RETURNS BOOL AS '@MODULE_PATHNAME@',
'ts_hypertable_osm_range_update' LANGUAGE C VOLATILE;

-- Acquires a FOR UPDATE row lock on the dimension slice tuple belonging to the
-- OSM chunk of the given hypertable. There is exactly one OSM chunk per
-- hypertable, so this locks its single dimension_slice entry. The lock is held
-- until the end of the current transaction; nothing is returned.
CREATE OR REPLACE FUNCTION _timescaledb_functions.lock_osm_chunk_dimension_slice(
    htoid REGCLASS
) RETURNS VOID AS '@MODULE_PATHNAME@',
'ts_lock_osm_chunk_dimension_slice' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_info(IN relation REGCLASS, OUT hypertable_id INTEGER, OUT schema_name name, OUT table_name name, OUT is_cagg BOOLEAN) AS $$
  SELECT ht.id, ht.schema_name, ht.table_name, false AS is_cagg
  FROM _timescaledb_catalog.hypertable ht
  JOIN pg_class c ON ht.table_name = c.relname AND c.oid = $1
  JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = ht.schema_name;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_info_by_id(IN hypertable_id INTEGER, OUT hypertable_id INTEGER, OUT schema_name name, OUT table_name name, OUT is_cagg BOOLEAN) AS $$
  SELECT ht.id, ht.schema_name, ht.table_name, false AS is_cagg
  FROM _timescaledb_catalog.hypertable ht
  WHERE ht.id = $1;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_integer_now_func(IN htid INTEGER, OUT hypertable REGCLASS, OUT integer_func REGPROC)
AS $$
DECLARE
  func_name TEXT;
  func_schema TEXT;
  raw_ht_id INTEGER;
BEGIN
  SELECT integer_now_func_schema, integer_now_func INTO func_schema, func_name
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = $1 ORDER BY id LIMIT 1;
  IF func_schema IS NOT NULL AND func_name IS NOT NULL THEN
    SELECT to_regclass(format('%I.%I', schema_name, table_name)) INTO hypertable FROM _timescaledb_catalog.hypertable WHERE id = $1;
    integer_func := to_regproc(format('%I.%I', func_schema, func_name));
    RETURN;
  END IF;

  SELECT raw_hypertable_id INTO raw_ht_id FROM _timescaledb_catalog.continuous_agg WHERE mat_hypertable_id = htid;
  IF raw_ht_id IS NOT NULL THEN
    SELECT * INTO hypertable, integer_func FROM _timescaledb_functions.get_integer_now_func(raw_ht_id);
    RETURN;
  END IF;

  RETURN;
END
$$ LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_primary_dimension(IN hypertable_id INTEGER, OUT column_name NAME, OUT column_type REGTYPE, OUT integer_now_func regproc) AS $$
  SELECT
    d.column_name,
    d.column_type,
    (_timescaledb_functions.get_integer_now_func(d.hypertable_id)).integer_func AS integer_now_func
  FROM _timescaledb_catalog.dimension d
  WHERE d.hypertable_id = $1 ORDER BY d.id LIMIT 1;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_chunk_info(IN chunk REGCLASS, OUT chunk_id INTEGER, OUT hypertable_id INTEGER, OUT ht_schema_name NAME, OUT ht_table_name NAME, OUT chunk_schema NAME, OUT chunk_name NAME, OUT is_osm_chunk BOOLEAN, OUT is_frozen BOOLEAN) AS $$
  SELECT
		ch.id AS chunk_id,
		ch.hypertable_id, ht.schema_name AS ht_schema_name, ht.table_name AS ht_table_name,
    ch.schema_name AS chunk_schema, ch.table_name AS chunk_name,
    ch.osm_chunk AS is_osm_chunk,
    (ch.status & 4)::bool AS is_frozen
	FROM _timescaledb_catalog.chunk ch
  JOIN pg_class c ON c.oid=$1 AND ch.table_name=c.relname
  JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = ch.schema_name
  JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_chunk_info_by_id(IN chunk_id INTEGER, OUT chunk_id INTEGER, OUT hypertable_id INTEGER, OUT ht_schema_name NAME, OUT ht_table_name NAME, OUT chunk_schema NAME, OUT chunk_name NAME, OUT is_osm_chunk BOOLEAN, OUT is_frozen BOOLEAN) AS $$
  SELECT
		ch.id AS chunk_id,
		ch.hypertable_id, ht.schema_name AS ht_schema_name, ht.table_name AS ht_table_name,
    ch.schema_name AS chunk_schema, ch.table_name AS chunk_name,
    ch.osm_chunk AS is_osm_chunk,
    (ch.status & 4)::bool AS is_frozen
	FROM _timescaledb_catalog.chunk ch
  JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id
  WHERE ch.id = $1;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_chunk_primary_range(IN chunk REGCLASS, OUT range_start BIGINT, OUT range_end BIGINT)
AS $$
  SELECT range_start, range_end
  FROM _timescaledb_catalog.dimension_slice d
  JOIN _timescaledb_catalog.chunk ch ON ch.id = d.chunk_id
  JOIN pg_class c ON c.oid=$1 AND ch.table_name=c.relname
  JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = ch.schema_name
  ORDER BY dimension_id LIMIT 1;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_chunk_primary_range_by_id(IN chunk_id INTEGER, OUT range_start BIGINT, OUT range_end BIGINT)
AS $$
  SELECT range_start, range_end
  FROM _timescaledb_catalog.dimension_slice d
  WHERE chunk_id = $1 ORDER BY dimension_id LIMIT 1;
$$ LANGUAGE SQL STABLE SET search_path = pg_catalog, pg_temp;

