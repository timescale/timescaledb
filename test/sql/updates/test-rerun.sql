--keep same order of tables as tables.sql
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions
FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.tablespace ORDER BY id;
--partitioning function changed, so don't output
SELECT id, hypertable_id, column_name, column_type, aligned, num_slices, interval_length
FROM _timescaledb_catalog.dimension ORDER BY id;
SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY dimension_id, range_start, range_end;
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
SELECT chunk_id, dimension_slice_id, hypertable_constraint_name, constraint_name IS NULL FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, hypertable_constraint_name;
--index name changed, so don't output that
SELECT hypertable_index_name, hypertable_id, chunk_id
FROM _timescaledb_catalog.chunk_index
ORDER BY  hypertable_index_name, hypertable_id, chunk_id;
