SELECT _timescaledb_internal.set_time_columns_not_null();
--has to be done since old range_end for the CHECK constraint was 2147483647 on closed partitions.
DO $$
DECLARE
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    FOR chunk_constraint_row IN
        SELECT cc.*
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
        INNER JOIN  _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
        WHERE d.partitioning_func IS NOT NULL
    LOOP
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;

        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name);

        PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);
    END LOOP;
END$$;
