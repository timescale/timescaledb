SELECT _timescaledb_internal.set_time_columns_not_null();
--has to be done since old range_end for the CHECK constraint was 2147483647 on closed partitions.
DO $$
DECLARE
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    -- Need to do this update in two loops: first remove constraints, then add back.
    -- This is because we can only remove the old partitioning function when
    -- there are no constraints on the tables referencing the old function
    FOR chunk_constraint_row IN
        SELECT cc.*
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
        INNER JOIN  _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
        WHERE d.partitioning_func IS NOT NULL
    LOOP
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;

        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name);
    END LOOP;

    DROP FUNCTION _timescaledb_internal.get_partition_for_key(text);

    FOR chunk_constraint_row IN
        SELECT cc.*
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
        INNER JOIN  _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
        WHERE d.partitioning_func IS NOT NULL
    LOOP
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
        PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);
    END LOOP;

END$$;

--for timestamp (non-tz) columns we used to have internal_time -> constraint_time via local_time. 
--So the internal time was interpreted as UTC but the constraint was printed in terms of the local time.
--Now we interpret the internal_time as UTC and the constraints is generated as UTC as well. 
--These constraints should not be re-written since they are correct for the data. But we should adjust the internal time
--to be consistent. 

-- So _timescaledb_internal.to_timestamp(internal_time)::timestamp gives you the old constraint
-- We then convert it to timestamptz as though it was at UTC
-- finally, we convert it to the internal represtentation back.

UPDATE _timescaledb_catalog.dimension_slice ds
SET 
range_end = _timescaledb_internal.to_unix_microseconds(timezone('UTC',_timescaledb_internal.to_timestamp(range_end)::timestamp)),
range_start = _timescaledb_internal.to_unix_microseconds(timezone('UTC',_timescaledb_internal.to_timestamp(range_start)::timestamp))
FROM _timescaledb_catalog.dimension d
WHERE ds.dimension_id = d.id AND d.column_type = 'timestamp'::regtype;

