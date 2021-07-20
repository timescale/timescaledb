-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We do not dump the size of the tables here since that might differ
-- between an updated node and a restored node. For examples, stats
-- tables can have different sizes, and this is not relevant for an
-- update test.
\dt _timescaledb_internal.*

CREATE OR REPLACE FUNCTION timescaledb_integrity_test()
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    dimension_slice RECORD;
    constraint_row RECORD;
    index_row      RECORD;
    chunk_count    INTEGER;
    chunk_constraint_count INTEGER;
    chunk_index_count      INTEGER;
BEGIN
    -- Check integrity of chunk indexes on non-distributed hypertables
    -- (distributed ones do not have chunk indexes on the access node)
    FOR index_row IN
    SELECT h.schema_name, c.relname AS index_name, h.id AS hypertable_id, h.table_name AS hypertable_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN pg_index i ON (i.indrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
    INNER JOIN pg_class c ON (i.indexrelid = c.oid)
    WHERE h.replication_factor IS NULL
    EXCEPT
    SELECT h.schema_name, c.relname AS index_name, h.id AS hypertable_id, h.table_name AS hypertable_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN pg_index i ON (i.indrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
    INNER JOIN pg_class c ON (i.indexrelid = c.oid)
    INNER JOIN pg_constraint cc ON (c.oid = cc.conindid)
    LOOP
        SELECT count(*) FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = index_row.hypertable_id AND NOT dropped
        INTO STRICT chunk_count;

        SELECT count(c.*) FROM _timescaledb_catalog.chunk_index c
        WHERE c.hypertable_id = index_row.hypertable_id
        AND c.hypertable_index_name = index_row.index_name
        INTO STRICT chunk_index_count;

        IF chunk_index_count != chunk_count THEN
           RAISE EXCEPTION 'Missing chunk indexes. Expected %, but found %', chunk_count, chunk_index_count;
        END IF;
    END LOOP;

    -- Check integrity of chunk_constraints
    FOR constraint_row IN
    SELECT c.conname, h.id AS hypertable_id FROM _timescaledb_catalog.hypertable h INNER JOIN
           pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
        WHERE c.contype != 'c'
    LOOP
        SELECT count(*) FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = constraint_row.hypertable_id
        INTO STRICT chunk_count;

        SELECT count(cc.*) FROM _timescaledb_catalog.chunk_constraint cc,
        _timescaledb_catalog.chunk c
        WHERE hypertable_constraint_name = constraint_row.conname
        AND c.id = cc.chunk_id
        AND c.hypertable_id = constraint_row.hypertable_id
        INTO STRICT chunk_constraint_count;

        IF chunk_constraint_count != chunk_count THEN
           RAISE EXCEPTION 'Missing chunk constraints for %. Expected %, but found %', constraint_row.conname, chunk_count, chunk_constraint_count;
        END IF;
    END LOOP;

    FOR dimension_slice IN
    SELECT chunk_id, dimension_slice_id FROM _timescaledb_catalog.chunk_constraint
     WHERE dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice)
    LOOP
      RAISE EXCEPTION 'Missing dimension slice with id % for chunk %.', dimension_slice.dimension_slice_id, dimension_slice.chunk_id;
    END LOOP;
END;
$BODY$;

SELECT timescaledb_integrity_test();

-- Verify that the default jobs are the same in bgw_job
SELECT id, application_name FROM _timescaledb_config.bgw_job ORDER BY id;
