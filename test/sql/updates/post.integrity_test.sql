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
BEGIN
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
SELECT relnamespace::regnamespace "JOB_SCHEMA" FROM pg_class WHERE relname='bgw_job' and relkind = 'r' \gset
SELECT id, application_name FROM :JOB_SCHEMA.bgw_job ORDER BY id;

