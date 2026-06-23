-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We do not dump the size of the tables here since that might differ
-- between an updated node and a restored node. For examples, stats
-- tables can have different sizes, and this is not relevant for an
-- update test. This mirrors \dt _timescaledb_internal.* but normalizes the
-- chunk relation names, which are renumbered and renamed across the upgrade.
SELECT n.nspname AS "Schema",
       pg_temp.normalize_chunk(c.relname) AS "Name",
       'table' AS "Type",
       pg_catalog.pg_get_userbyid(c.relowner) AS "Owner"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '_timescaledb_internal'
  AND c.relkind = 'r'
ORDER BY 1, 2, 4;

CREATE OR REPLACE FUNCTION timescaledb_integrity_test()
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    gap RECORD;
BEGIN
    IF (SELECT extversion >= '2.28.0' FROM pg_extension WHERE extname = 'timescaledb') THEN
        FOR gap IN
        SELECT ch.id AS chunk_id, d.id AS dimension_id
        FROM _timescaledb_catalog.chunk ch
        JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = ch.hypertable_id
        WHERE NOT ch.osm_chunk
          AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.dimension_slice ds
                          WHERE ds.chunk_id = ch.id AND ds.dimension_id = d.id)
        LOOP
          RAISE EXCEPTION 'Missing dimension slice for chunk % on dimension %.', gap.chunk_id, gap.dimension_id;
        END LOOP;
    ELSE
        FOR gap IN
        SELECT ch.id AS chunk_id, d.id AS dimension_id
        FROM _timescaledb_catalog.chunk ch
        JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = ch.hypertable_id
        WHERE NOT ch.osm_chunk
          AND NOT EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk_constraint cc
                          JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
                          WHERE cc.chunk_id = ch.id AND ds.dimension_id = d.id)
        LOOP
          RAISE EXCEPTION 'Missing dimension slice for chunk % on dimension %.', gap.chunk_id, gap.dimension_id;
        END LOOP;
    END IF;
END;
$BODY$;

SELECT timescaledb_integrity_test();

-- Verify that the default jobs are the same in bgw_job
SELECT relnamespace::regnamespace "JOB_SCHEMA" FROM pg_class WHERE relname='bgw_job' and relkind = 'r' \gset
SELECT id, application_name FROM :JOB_SCHEMA.bgw_job ORDER BY id;

