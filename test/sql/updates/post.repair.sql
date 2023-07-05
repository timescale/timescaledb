-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT extversion < '2.10.0' AS test_repair_dimension
FROM pg_extension
WHERE extname = 'timescaledb' \gset

SELECT extversion >= '2.10.0' AND :'TEST_VERSION' >= 'v8' AS test_repair_cagg_joins
FROM pg_extension
WHERE extname = 'timescaledb' \gset

\if :test_repair_dimension
    -- Re-add the dropped foreign key constraint that was dropped for
    -- repair testing.
    ALTER TABLE _timescaledb_catalog.chunk_constraint
    ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
    FOREIGN KEY (dimension_slice_id) REFERENCES _timescaledb_catalog.dimension_slice (id);
\endif

\if :test_repair_cagg_joins
--Check if the repaired cagg with joins work alright now
    \ir post.repair.cagg_joins.sql
    \ir post.repair.hierarchical_cagg.sql
\endif

