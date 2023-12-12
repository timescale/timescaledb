-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT
     split_part(extversion, '.', 1)::int * 100000 +
     split_part(extversion, '.', 2)::int *    100 AS extversion_num
FROM
     pg_extension WHERE extname = 'timescaledb' \gset

SELECT
     :extversion_num <  201000 AS test_repair_dimension,
     :extversion_num >= 201000 AND :'TEST_VERSION' >= 'v8' AS test_repair_cagg_joins \gset

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

\z repair_test_int
\z repair_test_extra


