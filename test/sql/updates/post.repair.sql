-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Re-add the dropped foreign key constraint that was dropped for
-- repair testing.
ALTER TABLE _timescaledb_catalog.chunk_constraint
    ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
    FOREIGN KEY (dimension_slice_id) REFERENCES _timescaledb_catalog.dimension_slice (id);

