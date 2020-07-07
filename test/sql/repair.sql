-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test for the repair tables script in the upgrade directory. This
-- will check that the repair script can successfully repair the
-- dimension slice table.

-- First, create some hypertables to get dimension slices added. We
-- create tables so that we get slices for space- as well as
-- time-dimensions.

\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/repair-tables.sql

CREATE VIEW missing_slices AS
SELECT DISTINCT
    dimension_slice_id,
    constraint_name,
    attname AS column_name,
    pg_get_expr(conbin, conrelid) AS constraint_expr
FROM
    _timescaledb_catalog.chunk_constraint cc
    JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
    JOIN pg_constraint ON conname = constraint_name
    JOIN pg_namespace ns ON connamespace = ns.oid
        AND ns.nspname = ch.schema_name
    JOIN pg_attribute ON attnum = conkey[1]
        AND attrelid = conrelid
WHERE
    dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice);

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS :MODULE_PATHNAME, 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

CREATE TABLE chunk_test_int(time integer, temp float8, tag integer, color integer);
CREATE TABLE chunk_test_time(time timestamptz, temp float8, tag integer, color integer);

SELECT create_hypertable('chunk_test_int', 'time', 'tag', 2, chunk_time_interval => 3);
SELECT create_hypertable('chunk_test_time', 'time', 'tag', 2, chunk_time_interval => '1 day'::interval);

INSERT INTO chunk_test_int VALUES
       (4, 24.3, 1, 1),
       (4, 24.3, 2, 1),
       (10, 24.3, 2, 1);

INSERT INTO chunk_test_time VALUES
       ('2020-01-01 10:11:12', 24.3, 1, 1),
       ('2020-01-01 10:11:13', 24.3, 2, 1),
       ('2020-01-02 10:11:14', 24.3, 2, 1);

SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

SELECT DISTINCT
       chunk_id,
       dimension_slice_id,
       constraint_name,
       pg_get_expr(conbin, conrelid) AS constraint_expr
FROM _timescaledb_catalog.chunk_constraint,
     LATERAL (
     	     SELECT *
	     FROM pg_constraint JOIN pg_namespace ns ON connamespace = ns.oid
	     WHERE conname = constraint_name
     ) AS con
ORDER BY chunk_id, dimension_slice_id;

-- Now, we remove some dimension slices to break the table. For that
-- we need to remove some dimension slices. We could remove all, but
-- that will not tell us if the script works even for partial results
-- (which is the normal case), so we remove partial ones. In
-- particular, we have some slices that are referenced by several
-- chunks and we want to check that these works as well.
--
-- For each dimension, we will drop it, list the missing slices,
-- repair the table, and check that the repair added the missing
-- slices. Each test is preceeded by a comment to make diffs clearly
-- distinguishable from each others.

-- To drop rows from dimension_slice table, we need to remove some
-- constraints.
ALTER TABLE _timescaledb_catalog.chunk_constraint
      DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey;

-- Drop slice 1
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 1;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 2
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 2;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 3
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 3;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 4
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 4;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 5
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 5;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 6
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 6;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 7
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 7;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

-- Drop slice 8
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 8;
SELECT * FROM missing_slices;
CALL repair_dimension_slice();
SELECT * FROM missing_slices;

