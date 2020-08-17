-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Hypertables can break as a result of race conditions, but we should
-- still not crash when trying to truncate or delete the broken table.

\c :TEST_DBNAME :ROLE_SUPERUSER

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

-- To drop rows from dimension_slice table, we need to remove some
-- constraints.
ALTER TABLE _timescaledb_catalog.chunk_constraint
      DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey;

CREATE TABLE chunk_test_int(time integer, temp float8, tag integer, color integer);
SELECT create_hypertable('chunk_test_int', 'time', 'tag', 2, chunk_time_interval => 3);

INSERT INTO chunk_test_int VALUES
       (4, 24.3, 1, 1),
       (4, 24.3, 2, 1),
       (10, 24.3, 2, 1);

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

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 1;
SELECT * FROM missing_slices;

TRUNCATE TABLE chunk_test_int;
DROP TABLE chunk_test_int;

CREATE TABLE chunk_test_int(time integer, temp float8, tag integer, color integer);
SELECT create_hypertable('chunk_test_int', 'time', 'tag', 2, chunk_time_interval => 3);

INSERT INTO chunk_test_int VALUES
       (4, 24.3, 1, 1),
       (4, 24.3, 2, 1),
       (10, 24.3, 2, 1);

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

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id = 5;
SELECT * FROM missing_slices;

DROP TABLE chunk_test_int;
