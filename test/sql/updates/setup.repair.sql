-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test file to check that the repair script works. It will create a
-- bunch of tables and "break" them by removing dimension slices from
-- the dimension slice table. The repair script should then repair all
-- of them and there should be no dimension slices missing.

SELECT extversion < '2.10.0' AS test_repair_dimension
FROM pg_extension
WHERE extname = 'timescaledb' \gset

SELECT extversion >= '2.10.0' AS has_cagg_joins
FROM pg_extension
WHERE extname = 'timescaledb' \gset

CREATE TABLE repair_test_int(time integer not null, temp float8, tag integer, color integer);
CREATE TABLE repair_test_timestamptz(time timestamptz not null, temp float8, tag integer, color integer);
CREATE TABLE repair_test_extra(time timestamptz not null, temp float8, tag integer, color integer);
CREATE TABLE repair_test_timestamp(time timestamp not null, temp float8, tag integer, color integer);
CREATE TABLE repair_test_date(time date not null, temp float8, tag integer, color integer);

-- We only break the dimension slice table if there is repair that is
-- going to be done, but we create the tables regardless so that we
-- can compare the databases.
SELECT create_hypertable('repair_test_int', 'time', 'tag', 2, chunk_time_interval => '3'::bigint);
SELECT create_hypertable('repair_test_timestamptz', 'time', 'tag', 2, chunk_time_interval => '1 day'::interval);
SELECT create_hypertable('repair_test_extra', 'time', 'tag', 2, chunk_time_interval => '1 day'::interval);
SELECT create_hypertable('repair_test_timestamp', 'time', 'tag', 2, chunk_time_interval => '1 day'::interval);
SELECT create_hypertable('repair_test_date', 'time', 'tag', 2, chunk_time_interval => '1 day'::interval);

-- These rows will create four constraints for each table.
INSERT INTO repair_test_int VALUES
       (4, 24.3, 1, 1),
       (4, 24.3, 2, 1),
       (10, 24.3, 2, 1);

INSERT INTO repair_test_timestamptz VALUES
       ('2020-01-01 10:11:12', 24.3, 1, 1),
       ('2020-01-01 10:11:13', 24.3, 2, 1),
       ('2020-01-02 10:11:14', 24.3, 2, 1);

INSERT INTO repair_test_extra VALUES
       ('2020-01-01 10:11:12', 24.3, 1, 1),
       ('2020-01-01 10:11:13', 24.3, 2, 1),
       ('2020-01-02 10:11:14', 24.3, 2, 1);

INSERT INTO repair_test_timestamp VALUES
       ('2020-01-01 10:11:12', 24.3, 1, 1),
       ('2020-01-01 10:11:13', 24.3, 2, 1),
       ('2020-01-02 10:11:14', 24.3, 2, 1);

INSERT INTO repair_test_date VALUES
       ('2020-01-01 10:11:12', 24.3, 1, 1),
       ('2020-01-01 10:11:13', 24.3, 2, 1),
       ('2020-01-02 10:11:14', 24.3, 2, 1);

-- We always drop the constraint and restore it in the
-- post.repair.sql.
--
-- This way if there are constraint violations remaining that wasn't
-- repaired properly, we will notice them when restoring the
-- constraint.
ALTER TABLE _timescaledb_catalog.chunk_constraint
      DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey;

CREATE VIEW slices AS (
      SELECT ch.hypertable_id,
             (
                 SELECT format('%I.%I', schema_name, table_name)::regclass
                 FROM _timescaledb_catalog.hypertable ht
                 WHERE ht.id = ch.hypertable_id
             ) AS hypertable,
             chunk_id,
             di.id AS dimension_id,
             dimension_slice_id,
             constraint_name,
             attname AS column_name,
             column_type,
             pg_get_expr(conbin, conrelid) AS constraint_expr
      FROM _timescaledb_catalog.chunk_constraint cc
      JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
      JOIN pg_constraint ON conname = constraint_name
      JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
      JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
      JOIN _timescaledb_catalog.dimension di
           ON di.hypertable_id = ch.hypertable_id AND attname = di.column_name
   );

\if :test_repair_dimension
-- Break the first time dimension on each table. These are different
-- depending on the time type for the table and we need to check all
-- versions.
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_int'::regclass AND column_name = 'time'
   ORDER BY dimension_slice_id LIMIT 1
);

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_timestamp'::regclass AND column_name = 'time'
   ORDER BY dimension_slice_id LIMIT 1
);

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_timestamptz'::regclass AND column_name = 'time'
   ORDER BY dimension_slice_id LIMIT 1
);

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_date'::regclass AND column_name = 'time'
   ORDER BY dimension_slice_id LIMIT 1
);

-- Delete all dimension slices for one table to break it seriously. It
-- should still be repaired.
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_extra'::regclass
);

-- Break the partition constraints on some of the tables. The
-- partition constraints look the same in all tables so we create a
-- mix of tables with no missing partition constraint slices, just one
-- missing partition constraint dimension slice, and several missing
-- partition constraint dimension slices.
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_timestamp'::regclass AND column_name = 'tag'
   ORDER BY dimension_slice_id LIMIT 1
);

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN (
   SELECT dimension_slice_id FROM slices
   WHERE hypertable = 'repair_test_date'::regclass AND column_name = 'tag'
   ORDER BY dimension_slice_id
);

\echo **** Expected repairs ****
WITH unparsed_slices AS (
    SELECT dimension_id,
	   dimension_slice_id,
	   hypertable,
	   constraint_name,
	   column_type,
	   column_name,
	   (SELECT SUBSTRING(constraint_expr, $$>=\s*'?([\w\d\s:+-]+)'?$$)) AS range_start,
	   (SELECT SUBSTRING(constraint_expr, $$<\s*'?([\w\d\s:+-]+)'?$$)) AS range_end
      FROM slices
)
SELECT DISTINCT
       dimension_slice_id,
       dimension_id,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_start::date)::bigint * 1000000
       ELSE
            CASE
            WHEN range_start IS NULL
            THEN (-9223372036854775808)::bigint
            ELSE range_start::bigint
            END
       END AS range_start,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_end::date)::bigint * 1000000
       ELSE
            CASE WHEN range_end IS NULL
            THEN 9223372036854775807::bigint
            ELSE range_end::bigint
            END
       END AS range_end
  FROM unparsed_slices
  WHERE dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice);

DROP VIEW slices;
\endif

\ir setup.repair.cagg.sql
\if :has_cagg_joins
     \ir setup.repair.hierarchical_cagg.sql
     \ir setup.repair.cagg_joins.sql
\endif
