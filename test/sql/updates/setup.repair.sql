-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test file to check that the repair script works. It will create a
-- bunch of tables and "break" them by removing dimension slices from
-- the dimension slice table. The repair script should then repair all
-- of them and there should be no dimension slices missing.

CREATE USER wizard;
CREATE USER "Random L User";

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

-- Grant privileges to some tables above. All should be repaired.
GRANT ALL ON repair_test_int TO wizard;
GRANT ALL ON repair_test_extra TO wizard;
GRANT SELECT, INSERT ON repair_test_int TO "Random L User";
GRANT INSERT ON repair_test_extra TO "Random L User";

-- Break the relacl of the table by deleting users directly from
-- pg_authid table.
DELETE FROM pg_authid WHERE rolname IN ('wizard', 'Random L User');

\ir setup.repair.cagg.sql
\ir setup.repair.hierarchical_cagg.sql
\ir setup.repair.cagg_joins.sql
