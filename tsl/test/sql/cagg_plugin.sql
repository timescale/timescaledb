-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Test the invalidation plugin and make sure it can return the data
-- we expect. We do that by creating a hypertable and then registering
-- a logical decoding slot on it and using the plugin to get the
-- changes.

SET timezone TO UTC;
SET datestyle TO ISO;
SELECT setseed(1);

CREATE FUNCTION get_invalidations(slot text, hypertable name, attr name)
RETURNS TABLE (hypertable_relid regclass,
	       lowest_modified_value timestamptz,
	       greatest_modified_value timestamptz) AS $$
WITH
  changes AS (
      SELECT (_timescaledb_functions.cagg_parse_invalidation_record(data)).*
        FROM pg_logical_slot_get_binary_changes(slot, NULL, NULL, hypertable, attr))
SELECT hypertable_relid,
       _timescaledb_functions.to_timestamp(lowest_modified_value),
       _timescaledb_functions.to_timestamp(greatest_modified_value)
  FROM changes
ORDER BY 1,2,3;
$$ LANGUAGE sql;

-- Creating a table with a primary key since we need a replica
-- identity to use logical decoding.
CREATE TABLE conditions (
       recorded_at timestamptz NOT NULL,
       device_id int,
       temp float,
       primary key (recorded_at, device_id)
);
SELECT create_hypertable('conditions', 'recorded_at');

SELECT setseed(1.0);

INSERT INTO conditions
SELECT recorded_at, (random()*3 + 1)::int, random()*80 - 40
  FROM generate_series('2025-02-01'::timestamptz,
  	               '2025-03-31'::timestamptz,
		       '1 minute'::interval) AS recorded_at;

select from pg_create_logical_replication_slot('my_slot', 'timescaledb-invalidations', false, true);

-- Generate a few entries in one go. This caused some problems
-- initially, so check this first.
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 98, 37.0);
INSERT INTO conditions VALUES
       ('2020-05-02 12:34:56Z', 98, 37.0),
       ('2020-05-02 13:45:00Z', 98, 37.0);
SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Generate a single invalidation line and check how that appears.
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 99, 37.0);

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

INSERT INTO conditions VALUES
       ('2020-05-02 12:34:56Z', 99, 37.0),
       ('2020-05-02 13:45:00Z', 99, 37.0);

-- Check that we got the invalidations
SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- This should generate an invalidation including the full range since
-- it is inside a transaction.
INSERT INTO conditions
SELECT recorded_at, 99, random()*80 - 40
  FROM generate_series('2025-02-01'::timestamptz,
  	               '2025-03-31'::timestamptz,
		       '1 minute'::interval) AS recorded_at;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

UPDATE conditions SET temp = 0.0 WHERE device_id = 99;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

DELETE FROM conditions WHERE device_id = 99;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Test that we can do multiple changes (in different transactions) in
-- a suite and get the changes for that using the plugin.
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 99, 37.0);
INSERT INTO conditions VALUES
       ('2020-05-02 12:34:56Z', 99, 37.0),
       ('2020-05-02 13:45:00Z', 99, 37.0);
INSERT INTO conditions
SELECT recorded_at, 99, random()*80 - 40
  FROM generate_series('2025-02-01'::timestamptz,
  	               '2025-03-31'::timestamptz,
		       '1 minute'::interval) AS recorded_at;
UPDATE conditions SET temp = 0.0 WHERE device_id = 99;
DELETE FROM conditions WHERE device_id = 99;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Test that we can do multiple changes in a single transaction get
-- the changes for that using the plugin.
START TRANSACTION;
INSERT INTO conditions VALUES ('2020-06-01 12:34:56Z', 101, 37.0);
INSERT INTO conditions VALUES
       ('2020-06-02 12:34:56Z', 101, 37.0),
       ('2020-06-02 13:45:00Z', 101, 37.0);
INSERT INTO conditions
SELECT recorded_at, 101, random()*80 - 40
  FROM generate_series('2020-06-10'::timestamptz,
  	               '2020-07-31'::timestamptz,
		       '1 minute'::interval) AS recorded_at;
UPDATE conditions SET temp = 0.0 WHERE device_id = 101;
DELETE FROM conditions WHERE device_id = 101;
COMMIT;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Insert some tuples, remove a column, and insert some more data, all
-- inside a transaction, and check that it behaves sane even when the
-- tuple format changes within a transaction.
START TRANSACTION;
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 102, 37.0);
ALTER TABLE conditions DROP COLUMN temp;
INSERT INTO conditions VALUES ('2020-05-01 12:34:57Z', 102);
ALTER TABLE conditions ADD COLUMN temp numeric;
INSERT INTO conditions VALUES ('2020-05-01 12:34:58Z', 102, 38.0);
COMMIT;

SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Test to insert some changes and then drop the table to check that
-- it behaves sane.
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 103, 37.0);

DROP TABLE conditions;

\set ON_ERROR_STOP 0
SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');
\set ON_ERROR_STOP 1

-- Create the hypertable again with the same name and see that we do
-- not read bad data.
CREATE TABLE conditions (
       recorded_at timestamptz NOT NULL,
       device_id int,
       temp float,
       primary key (recorded_at, device_id)
);
SELECT create_hypertable('conditions', 'recorded_at');

-- New hypertable: we should not see the ones that we inserted before
-- dropping the old version of the hypertable.
SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Insert some data so that we have some entries.
INSERT INTO conditions VALUES ('2020-05-01 12:34:56Z', 104, 37.0);

-- Check that trying to read a non-existing attribute generates an
-- error.
\set ON_ERROR_STOP 0
SELECT * FROM get_invalidations('my_slot', 'conditions', 'does_not_exist');
\set ON_ERROR_STOP 1

-- We should still be able to get invalidations if we provide a good
-- attribute after this. (This turned out to be a problem during
-- testing.)
SELECT * FROM get_invalidations('my_slot', 'conditions', 'recorded_at');

-- Test using a table without a primary key but with an index that can
-- be used as replica identity.

CREATE TABLE conditions_index (
       captured_at TIMESTAMPTZ NOT NULL,
       device_id INT NOT NULL,
       temp FLOAT,
       UNIQUE (captured_at, device_id)
);

ALTER TABLE conditions_index
      REPLICA IDENTITY USING INDEX conditions_index_captured_at_device_id_key;

SELECT create_hypertable('conditions_index', 'captured_at');

INSERT INTO conditions_index VALUES
       ('2020-05-02 12:34:56Z', 42, 37.0),
       ('2020-05-02 13:45:00Z', 42, 37.0);

SELECT * FROM get_invalidations('my_slot', 'conditions_index', 'captured_at');

-- Test using a table without a primary key but with a replica
-- identity using full rows.

CREATE TABLE conditions_full (
       recorded_at TIMESTAMPTZ NOT NULL,
       device_id INT NOT NULL,
       temp FLOAT
);

ALTER TABLE conditions_full REPLICA IDENTITY FULL;

SELECT create_hypertable('conditions_full', 'recorded_at');

INSERT INTO conditions_full VALUES
       ('2020-05-02 12:34:56Z', 42, 37.0),
       ('2020-05-02 13:45:00Z', 42, 37.0);

SELECT * FROM get_invalidations('my_slot', 'conditions_full', 'recorded_at');

-- Test getting invalidations for multiple hypertables in one go, to
-- make sure that the plugin works with multiple tables as well.

-- We add more invalidations to more tables than we actually fetch,
-- because that should work too.
INSERT INTO conditions_full VALUES
       ('2020-05-03 11:34:00Z', 43, 37.0),
       ('2020-05-03 12:45:00Z', 43, 37.0);
INSERT INTO conditions_index VALUES
       ('2020-05-03 12:00:00Z', 43, 37.0),
       ('2020-05-03 13:00:00Z', 43, 37.0);
INSERT INTO conditions VALUES
       ('2020-05-03 15:00:01Z', 43, 37.0);

-- Capture only two of the three tables above, also with different
-- attribute names.
WITH
  changes AS (
      SELECT (_timescaledb_functions.cagg_parse_invalidation_record(data)).*
        FROM pg_logical_slot_get_binary_changes('my_slot', NULL, NULL,
                                                'conditions_full', 'recorded_at',
                                                'conditions_index', 'captured_at'))
SELECT hypertable_relid,
       _timescaledb_functions.to_timestamp(lowest_modified_value),
       _timescaledb_functions.to_timestamp(greatest_modified_value)
  FROM changes
ORDER BY hypertable_relid;

SELECT * FROM pg_drop_replication_slot('my_slot');
