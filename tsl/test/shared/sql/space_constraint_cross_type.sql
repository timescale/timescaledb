-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\pset tuples_only on

CREATE TABLE space_constraint_cross_type(departs_at timestamptz NOT NULL, route_added_on date NOT NULL);
SELECT create_hypertable('space_constraint_cross_type', 'departs_at') AS hypertable \gset
SELECT add_dimension('space_constraint_cross_type', 'route_added_on', number_partitions => 2) AS dimension \gset
INSERT INTO space_constraint_cross_type VALUES ('2024-01-15 08:00:00+00', '2024-01-15');
BEGIN;
DELETE FROM space_constraint_cross_type WHERE route_added_on = '2024-01-15'::timestamp;
SELECT count(*) FROM space_constraint_cross_type;
ROLLBACK;
DROP TABLE space_constraint_cross_type;
