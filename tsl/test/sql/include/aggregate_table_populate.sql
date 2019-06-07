-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This files assumes the existence of some table with definition as seen in the aggregate_table.sql file.
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'POR', 'west', generate_series(25, 85, 0.0625), 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'SFO', 'west', generate_series(25, 85, 0.0625), 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'SAC', 'west', generate_series(25, 85, 0.0625), 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'SEA', 'west', generate_series(25, 85, 0.0625), 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'TAC', 'west', generate_series(25, 85, 0.0625), 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'NYC', 'north-east', generate_series(29, 41, 0.0125), 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'BOS', 'north-east', generate_series(29, 41, 0.0125), 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'CHI', 'midwest', generate_series(29, 41, 0.0125), 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'MIN', 'midwest', generate_series(29, 41, 0.0125), 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'DET', 'midwest', generate_series(29, 41, 0.0125), 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'LA', 'west', generate_series(61, 85, 0.025), 55, NULL, 28, NULL, NULL, 8, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'SDG', 'west', generate_series(61, 85, 0.025), 55, NULL, 28, NULL, NULL, 8, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'PHX', 'west', generate_series(61, 85, 0.025), 55, NULL, 28, NULL, NULL, 8, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'DAL', 'south', generate_series(61, 85, 0.025), 55, NULL, 28, NULL, NULL, 8, true;
INSERT INTO :TEST_TABLE
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-04 08:00'::timestamp, '5 minute'), 'AUS', 'south', generate_series(61, 85, 0.025), 55, NULL, 28, NULL, NULL, 8, true;
