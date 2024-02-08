-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- attempt to create playground without creating the schema first
DROP SCHEMA IF EXISTS tsdb_playground;

SELECT _timescaledb_functions.create_playground('public.conditions_1');

CREATE SCHEMA IF NOT EXISTS tsdb_playground;

CREATE TABLE public.conditions_1 (
		time TIMESTAMPTZ NOT NULL,
		temperature NUMERIC
		);

-- test creating playground on raw table
SELECT _timescaledb_functions.create_playground('public.conditions_1');

SELECT table_name FROM public.create_hypertable('public.conditions_1', 'time');

-- Attempt to create playground on empty hypertable
SELECT _timescaledb_functions.create_playground('public.conditions_1');


INSERT INTO public.conditions_1
	SELECT generate_series('2021-01-01 00:00'::timestamp, '2021-01-2 23:59:59'::timestamp, '1 hour'), random()*100;

SELECT _timescaledb_functions.create_playground('public.conditions_1');

SELECT count(*) FROM tsdb_playground.conditions_1_1;

SELECT _timescaledb_functions.create_playground('public.conditions_1');

SELECT count(*) FROM tsdb_playground.conditions_1_2;

-- use table names with space characters
CREATE TABLE public."bar foo" (
                time TIMESTAMPTZ NOT NULL,
                temperature NUMERIC
                );

SELECT table_name FROM public.create_hypertable('public."bar foo"', 'time');

INSERT INTO public."bar foo"
        SELECT generate_series('2021-01-01 00:00'::timestamp, '2021-01-2 23:59:59'::timestamp, '1 hour'), random()*100;

SELECT _timescaledb_functions.create_playground('public."bar foo"');

SELECT _timescaledb_functions.create_playground('public."bar foo"');
