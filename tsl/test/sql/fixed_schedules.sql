-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_test_next_scheduled_execution_slot(schedule_interval INTERVAL, finish_time TIMESTAMPTZ, initial_start TIMESTAMPTZ, timezone TEXT = NULL)
RETURNS TIMESTAMPTZ AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\set client_min_messages = DEBUG;

select '2023-01-02 11:53:19.059771+02'::timestamptz as finish_time \gset
select :'finish_time'::timestamptz - interval '5 sec' as start_time \gset

-- years
select ts_test_next_scheduled_execution_slot('1 year', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "1 yr",
ts_test_next_scheduled_execution_slot('1 year', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "1 yr timezone",
ts_test_next_scheduled_execution_slot('10 year', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "10 yr",
ts_test_next_scheduled_execution_slot('10 year', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "10 yr timezone";

-- weeks
select ts_test_next_scheduled_execution_slot('1 week', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "1 week",
ts_test_next_scheduled_execution_slot('1 week', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "1 week timezone",
ts_test_next_scheduled_execution_slot('2 week', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "2 week",
ts_test_next_scheduled_execution_slot('2 week', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "2 week timezone";

-- months
select ts_test_next_scheduled_execution_slot('1 month', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "1 month",
ts_test_next_scheduled_execution_slot('1 month', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "1 month timezone",
ts_test_next_scheduled_execution_slot('2 month', :'finish_time'::timestamptz, :'start_time'::timestamptz) as "2 month",
ts_test_next_scheduled_execution_slot('2 month', :'finish_time'::timestamptz, :'start_time'::timestamptz, 'Europe/Athens') as "2 month timezone";

-- timezone in Berlin changes between 1 and 2 am
\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'1 hour\''
\set START_TIME_BEFORE_SUMMER_SWITCH 'TIMESTAMPTZ \'2022-03-27 01:00:00\''
\set FINISH_TIME_AFTER_SUMMER_SWITCH 'TIMESTAMPTZ \'2022-03-27 01:19:20\''
\set START_TIME_BEFORE_WINTER_SWITCH 'TIMESTAMPTZ \'2022-10-30 01:01:00\''
\set FINISH_TIME_AFTER_WINTER_SWITCH 'TIMESTAMPTZ \'2022-10-30 01:10:19\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'1 day\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'1 week\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'1 week\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'1 month\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'1 month\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'1 year\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'1 year\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'2 month\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'2 month\''
\ir include/scheduler_fixed_common.sql

\set BUCKET_WIDTH_WINTER_SUMMER 'INTERVAL \'2 year\''
\set BUCKET_WIDTH_SUMMER_WINTER 'INTERVAL \'2 year\''
\ir include/scheduler_fixed_common.sql
