-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_test_next_scheduled_execution_slot(schedule_interval INTERVAL, finish_time TIMESTAMPTZ, initial_start TIMESTAMPTZ, timezone TEXT = NULL)
RETURNS TIMESTAMPTZ AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\set client_min_messages = DEBUG;

-- test what happens across DST
-- go from +1 to +2
set timezone to 'Europe/Berlin';
-- DST switch on March 27th 2022, in particular, between 1am and 2 am (old time)
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :FINISH_TIME_AFTER_SUMMER_SWITCH, :START_TIME_BEFORE_SUMMER_SWITCH) as t1 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :FINISH_TIME_AFTER_SUMMER_SWITCH, :START_TIME_BEFORE_SUMMER_SWITCH, timezone => 'Europe/Berlin')
as t1_tz \gset
select :START_TIME_BEFORE_SUMMER_SWITCH as initial_start_summer_switch, :FINISH_TIME_AFTER_SUMMER_SWITCH as first_finish_time_summer_switch;
select :'t1' as without_timezone, :'t1_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t1'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH) as t2 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t1_tz'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH, timezone => 'Europe/Berlin') as t2_tz \gset
select :'t2' as without_timezone, :'t2_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t2'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH) as t3 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t2_tz'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH, timezone => 'Europe/Berlin') as t3_tz \gset
select :'t3' as without_timezone, :'t3_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t3'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH) as t4 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t3_tz'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH, 'Europe/Berlin') as t4_tz \gset
select :'t4' as without_timezone, :'t4_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t4'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH) as t5 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t4_tz'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH, 'Europe/Berlin') as t5_tz \gset
select :'t5' as without_timezone, :'t5_tz' as with_timezone;
--------- and then +2 to +1
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :FINISH_TIME_AFTER_WINTER_SWITCH, :START_TIME_BEFORE_WINTER_SWITCH) as t1 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :FINISH_TIME_AFTER_WINTER_SWITCH, :START_TIME_BEFORE_WINTER_SWITCH, timezone => 'Europe/Berlin')
as t1_tz \gset
select :START_TIME_BEFORE_WINTER_SWITCH as initial_start_winter_switch, :FINISH_TIME_AFTER_WINTER_SWITCH as first_finish_time_winter_switch;
select :'t1' as without_timezone, :'t1_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t1'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH) as t2 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t1_tz'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH, timezone => 'Europe/Berlin') as t2_tz \gset
select :'t2' as without_timezone, :'t2_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t2'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH) as t3 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t2_tz'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH, timezone => 'Europe/Berlin') as t3_tz \gset
select :'t3' as without_timezone, :'t3_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t3'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH) as t4 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_SUMMER_WINTER, :'t3_tz'::timestamptz, :START_TIME_BEFORE_WINTER_SWITCH, 'Europe/Berlin') as t4_tz \gset
select :'t4' as without_timezone, :'t4_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t4'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH) as t5 \gset
select ts_test_next_scheduled_execution_slot(:BUCKET_WIDTH_WINTER_SUMMER, :'t4_tz'::timestamptz, :START_TIME_BEFORE_SUMMER_SWITCH, 'Europe/Berlin') as t5_tz \gset
select :'t5' as without_timezone, :'t5_tz' as with_timezone;

