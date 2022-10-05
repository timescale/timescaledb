-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Setup test variables
\set IS_DISTRIBUTED FALSE
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_hourly
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_daily
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_weekly
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_3TH 'INTERVAL \'1 week\''

SET timezone TO 'UTC';

-- Run tests
\ir include/cagg_on_cagg_common.sql
