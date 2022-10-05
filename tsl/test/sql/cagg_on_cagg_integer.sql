-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Setup test variables
\set IS_DISTRIBUTED FALSE
\set IS_TIME_DIMENSION FALSE
\set TIME_DIMENSION_DATATYPE INTEGER
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_1
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_5
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_10
\set BUCKET_WIDTH_1ST 'INTEGER \'1\''
\set BUCKET_WIDTH_2TH 'INTEGER \'5\''
\set BUCKET_WIDTH_3TH 'INTEGER \'10\''

-- Run tests
\ir include/cagg_on_cagg_common.sql
