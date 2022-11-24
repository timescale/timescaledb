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

--
-- Run common tests
--
\ir include/cagg_on_cagg_common.sql

--
-- Validation test for non-multiple bucket sizes
--
\set BUCKET_WIDTH_1ST 'INTEGER \'2\''
\set BUCKET_WIDTH_2TH 'INTEGER \'5\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for equal bucket sizes
--
\set BUCKET_WIDTH_1ST 'INTEGER \'2\''
\set BUCKET_WIDTH_2TH 'INTEGER \'2\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should not be equal to previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for bucket size less than source
--
\set BUCKET_WIDTH_1ST 'INTEGER \'4\''
\set BUCKET_WIDTH_2TH 'INTEGER \'2\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql
